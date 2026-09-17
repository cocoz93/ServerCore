//
// IOCP 전송 계층 — 완료 포트(GQCS/GQCSEx) 기반 제출·수거 구현.
//   공통 골격(IOCPServer.cpp)은 Transport* 위임 지점으로만 이 파일을 호출한다.
//   RIO 빌드에서는 파일 전체가 비활성 — 빈 번역단위가 된다.
//
#include "IOCPServer.h"

#if !USE_RIO_TRANSPORT

#include <Common/ErrorLog.h>
#include <chrono>
#include <algorithm>
#include <cassert>

#include "CoreAffinity.h"
#include "Platform/Platform.h"   // 코어 수 산정·스레드 CPU 측정 (OS별 구현은 이 경계 뒤)

// ============================================================================
// 전송 계층 경계 — 공통 골격이 위임하는 지점 (선언은 IOCPServer.h)
// ============================================================================

// 세션 링버퍼 준비 — IOCP는 세션마다 자기 힙 버퍼를 갖는다 (RIO는 등록 슬랩의 슬라이스).
bool CIOCPServer::TransportInitSessionBuffer(CSession* session)
{
#if USE_LOCKFREE_SENDQ
    return session->_recvQ.Init() && session->_sendQ.Init(256);
#else
    return session->_recvQ.Init() && session->_sendQ.Init();
#endif
}

// 리슨 소켓 "전" 준비 — 완료 포트 핸들. accept 소켓을 여기에 바인드한다.
bool CIOCPServer::TransportPreListen()
{
    // 워커 수·IOCP concurrency 산정 — affinity로 제한된 가용 코어 수가 단일 기준.
    // (INI WorkerThreads>0이면 워커 수만 그 값으로 오버라이드, concurrency는 코어 수 유지)
    // main에서 프로세스 affinity를 건 "뒤"에 호출되므로 제한된 코어 수가 정확히 잡힌다.
    _coreCount = Platform::GetAvailableCoreCount();
    _workerThreadCount = (_configuredWorkers > 0) ? _configuredWorkers : _coreCount;

    // IOCP 핸들 생성 — NumberOfConcurrentThreads = 가용 코어 수 (동시 실행 워커 상한)
    _iocpHandle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0,
                                         static_cast<DWORD>(_coreCount));
    return (_iocpHandle != NULL);
}

// 리슨 소켓 생성이 실패했을 때 위 준비물 되돌리기
void CIOCPServer::TransportPreListenCleanup()
{
    CloseHandle(_iocpHandle);
}

// 리슨 소켓 "후" 준비 — IOCP는 할 일이 없다 (RIO만 함수 테이블·슬랩·CQ가 필요).
bool CIOCPServer::TransportPostListen()
{
    return true;
}

// WSASocket 플래그 — RIO 팔은 여기에 REGISTERED_IO를 더한다.
uint32_t CIOCPServer::TransportListenFlags() const
{
    return WSA_FLAG_OVERLAPPED;
}

// 완료 워커·송신 워커 기동
void CIOCPServer::TransportStartWorkers()
{
    // 완료 수거 방식 확정 — 워커가 읽는 값이라 반드시 기동 "전"에 정하고, 이후 건드리지 않는다.
    //   0이면 기존 GQCS(완료 1건당 syscall 1회), N>0이면 GQCSEx로 한 번에 최대 N건.
    //   A/B를 재빌드가 아니라 INI 한 줄로 돌리는 이유는 두 팔이 같은 바이너리여야 빌드 차이가
    //   변인으로 섞이지 않기 때문이다(증분빌드 오라벨 위험도 함께 사라진다).
    _completionBatch = (_configuredCompletionBatch > 0)
                     ? std::clamp(_configuredCompletionBatch, 1, MAX_COMPLETION_BATCH)
                     : 0;
    _monitor._completionBatch = _completionBatch;   // 수집 스크립트가 아암 라벨과 대조하는 게이지

    if (_completionBatch == 0)
        SLOG_INFO("[Network] Completion harvest = GQCS (완료 1건당 1회 수거)");
    else
        SLOG_INFO("[Network] Completion harvest = GQCSEx (배치 상한 {})", _completionBatch);

    // 워커 스레드 생성 — affinity 가용 코어 수 기준 (INI WorkerThreads로 오버라이드 가능)
    for (int i = 0; i < _workerThreadCount; ++i)
    {
        _workerThreads.emplace_back(&CIOCPServer::WorkerThread, this);
    }

#if USE_SEND_THREAD
    // 전용 송신 워커 풀 생성 — K개 워커(uniqueId%K 분배). 기본 1 = 기존 단일 스레드와 동등(회귀 기준선).
    //   워커 객체(mutex/cv 참조)를 먼저 전부 생성한 뒤 스레드 시작 → 스레드가 자기 워커 참조 시 존재 보장.
    const int sendCount = std::clamp((_configuredSendWorkers > 0) ? _configuredSendWorkers : 1,
                                     1, CMonitorManager::MAX_SEND_WORKERS);
    _sendWorkerCount = sendCount;
    _monitor._sendWorkerCount.Store(sendCount);       // 노출 루프 상한
    _sendWorkers.reserve(sendCount);
    for (int i = 0; i < sendCount; ++i)
        _sendWorkers.push_back(std::make_unique<SendWorker>());
    for (int i = 0; i < sendCount; ++i)
        _sendWorkers[i]->thread = std::thread(&CIOCPServer::SendWorkerThread, this, i);
    SLOG_INFO("[Network] Send workers = {} (uniqueId % K 분배)", sendCount);
#endif
}

// 기동 완료 로그 — 스모크가 이 문구로 팔(arm)을 판정한다 (변경 시 RIO\echo-smoke.ps1 동반 수정).
void CIOCPServer::TransportLogStarted(const char* modeName) const
{
    SLOG_INFO("[Network] Server started — worker threads={}, IOCP concurrency={} (affinity cores={}, Mode: {})",
              _workerThreadCount, _coreCount, _coreCount, modeName);
}

// IOCount 드레인 "전" 정지 — 송신 워커를 먼저 멈춰야 PostSend가 pin한 IOCount가 풀린다.
void CIOCPServer::TransportStopBeforeDrain()
{
#if USE_SEND_THREAD
    // send 워커 정지·드워커 — 아래 IOCount 0 대기 루프 *이전*에 모든 워커을 멈춰야
    //      PostSend가 pin한 IOCount 잔존으로 인한 데드락을 막는다(워커이 살아있으면 IOCount가 안 떨어짐).
    //      게임루프는 CGameServer::Stop에서 이미 join된 상태라 새 handoff는 들어오지 않는다.
    _sendStop.store(true);
    for (auto& worker : _sendWorkers)
    {
        { std::lock_guard<std::mutex> lk(worker->mutex); }  // 막 wait 진입하는 워커과의 race 방지
        worker->cv.notify_one();
    }
    for (auto& worker : _sendWorkers)
    {
        if (worker->thread.joinable())
            worker->thread.join();
    }
#endif
}

// IOCount 드레인 "후" 정지 — 완료 워커 종료·완료 포트 해제
void CIOCPServer::TransportStopAfterDrain()
{
    if (_iocpHandle != NULL)
    {
        for (size_t i = 0; i < _workerThreads.size(); ++i)
        {
            PostQueuedCompletionStatus(_iocpHandle, 0, 0, nullptr);
        }
    }

    for (auto& thread : _workerThreads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    if (_iocpHandle != NULL)
    {
        CloseHandle(_iocpHandle);
        _iocpHandle = NULL;
    }
}

// 완료 통지 연결 — IOCP의 CompletionKey는 단순 식별자 역할이므로 세션 소유권을 갖지 않는다.
bool CIOCPServer::TransportAttachSession(CSession* session, Platform::NetSocket clientSocket)
{
    return BindIOCP(clientSocket, reinterpret_cast<ULONG_PTR>(session));
}

// 첫 Recv 착수 — Initialize의 IOCount=1이 이 IO의 ref. AcquireSession 불필요.
void CIOCPServer::TransportStartFirstRecv(CSession* session, Platform::NetSocket clientSocket, int64_t sessionId)
{
    (void)clientSocket;
    (void)sessionId;
    PostRecv(session, true);
}

// SendFlush::Immediate — 호출 스레드에서 곧바로 제출한다.
void CIOCPServer::TransportSendImmediate(CSession* session, int64_t sessionId)
{
    (void)sessionId;
    PostSend(session);
}

// 틱 끝 dirty 배치 → 송신 (게임 스레드 단독 호출)
void CIOCPServer::TransportFlushDirty()
{
#if USE_SEND_THREAD
    // dirty 배치(sessionId)를 uniqueId%K 워커에 핸드오프. 게임루프는 WSASend(PostSend)를 하지 않는다.
    //   한 세션은 항상 같은 워커 → FIFO 보장. push마다 락 대신 워커별로 묶어 1회 락(최대 K회).
    //   분류 버퍼는 게임 스레드 단독 접근이라 무락(thread_local 재사용으로 매틱 할당 회피).
    thread_local std::vector<std::vector<int64_t>> perWorker;
    if (static_cast<int>(perWorker.size()) != _sendWorkerCount)
        perWorker.assign(_sendWorkerCount, {});
    else
        for (auto& v : perWorker)
            v.clear();

    for (CSession* session : _dirtySessions)
    {
        session->_sendDirty = false;                   // 게임 스레드 단독 접근 → 안전
        // 틱을 넘는 중복 방지(_queuedForSend): 이미 큐에 있으면(미처리) 다시 넣지 않는다.
        //   발산 시 같은 세션이 매 틱 쌓여 큐가 무한 증가하는 것을 막음(처리량 천장은 별개).
        if (InterlockedExchange(&session->_queuedForSend, TRUE) == FALSE)
        {
            // sessionId는 exchange "후"에 읽는다 — 읽기~exchange 사이 slot 재사용 시 옛 id를 push하면
            //   워커가 FindSession으로 새 세션을 못 찾아 _queuedForSend가 TRUE인 채 안 지워져 송신 mute.
            // [분배 수정] uniqueId(하위 48비트)로 분배 — raw sessionId%K는 K가 2의 거듭제곱이 아니면
            //   상위 index 비트가 modulo에 새어든다. index는 스택 재사용으로 카운트다운·uniqueId는 카운트업이라
            //   (index+uniqueId)가 상수가 돼 K3에서 한 워커로 ~90% 쏠림(실측·시뮬 확인). uniqueId만 쓰면 K 무관 균등·FIFO 보존.
            const int64_t sessionId = session->_sessionId; // volatile → 일반 복사 후 사용
            perWorker[CSession::ExtractUniqueId(sessionId) % _sendWorkerCount].push_back(sessionId);  // raw ptr 아닌 id
        }
    }
    _dirtySessions.clear();

    for (int i = 0; i < _sendWorkerCount; ++i)
    {
        if (perWorker[i].empty())
            continue;
        SendWorker& worker = *_sendWorkers[i];
        {
            std::lock_guard<std::mutex> lk(worker.mutex);
            worker.queue.insert(worker.queue.end(), perWorker[i].begin(), perWorker[i].end());
        }
        worker.cv.notify_one();
    }
#elif USE_SEND_COALESCING
    for (CSession* session : _dirtySessions)
    {
        session->_sendDirty = false;
        PostSend(session);
    }
    _dirtySessions.clear();
#endif
}

// 종료 유도 — CancelIoEx가 pending을 에러 완료시켜 IOCount 0 수렴을 이끈다.
bool CIOCPServer::TransportRequestDisconnect(CSession* session)
{
    // 다른스레드에서 이미 처리중인 경우
    if (InterlockedExchange(&session->_disconnecting, TRUE) == TRUE)
        return false;

    // CancelIoEx로 pending IO를 즉시 완료(에러)시켜 IOCount가 0으로 수렴하게 한다.
    // INVALID_SOCKET이면 ERROR_INVALID_HANDLE로 실패할 뿐, 부작용 없음.
    CancelIoEx(reinterpret_cast<HANDLE>(session->_socket), nullptr);

    return true;
}

// ============================================================================
// IOCP 본체 — 완료 포트 바인드 · 수거 루프 · 제출
// ============================================================================

bool CIOCPServer::BindIOCP(SOCKET socket, ULONG_PTR completionKey)
{
    auto handle = CreateIoCompletionPort((HANDLE)socket, _iocpHandle, completionKey, 0);
    if (handle == NULL)
    {
        LOG_ERROR_STREAM("BindIOCP failed: " << GetLastError());
        return false;
    }
    return true;
}

// 완료 통지 처리. IOCount 감소는 ProcessRecv/ProcessSend 이후에만 수행한다.
void CIOCPServer::WorkerThread()
{
    CoreAffinity::PinIoThread();   // IOCP 워커 → 게임코어 밖으로 (격리 off면 no-op)

    int workerIndex = _monitor.RegisterWorkerThread();

    // CPU 점유율 측정용: 자기 스레드의 측정 핸들을 슬롯에 등록 (HTTP 스레드가 외부에서 읽음)
    if (workerIndex >= 0 && workerIndex < CMonitorManager::MAX_WORKER_THREADS)
    {
        _monitor._workerCounters[workerIndex].threadHandle = Platform::CaptureCurrentThreadCpu();
    }

    // 수거 방식은 Start()에서 워커 기동 전에 확정되므로 루프 진입 전에 한 번만 읽는다.
    const int batchCap = _completionBatch;
    OVERLAPPED_ENTRY entries[MAX_COMPLETION_BATCH];   // 4KB — batchCap==0이면 건드리지 않는다

    while (true)
    {
        // ── GQCSEx 경로 — 한 번의 수거로 최대 batchCap건 (분기는 배치당 1회라 완료당 비용엔 안 잡힌다) ──
        if (batchCap > 0)
        {
            ULONG removed = 0;
            const BOOL ok = GetQueuedCompletionStatusEx(_iocpHandle, entries,
                static_cast<ULONG>(batchCap), &removed, INFINITE, FALSE);

            // [계측] 수거 호출 횟수 — 완료수/호출수가 곧 배치 효율. GQCS 팔의 1.0이 비교 기준선이다.
            if (workerIndex >= 0 && workerIndex < CMonitorManager::MAX_WORKER_THREADS)
                _monitor._workerCounters[workerIndex].AddDequeue();

            if (ok == FALSE)
            {
                // INFINITE 대기에서의 실패는 포트 핸들이 닫혔다는 뜻 — GQCS 경로가 널 overlapped로
                // 빠져나가던 자리와 같게 취급한다.
                if (_running == FALSE)
                    break;
                continue;
            }

            int stopSignals = 0;
            for (ULONG i = 0; i < removed; ++i)
            {
                OVERLAPPED* ov = entries[i].lpOverlapped;
                if (ov == nullptr)
                {
                    ++stopSignals;   // 종료 깨우기 패킷 (PostQueuedCompletionStatus)
                    continue;
                }

                // GQCSEx는 항목별 성공/실패를 안 준다. OVERLAPPED::Internal이 그 IO의 NTSTATUS이고,
                // 최상위 비트가 서면 실패 — GQCS의 result==FALSE와 같은 판정이다.
                const bool ioFailed = (static_cast<LONG>(ov->Internal) < 0);
                HandleCompletion(ov, entries[i].lpCompletionKey,
                                 entries[i].dwNumberOfBytesTransferred, ioFailed, workerIndex);
            }

            if (stopSignals > 0 && _running == FALSE)
            {
                // [셧다운 불변식] 종료 패킷은 "워커 1개당 1개"만 뿌려지는데, 배치 수거는 한 번에
                //   여러 개를 삼킬 수 있다. 삼킨 채로 나가면 남은 워커가 영영 안 깨어나 join이 행에 걸린다.
                //   내 몫 1개만 쓰고 나머지는 큐에 되돌려 놓는다 (총량 보존 → 모든 워커가 정확히 1개씩).
                for (int k = 1; k < stopSignals; ++k)
                    PostQueuedCompletionStatus(_iocpHandle, 0, 0, nullptr);
                break;
            }
            continue;
        }

        // ── GQCS 경로 (기존 동작) — 완료 1건당 syscall 1회. 비교 기준선이자 기본값 ──
        DWORD bytesTransferred = 0;
        ULONG_PTR completionKey = 0;
        OVERLAPPED* overlapped = nullptr;

        BOOL result = GetQueuedCompletionStatus(_iocpHandle, &bytesTransferred,
            &completionKey, &overlapped, INFINITE);

        // [계측] 완료 수거 호출 횟수 — GQCS는 1콜당 1완료(배치 없음)라 완료수와 같아지는 게 정상이고,
        //   그 1:1이 GQCSEx 팔의 배치 효율을 재는 기준선이 된다.
        //   빈 깨어남(종료 통지 등)도 호출은 호출이므로 널 검사보다 앞에서 센다.
        if (workerIndex >= 0 && workerIndex < CMonitorManager::MAX_WORKER_THREADS)
            _monitor._workerCounters[workerIndex].AddDequeue();

        if (overlapped == nullptr)
        {
            if (_running == FALSE)
                break;
            continue;
        }

        HandleCompletion(overlapped, completionKey, bytesTransferred,
                         (result == FALSE), workerIndex);
    }
}

// 완료 1건 처리 — 두 수거 경로(GQCS/GQCSEx)의 유일한 공통 본체.
//   여기 한 곳만 두는 이유는 A/B 때문이다: 판정이 두 벌로 갈리면 팔 사이 동작 차이가
//   측정값에 섞여도 알아채기 어렵다.
void CIOCPServer::HandleCompletion(OVERLAPPED* overlapped, ULONG_PTR completionKey,
                                   DWORD bytesTransferred, bool ioFailed, int workerIndex)
{
    auto overlappedEx = reinterpret_cast<CSession::OverlappedEx*>(overlapped);
    auto session = reinterpret_cast<CSession*>(completionKey);

    // [불변식] overlapped 널은 호출자가 걸렀고, completionKey는 BindIOCP가
    //   _sessions[index].get()(비널)로만 등록한다.
    assert(overlappedEx != nullptr && session != nullptr);

    IOOperation op = overlappedEx->operation;
    bool canProcess = (!ioFailed && bytesTransferred != 0 &&
        session->_disconnecting == FALSE);

    if (canProcess)
    {
        switch (op)
        {
        case IOOperation::RECV:
            ProcessRecv(session, bytesTransferred);
            break;
        case IOOperation::SEND:
            ProcessSend(session, bytesTransferred, overlappedEx->slot);
            break;
        default:
            break;
        }
    }
    else if (ioFailed || bytesTransferred == 0)
    {
        RequestDisconnectSession(session);
    }

    IOCountDecrement(session);

    // 워커 스레드별 완료 통지 카운트
    if (workerIndex >= 0 && workerIndex < CMonitorManager::MAX_WORKER_THREADS)
        _monitor._workerCounters[workerIndex].AddCompletion();
}

// Recv I/O 제출. 제출 전 IOCount로 세션을 pin한다.
// skipAcquire=true: ProcessAccept에서 첫 Recv 시 Initialize의 IOCount=1을 그대로 사용.
void CIOCPServer::PostRecv(CSession* session, bool skipAcquire)
{
    if (!session)
        return;

    if (!skipAcquire)
    {
        if (!AcquireSession(session))
            return;
    }

    const int64_t sessionId = session->_sessionId;

    CSession::OverlappedEx* ex = &session->_recvOverlapped;
    ZeroMemory(&ex->overlapped, sizeof(OVERLAPPED));
    ex->operation = IOOperation::RECV;

    // 링버퍼에서 쓰기 가능한 공간 확보
    char* writePtr = session->_recvQ.GetWritePtr();
    size_t directWriteSize = session->_recvQ.GetDirectWriteSize();

    WSABUF wsaBuf[2];
    int bufCount = 0;

    if (directWriteSize > 0)
    {
        wsaBuf[bufCount].buf = writePtr;
        wsaBuf[bufCount].len = static_cast<ULONG>(directWriteSize);
        bufCount++;

        size_t freeSize = session->_recvQ.GetFreeSize();
        if (freeSize > directWriteSize)
        {
            size_t wrapSize = freeSize - directWriteSize;
            wsaBuf[bufCount].buf = session->_recvQ._buffer;
            wsaBuf[bufCount].len = static_cast<ULONG>(wrapSize);
            bufCount++;
        }
    }

    // [불변식] 링이 가득 찰(bufCount==0) 수 없다 — 파싱 후 잔여는 불완전 패킷 조각뿐이라
    //   MAX_PACKET_SIZE(1458) 미만인데 링 용량은 65535다. 파싱이 disconnect로 끝났다면
    //   위 AcquireSession이 먼저 실패한다. 이 용량 관계가 어긋나면 여기서 먼저 터진다.
    assert(bufCount > 0);

    if (session->_disconnecting == TRUE)
    {
        // WSABUF 준비 ~ IO 제출 사이에 세션이 disconnect되면 제출하지 않는다.
        IOCountDecrement(session);
        return;
    }

    SOCKET socket = session->_socket;
    if (socket == INVALID_SOCKET)
    {
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return;
    }

    DWORD flags = 0;
    DWORD recvBytes = 0;

    _monitor._wsaRecvCalls.Inc();
    int result = WSARecv(socket, wsaBuf, bufCount, &recvBytes, &flags,
        &ex->overlapped, NULL);

    if (result == SOCKET_ERROR)
    {
        const int wsaErr = WSAGetLastError();
        if (wsaErr != WSA_IO_PENDING)
        {
            if (!shared::ShouldIgnoreWsaError(wsaErr))
            {
                LOG_WSA_ERROR_STREAM("WSARecv failed - SessionId: " << sessionId << ", WSAError: ", wsaErr);
            }
            RequestDisconnectSession(session);
            IOCountDecrement(session);
            return;
        }
    }

    // Post-check: pre-check ~ WSARecv 사이에 끼어든 disconnect race 회수.
    // CancelIoEx가 먼저 지나간 뒤 WSARecv가 늦게 걸린 IO를 취소한다.
    if (session->_disconnecting == TRUE)
    {
        CancelIoEx(reinterpret_cast<HANDLE>(socket), &ex->overlapped);
    }
}

// Send는 1회로 제한
// Send I/O 제출. 제출 전 IOCount로 세션을 pin한다.
#if USE_LOCKFREE_SENDQ
void CIOCPServer::PostSend(CSession* session)
{
    if (!session)
        return;

    if (!AcquireSession(session))
        return;

    const int64_t sessionId = session->_sessionId;

    if (InterlockedExchange(&session->_sendSubmitBusy, TRUE) == TRUE)
    {
        _monitor._sendContention.Inc();
        IOCountDecrement(session);
        return;
    }

    // Dequeue up to MAX_SEND_BUFS
    WSABUF wsaBuf[CSession::MAX_SEND_BUFS];
    int bufCount = 0;
    int totalBytes = 0;
    CSerialBuffer* pBuf = nullptr;

    while (bufCount < CSession::MAX_SEND_BUFS && session->_sendQ.Dequeue(&pBuf))
    {
        session->_pendingSendBufs[bufCount] = pBuf;
        wsaBuf[bufCount].buf = pBuf->GetReadBufferPtr();
        wsaBuf[bufCount].len = static_cast<ULONG>(pBuf->GetDataSize());
        totalBytes += wsaBuf[bufCount].len;
        bufCount++;
    }
    session->_pendingSendCount = bufCount;
    session->_pendingSendBytes = totalBytes;

    if (bufCount == 0)
    {
        InterlockedExchange(&session->_sendSubmitBusy, FALSE);
        IOCountDecrement(session);
        return;
    }

    if (session->_disconnecting == TRUE)
    {
        session->ReleasePendingSendBufs();
        InterlockedExchange(&session->_sendSubmitBusy, FALSE);
        IOCountDecrement(session);
        return;
    }

    SOCKET socket = session->_socket;
    if (socket == INVALID_SOCKET)
    {
        session->ReleasePendingSendBufs();
        InterlockedExchange(&session->_sendSubmitBusy, FALSE);
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return;
    }

    // 락프리 큐 경로는 깊이를 쓰지 않으므로 슬롯 0만 돌려쓴다 (옛 단일 _sendOverlapped 자리).
    CSession::OverlappedEx* ex = &session->_sendSlots[0].ov;
    ZeroMemory(&ex->overlapped, sizeof(OVERLAPPED));
    ex->operation = IOOperation::SEND;

    DWORD sendBytes = 0;
    _monitor._wsaSendCalls.Inc();
    int result = WSASend(socket, wsaBuf, bufCount, &sendBytes, 0,
        &ex->overlapped, NULL);

    if (result == SOCKET_ERROR)
    {
        const int wsaErr = WSAGetLastError();
        if (wsaErr != WSA_IO_PENDING)
        {
            if (!shared::ShouldIgnoreWsaError(wsaErr))
            {
                LOG_WSA_ERROR_STREAM("WSASend failed - SessionId: " << sessionId
                    << ", WSAError: ", wsaErr);
            }
            session->ReleasePendingSendBufs();
            InterlockedExchange(&session->_sendSubmitBusy, FALSE);
            RequestDisconnectSession(session);
            IOCountDecrement(session);
            return;
        }
    }

    // Post-check: pre-check ~ WSASend 사이에 끼어든 disconnect race 회수.
    if (session->_disconnecting == TRUE)
    {
        CancelIoEx(reinterpret_cast<HANDLE>(socket), &ex->overlapped);
    }
}
#else
// 미제출 구간을 WSASend 한 번으로 내보낸다 — 랩이 있으면 WSABUF 2개로 함께 보내므로
//   in-flight 슬롯은 항상 1개만 쓴다 (요청당 버퍼가 1개인 RIO 판과 갈리는 지점).
int CIOCPServer::TransportSubmitSegment(CSession* session, Platform::NetSocket socket,
                                        const CRingBufferMT::SubmitInfo& info,
                                        int slot, int slotsAvailable, size_t* submittedBytes)
{
    (void)slotsAvailable;   // 랩까지 한 방에 보내므로 여유 슬롯 수를 보지 않는다

    WSABUF wsaBuf[2];
    int bufCount = 0;

    wsaBuf[bufCount].buf = info.submitPtr;
    wsaBuf[bufCount].len = static_cast<ULONG>(info.directSize);
    bufCount++;

    if (info.size > info.directSize)
    {
        wsaBuf[bufCount].buf = session->_sendQ._buffer;
        wsaBuf[bufCount].len = static_cast<ULONG>(info.size - info.directSize);
        bufCount++;
    }

    // 슬롯에 제출량을 심는다 — 완료가 부분 송신 판정에 읽고, 회수가 링 반환량으로 쓴다.
    //   제출 "전"에 심어야 한다(제출 직후 다른 워커가 완료를 먼저 처리할 수 있다).
    CSession::SendSlot& s = session->_sendSlots[slot];
    s.bytes = info.size;

    // 제출 "전"에 경계를 옮긴다 — WSASend가 성공하면 그 즉시 다른 워커가 완료를 처리할 수 있고,
    //   그 완료는 ConsumeSubmitted로 경계 안쪽만 소비하므로 경계가 미리 앞서 있어야 한다.
    if (session->_sendQ.MarkSubmitted(info.size) != info.size)
    {
        s.bytes = 0;
        return 0;
    }

    CSession::OverlappedEx* ex = &s.ov;
    ZeroMemory(&ex->overlapped, sizeof(OVERLAPPED));
    ex->operation = IOOperation::SEND;
    ex->slot = slot;        // 완료 통지가 자기 슬롯을 알아내는 경로

    DWORD sendBytes = 0;
    _monitor._wsaSendCalls.Inc();
    int result = WSASend(socket, wsaBuf, bufCount, &sendBytes, 0, &ex->overlapped, NULL);

    if (result == SOCKET_ERROR)
    {
        const int wsaErr = WSAGetLastError();
        if (wsaErr != WSA_IO_PENDING)
        {
            if (!shared::ShouldIgnoreWsaError(wsaErr))
            {
                LOG_WSA_ERROR_STREAM("WSASend failed - SessionId: " << session->_sessionId
                    << ", WSAError: ", wsaErr);
            }
            return 0;   // 호출자(PostSend)가 세션 종료를 유도한다 — 기존 처리와 동일
        }
    }

    // Post-check: 세그먼트 계산 ~ WSASend 사이에 끼어든 disconnect race 회수.
    if (session->_disconnecting == TRUE)
    {
        CancelIoEx(reinterpret_cast<HANDLE>(socket), &ex->overlapped);
    }

    *submittedBytes = info.size;
    return 1;
}
#endif

#if USE_SEND_THREAD
// 전용 송신 스레드 — 게임루프가 넘긴 dirty 배치를 받아 세션당 1회 WSASend(PostSend)를 수행.
// sessionId로 받아 FindSession으로 재검증(_sessionId 일치·미종료 = ABA-safe)한 뒤 PostSend를 호출하며,
// PostSend 내부의 AcquireSession(수명)과 _sendSubmitBusy·_sendInFlight 게이트(제출 직렬화·깊이)가
// 세션당 송신 계약을 보장한다.
void CIOCPServer::SendWorkerThread(int workerIdx)
{
    CoreAffinity::PinIoThread();   // 송신 워커 → 게임코어 밖으로 (격리 off면 no-op)

    SendWorker& worker = *_sendWorkers[workerIdx];

    // [계측] CPU 점유율 측정용 — 자기 스레드의 측정 핸들을 모니터 슬롯에 등록 (게임루프/워커와 동일 패턴).
    {
        _monitor._sendCounters[workerIdx].threadHandle = Platform::CaptureCurrentThreadCpu();
    }

    std::vector<int64_t> local;
    while (true)
    {
        {
            std::unique_lock<std::mutex> lk(worker.mutex);
            worker.cv.wait(lk, [this, &worker] { return !worker.queue.empty() || _sendStop.load(); });
            if (worker.queue.empty() && _sendStop.load())
                break;                       // 정지 요청 + 잔여 배치 드워커 완료 → 종료
            local.swap(worker.queue);          // 누적분을 통째로 인출 (백로그 시 여러 틱 병합 가능)
        }

        // [계측] 핸드오프 백로그 — 이번 drain에서 인출한 세션 수 (1틱 dirty 수 초과 = 이 워커이 못 따라감)
        _monitor._sendCounters[workerIdx].backlog.Store(static_cast<int64_t>(local.size()));

        // [계측] 이 워커의 실제 WSASend 시간 — 슬롯당 단독 writer(워커 자신)라 원자 누적.
        const auto sendT0 = std::chrono::steady_clock::now();
        for (int64_t sessionId : local)
        {
            CSession* session = FindSession(sessionId);  // id 일치·미종료 검증
            if (session)
            {
                // 잔류 표식 해제는 PostSend(_sendQ Dequeue) "전"에 — 처리 도중/직후 도착한
                // 데이터가 다시 큐에 들어가 누락되지 않게(제출 잠금 해제 후 double-check와 동일 원리).
                InterlockedExchange(&session->_queuedForSend, FALSE);
                PostSend(session);
            }
        }
        const auto sendT1 = std::chrono::steady_clock::now();
        _monitor._sendCounters[workerIdx].flushUs.Add(std::chrono::duration_cast<std::chrono::microseconds>(sendT1 - sendT0).count());

        local.clear();
    }
}
#endif

#endif // !USE_RIO_TRANSPORT
