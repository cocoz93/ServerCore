//
// RIO 전송 계층 — Registered I/O 기반 제출·수거 구현.
//   불변식: 한 세션의 RQ 조작(생성·제출·closesocket)은 소유 워커 스레드에서만 수행한다.
//   IOCP 빌드에서는 파일 전체가 비활성 — 빈 번역단위가 된다.
//
#include "IOCPServer.h"

#if USE_RIO_TRANSPORT

#include "Crash/CrashDump.h"   // CRASH 매크로 (CQ 오염 시 즉사+덤프)
#include <Common/ErrorLog.h>
#include <chrono>
#include <algorithm>
#include <cassert>

#include "CoreAffinity.h"

namespace
{
    // 슬랩 슬라이스 크기 — 기존 링 기본값 65535의 페이지 정렬판 (링 로직은 capacity 임의값 허용)
    constexpr size_t RIO_RECV_RING_SIZE = 65536;
    constexpr size_t RIO_SEND_RING_SIZE = 65536;
    constexpr ULONG  RIO_DEQUEUE_BATCH  = 256;    // CQ 드레인 1회 최대 회수 건수

    // RIORESULT.RequestContext 태그 — 완료가 RECV인지 SEND인지 구분 (세션은 SocketContext)
    //   [다중 pending] SEND는 하위 4비트에 종류, 그 위에 in-flight 슬롯 번호를 얹는다.
    //   IOCP 팔이 OverlappedEx.slot으로 슬롯을 알아내는 것과 같은 역할.
    constexpr ULONGLONG RIO_CTX_RECV = 1;
    constexpr ULONGLONG RIO_CTX_SEND = 2;
    constexpr ULONGLONG RIO_CTX_KIND_MASK  = 0xF;
    constexpr int       RIO_CTX_SLOT_SHIFT = 4;

    // 이 스레드가 몇 번 RIO 워커인가 (-1 = 워커 아님) — RQ 소유권 판정용
    thread_local int t_rioWorkerIndex = -1;
}

// ============================================================================
// 전송 계층 경계 — 공통 골격이 위임하는 지점 (선언은 IOCPServer.h)
// ============================================================================

// 세션 링버퍼 준비 — RIO는 등록 슬랩의 슬라이스를 쓴다 (TransportPostListen에서 InitExternal).
bool CIOCPServer::TransportInitSessionBuffer(CSession* session)
{
    (void)session;
    return true;
}

// 리슨 소켓 "전" 준비 — RIO는 없다. 함수 테이블 probe에 소켓이 필요해 리슨 "후"로 미룬다.
bool CIOCPServer::TransportPreListen()
{
    return true;
}

void CIOCPServer::TransportPreListenCleanup()
{
}

// 리슨 소켓 "후" 준비: 함수 테이블 → 슬랩 등록(물리 고정) → 세션 링 슬라이스 → 워커 객체(CQ/이벤트)
//   스레드 기동 전(pre-_running)에 실패 가능한 자원을 전부 확보한다.
bool CIOCPServer::TransportPostListen()
{
    bool rioReady = CRioApi::Load(_listenSocket);

    const size_t perSession = RIO_RECV_RING_SIZE + RIO_SEND_RING_SIZE;
    if (rioReady && !_rioSlab.Init(perSession * static_cast<size_t>(_maxClients)))
        rioReady = false;

    if (rioReady)
    {
        for (int i = 0; i < _maxClients; ++i)
        {
            char* base = _rioSlab.Base() + static_cast<size_t>(i) * perSession;
            if (!_sessions[i]->_recvQ.InitExternal(base, RIO_RECV_RING_SIZE) ||
                !_sessions[i]->_sendQ.InitExternal(base + RIO_RECV_RING_SIZE, RIO_SEND_RING_SIZE))
            {
                rioReady = false;
                break;
            }
        }
    }

    if (rioReady)
    {
        // INI RioWorkers (0=자동 2) — 상한은 송신 카운터 슬롯 수(flushUs/backlog 노출용)
        _rioWorkerCount = std::clamp((_configuredRioWorkers > 0) ? _configuredRioWorkers : 2,
                                     1, CMonitorManager::MAX_SEND_WORKERS);
        _monitor._sendWorkerCount.Store(_rioWorkerCount);   // FlushSend 배치 계측 노출 루프 상한
        _rioStop.store(false);
        _rioWorkers.clear();
        _rioWorkers.reserve(_rioWorkerCount);

        // CQ 크기 = (recv 1 + send 깊이)×MaxClients + 여유 — 세션이 한 워커에 몰리는 최악
        // 분배에서도 RIOCreateRequestQueue의 슬롯 예약이 실패하지 않게 워스트로 잡는다.
        //   (ceil(MaxClients/N)으로 줄이면 분배가 치우친 순간 RQ 생성이 실패한다)
        const DWORD cqSize = static_cast<DWORD>(_maxClients) * static_cast<DWORD>(1 + _sendDepth) + 16;
        for (int i = 0; i < _rioWorkerCount && rioReady; ++i)
        {
            auto worker = std::make_unique<RioWorker>();
            worker->cqEvent = CreateEventW(NULL, FALSE, FALSE, NULL);
            worker->cmdEvent = CreateEventW(NULL, FALSE, FALSE, NULL);
            if (worker->cqEvent != nullptr && worker->cmdEvent != nullptr)
            {
                RIO_NOTIFICATION_COMPLETION nc = {};
                nc.Type = RIO_EVENT_COMPLETION;
                nc.Event.EventHandle = worker->cqEvent;
                nc.Event.NotifyReset = TRUE;
                worker->cq = CRioApi::Rio().RIOCreateCompletionQueue(cqSize, &nc);
            }
            if (worker->cq == RIO_INVALID_CQ)
                rioReady = false;
            _rioWorkers.push_back(std::move(worker));
        }
    }

    if (!rioReady)
    {
        SLOG_ERROR("[RIO] init failed (table/slab/ring/cq) — WSAError: {}", WSAGetLastError());
        for (auto& worker : _rioWorkers)
        {
            if (worker->cq != RIO_INVALID_CQ)
                CRioApi::Rio().RIOCloseCompletionQueue(worker->cq);
            if (worker->cqEvent)
                CloseHandle(worker->cqEvent);
            if (worker->cmdEvent)
                CloseHandle(worker->cmdEvent);
        }
        _rioWorkers.clear();
        _rioSlab.Release();
        closesocket(_listenSocket);
        _listenSocket = INVALID_SOCKET;
        WSACleanup();
        return false;
    }
    return true;
}

// WSASocket 플래그 — accept 소켓이 REGISTERED_IO를 상속한다 (Phase 0 스모크 실측).
// 주의: 상속된 소켓은 RIO 전용 — 일반 WSASend/WSARecv가 거부된다(10038).
uint32_t CIOCPServer::TransportListenFlags() const
{
    return WSA_FLAG_REGISTERED_IO | WSA_FLAG_OVERLAPPED;
}

// RIO 워커 기동 — CQ/이벤트/슬랩은 TransportPostListen에서 확보 완료 (WT 풀·SendWorker 풀 대체)
void CIOCPServer::TransportStartWorkers()
{
    for (int i = 0; i < _rioWorkerCount; ++i)
        _rioWorkers[i]->thread = std::thread(&CIOCPServer::RioWorkerThread, this, i);
}

// 기동 완료 로그 — 스모크가 "RIO workers=" 문구로 팔(arm)을 판정한다 (변경 시 echo-smoke.ps1 동반 수정).
void CIOCPServer::TransportLogStarted(const char* modeName) const
{
    SLOG_INFO("[Network] Server started — RIO workers={}, slab={}MB, cq={}/worker (Mode: {})",
              _rioWorkerCount,
              ((RIO_RECV_RING_SIZE + RIO_SEND_RING_SIZE) * static_cast<size_t>(_maxClients)) >> 20,
              static_cast<DWORD>(_maxClients) * static_cast<DWORD>(1 + _sendDepth) + 16, modeName);
}

void CIOCPServer::TransportStopBeforeDrain()
{
    // RIO는 여기서 할 일이 없다 — 워커가 완료 드레인·closesocket의 주체라
    // 모든 세션 IOCount==0 이후에 세운다 (TransportStopAfterDrain).
}

// IOCount 드레인 "후" 정지 — 반드시 "모든 세션 IOCount==0" 이후여야 한다.
void CIOCPServer::TransportStopAfterDrain()
{
    // RIO 워커 정지 — 반드시 "모든 세션 IOCount==0" 이후여야 한다. 워커가 완료 드레인과
    // Disconnect(closesocket) 처리의 주체라, 먼저 세우면 pending ref·핸드오프 pin이 영영
    // 안 풀린다 (SendWorker를 IOCount 대기 "전"에 멈추는 기존 순서와 반대인 이유).
    _rioStop.store(true);
    for (auto& worker : _rioWorkers)
        SetEvent(worker->cmdEvent);
    for (auto& worker : _rioWorkers)
    {
        if (worker->thread.joinable())
            worker->thread.join();
    }
    for (auto& worker : _rioWorkers)
    {
        if (worker->cq != RIO_INVALID_CQ)
        {
            CRioApi::Rio().RIOCloseCompletionQueue(worker->cq);
            worker->cq = RIO_INVALID_CQ;
        }
        if (worker->cqEvent)
        {
            CloseHandle(worker->cqEvent);
            worker->cqEvent = nullptr;
        }
        if (worker->cmdEvent)
        {
            CloseHandle(worker->cmdEvent);
            worker->cmdEvent = nullptr;
        }
    }
    _rioWorkers.clear();
    _rioSlab.Release();   // 모든 소켓·CQ 정리 후, WSACleanup 전 (등록 해제 → VirtualFree)
}

// 완료 통지 연결 — RIO는 소유 워커가 RQ를 만들 때 CQ에 묶는다 (여기서 할 일 없음).
bool CIOCPServer::TransportAttachSession(CSession* session, Platform::NetSocket clientSocket)
{
    (void)session;
    (void)clientSocket;
    return true;
}

// 첫 Recv 착수 — RQ 생성까지 소유 워커에 위임한다.
void CIOCPServer::TransportStartFirstRecv(CSession* session, Platform::NetSocket clientSocket, int64_t sessionId)
{
    // RQ 생성+첫 Recv는 소유 워커에 위임 — RQ 생성이 CQ 상태를 바꾸는데 RIO는 내부 락이
    // 없어 소유 워커에서만 만져야 한다. CONNECTED push(호출부)가 먼저라 이벤트 순서도 보존.
    // Initialize의 IOCount=1(첫 Recv ref)이 핸드오프 동안 세션 수명을 보장한다.
    RioCmd cmd;
    cmd.type = RioCmd::Type::NewConn;
    cmd.sessionId = sessionId;
    cmd.socket = clientSocket;
    cmd.session = session;
    RioEnqueueCmd(RioOwnerIndex(sessionId), std::move(cmd));
}

// SendFlush::Immediate — RQ 불변식 때문에 비소유 스레드면 핸드오프로 변환한다.
void CIOCPServer::TransportSendImmediate(CSession* session, int64_t sessionId)
{
    // RQ 불변식: 제출은 소유 워커에서만. 소유 워커 위(에코 모드의 recv 처리 중)면 직접,
    // 비소유 스레드(게임 등)면 FlushSend 핸드오프로 변환 — "즉시"가 µs급 핸드오프로 바뀔 뿐
    // 계약(가능한 한 빨리 송신)은 유지된다.
    const int ownerIdx = RioOwnerIndex(sessionId);
    if (t_rioWorkerIndex == ownerIdx)
    {
        PostSend(session);
    }
    else if (InterlockedExchange(&session->_queuedForSend, TRUE) == FALSE)
    {
        RioCmd cmd;
        cmd.type = RioCmd::Type::FlushSend;
        cmd.sessionId = sessionId;
        RioEnqueueCmd(ownerIdx, std::move(cmd));
    }
}

// 틱 끝 dirty 배치 → 소유 워커별 FlushSend 명령 핸드오프 (게임 스레드 단독 호출)
void CIOCPServer::TransportFlushDirty()
{
    // dirty 배치를 소유 워커별 FlushSend 명령으로 핸드오프 — 분배(uniqueId%N)·잔류표식(_queuedForSend)
    // dedup 로직은 기존 SendWorker 경로 그대로. 워커별 1회 락 + 1회 SetEvent.
    thread_local std::vector<std::vector<int64_t>> perWorker;
    if (static_cast<int>(perWorker.size()) != _rioWorkerCount)
        perWorker.assign(_rioWorkerCount, {});
    else
        for (auto& v : perWorker)
            v.clear();

    for (CSession* session : _dirtySessions)
    {
        session->_sendDirty = false;                   // 게임 스레드 단독 접근 → 안전
        if (InterlockedExchange(&session->_queuedForSend, TRUE) == FALSE)
        {
            // sessionId는 exchange "후"에 읽는다 — 읽기~exchange 사이 slot 재사용 시 옛 id를
            //   push하면 워커가 FindSession으로 새 세션을 못 찾아 _queuedForSend가 TRUE인 채
            //   안 지워져 그 세션이 송신 mute. exchange 후 읽으면 push id=플래그 세운 세션이라 봉합.
            const int64_t sessionId = session->_sessionId; // volatile → 일반 복사 후 사용
            perWorker[RioOwnerIndex(sessionId)].push_back(sessionId);
        }
    }
    _dirtySessions.clear();

    for (int i = 0; i < _rioWorkerCount; ++i)
    {
        if (perWorker[i].empty())
            continue;
        RioWorker& worker = *_rioWorkers[i];
        {
            std::lock_guard<std::mutex> lk(worker.cmdMutex);
            for (int64_t id : perWorker[i])
            {
                RioCmd cmd;
                cmd.type = RioCmd::Type::FlushSend;
                cmd.sessionId = id;
                worker.cmdQueue.push_back(cmd);
            }
        }
        SetEvent(worker.cmdEvent);
    }
}

// 종료 유도 — CancelIoEx가 없다. 소유 워커의 closesocket이 그 역할 (Phase 0 스모크 실측).
bool CIOCPServer::TransportRequestDisconnect(CSession* session)
{
    // pin을 "먼저" 확보 — _disconnecting을 먼저 세우면 AcquireSession이 스스로 실패한다.
    // 이 pin이 세션 재사용(ABA)을 막은 채로 Disconnect 명령에 실려 소유 워커까지 간다.
    // (sessionId 재조회 방식은 FindSession이 _disconnecting 세션을 숨겨 closesocket이
    //  누락되고 pending recv의 IOCount가 영영 안 풀린다 — 그래서 포인터+pin 핸드오프)
    if (!AcquireSession(session))
        return false;   // 이미 해제(IOCount 0)·해제 진행 중 — 종료 유도 불필요

    if (InterlockedExchange(&session->_disconnecting, TRUE) == TRUE)
    {
        IOCountDecrement(session);   // 다른 스레드가 이미 종료 처리 중
        return false;
    }

    const int64_t sessionId = session->_sessionId;   // pin 보유 중 → 유효
    const int ownerIdx = RioOwnerIndex(sessionId);
    if (t_rioWorkerIndex == ownerIdx)
    {
        RioCloseSocketOnOwner(session);   // 소유 워커 자신 — 즉시 닫기
        IOCountDecrement(session);
    }
    else
    {
        RioCmd cmd;
        cmd.type = RioCmd::Type::Disconnect;
        cmd.sessionId = sessionId;
        cmd.session = session;
        RioEnqueueCmd(ownerIdx, std::move(cmd));
    }
    return true;
}

// ============================================================================
// RIO 본체 — 워커 · 명령 · 제출 경로
// ============================================================================

// ==========================================================================
// RIO 전송 계층 — 워커·명령·제출 경로
//
// 불변식: 한 세션의 RQ 조작(RIOCreateRequestQueue·RIOReceive·RIOSend·closesocket)은
//         소유 워커(uniqueId % N) 스레드에서만 수행한다. RIO에는 CancelIoEx가 없으므로
//         "제출과 closesocket의 직렬화"가 취소를 대체한다 (Phase 0 스모크: closesocket 시
//         pending 요청이 에러 완료로 CQ에 도착 — 이것이 IOCount 수렴 수단).
// ==========================================================================

// 외부 스레드(게임/accept/타이머)가 소유 워커에 명령을 넘긴다.
void CIOCPServer::RioEnqueueCmd(int ownerIdx, RioCmd&& cmd)
{
    RioWorker& worker = *_rioWorkers[ownerIdx];
    {
        std::lock_guard<std::mutex> lk(worker.cmdMutex);
        worker.cmdQueue.push_back(cmd);
    }
    SetEvent(worker.cmdEvent);
}

// 소유 워커 전용 closesocket — pending RIO 요청을 에러 완료로 밀어내 IOCount 수렴을 유도.
void CIOCPServer::RioCloseSocketOnOwner(CSession* session)
{
    SOCKET socket = session->_socket;
    session->_socket = INVALID_SOCKET;   // ReleaseSession의 Close()가 이중 close하지 않도록 선마킹
    if (socket != INVALID_SOCKET)
        closesocket(socket);
}

// 명령 1건 처리 (소유 워커 위에서만 호출)
void CIOCPServer::RioHandleCmd(RioWorker& worker, RioCmd& cmd)
{
    switch (cmd.type)
    {
    case RioCmd::Type::NewConn:
    {
        // Initialize 완료 세션 — IOCount=1(첫 Recv ref)이 수명을 보장한다.
        CSession* session = cmd.session;

        // [레이스 방어] Disconnect가 NewConn을 추월한 경우 — CONNECTED push~NewConn 핸드오프
        // 틈에 게임 틱이 끼면 OnConnected 킥(중복접속·채널고갈·존만원)이 Disconnect를 먼저
        // enqueue할 수 있다. 소유 워커 직렬화로 이미 closesocket됐다면 cmd.socket은 stale
        // 핸들이고, OS가 그 값을 재사용했다면 "남의 살아있는 소켓"에 유령 RQ를 만들게 된다
        // (소켓당 RQ 1개 → 무고한 신규 접속의 정당한 RQ 생성까지 실패해 즉사).
        // 같은 워커에서 직렬화되므로 이 검사에는 레이스가 없다.
        if (session->_disconnecting == TRUE || session->_socket == INVALID_SOCKET)
        {
            IOCountDecrement(session);   // 첫 Recv ref 반환 → (Disconnect pin 소진 후) Release 수렴
            break;
        }

        // 요청 큐 깊이 — recv는 1로 둔다(요청마다 고정 범위를 받는 구조라 다중 pending을 쓰면
        //   스트림 연속성이 깨진다. 수신 다중화는 별도 재설계 대상).
        //   send는 송신 깊이만큼 열어야 한다 — MaxOutstandingSend가 1이면 두 번째 RIOSend가
        //   한도 초과로 실패해 깊이를 올린 의미가 없다.
        session->_rq = CRioApi::Rio().RIOCreateRequestQueue(cmd.socket,
                                                            1, 1,                                 // recv: outstanding 1, 버퍼 1
                                                            static_cast<ULONG>(_sendDepth), 1,    // send: outstanding=깊이, 버퍼 1
                                                            worker.cq, worker.cq, session);
        if (session->_rq == RIO_INVALID_RQ)
        {
            const int wsaErr = WSAGetLastError();
            SLOG_ERROR("[RIO] RIOCreateRequestQueue failed: {} (sessionId={})", wsaErr, cmd.sessionId);
            // BindIOCP 실패 경로와 동일 구조 — 종료 유도 + 첫 Recv ref 반환
            RequestDisconnectSession(session);
            IOCountDecrement(session);
            break;
        }
        PostRecv(session, true);   // 첫 Recv — Initialize의 IOCount=1을 그대로 사용
        break;
    }
    case RioCmd::Type::FlushSend:
    {
        CSession* session = FindSession(cmd.sessionId);   // id 일치·미종료 재검증 (기존 SendWorker와 동일)
        if (session)
        {
            // [레이스 방어] NewConn보다 먼저 도착한 FlushSend — ProcessAccept의 CONNECTED push와
            // NewConn 핸드오프 사이 µs 틈에 게임 틱 경계가 끼면, 생산자가 다르므로(게임 vs accept)
            // 이 워커 큐에 FlushSend가 먼저 들어올 수 있다. RQ 미생성 상태를 치명으로 처리하면
            // 신생 세션이 즉사하므로, 자기 큐 꼬리로 재투입해 NewConn 처리 뒤에 송신한다.
            // (NewConn이 RQ 생성에 실패하면 _disconnecting → FindSession이 걸러 재투입 종료 보장)
            if (session->_rq == RIO_INVALID_RQ)
            {
                RioCmd retry = cmd;
                RioEnqueueCmd(t_rioWorkerIndex, std::move(retry));
                break;
            }
            // 잔류 표식 해제는 송신 처리 "전" — 처리 도중 도착한 데이터가 다시 큐에 들어가
            // 누락되지 않게 (기존 SendWorkerThread의 _queuedForSend 주석 로직 그대로).
            InterlockedExchange(&session->_queuedForSend, FALSE);
            PostSend(session);
        }
        break;
    }
    case RioCmd::Type::Disconnect:
    {
        // 요청 스레드가 pin(IOCount+1)을 잡고 넘긴 포인터 — 재사용(ABA) 불가가 보장된다.
        RioCloseSocketOnOwner(cmd.session);
        IOCountDecrement(cmd.session);   // 핸드오프 pin 반환 (0 도달 시 ReleaseSession)
        break;
    }
    }
}

// CQ 한 배치 처리. 반환: 처리 건수, RIO_CORRUPT_CQ면 -1.
int CIOCPServer::RioDrainCompletions(RioWorker& worker, int monitorIndex)
{
    RIORESULT results[RIO_DEQUEUE_BATCH];
    const ULONG n = CRioApi::Rio().RIODequeueCompletion(worker.cq, results, RIO_DEQUEUE_BATCH);
    if (n == RIO_CORRUPT_CQ)
    {
        // CQ 오염 = 메모리 오염 시그널. 워커만 죽이면 이 파티션 세션들의 IOCount가 영영 안 풀려
        // 셧다운 무한 대기(조용한 좀비 서버)가 된다 — 오염 상태 지속보다 즉사+덤프가 낫다.
        SLOG_ERROR("[RIO] RIODequeueCompletion returned RIO_CORRUPT_CQ — crashing for dump");
        CRASH("RIO completion queue corrupted (RIO_CORRUPT_CQ)");
        return -1;   // 도달 불가 (CRASH 미복귀) — 컴파일러용
    }

    // [계측] 수거 호출 횟수 — RIO는 유저모드 링 조회라 syscall이 아니지만, 배치 효율(완료수/호출수)을
    //   IOCP 팔과 같은 잣대로 읽으려고 같은 카운터에 센다. 빈 조회(n=0)도 호출로 친다.
    if (monitorIndex >= 0 && monitorIndex < CMonitorManager::MAX_WORKER_THREADS)
        _monitor._workerCounters[monitorIndex].AddDequeue();

    for (ULONG i = 0; i < n; ++i)
    {
        auto session = reinterpret_cast<CSession*>(static_cast<uintptr_t>(results[i].SocketContext));
        const ULONGLONG ctx = results[i].RequestContext;
        const bool isRecv = ((ctx & RIO_CTX_KIND_MASK) == RIO_CTX_RECV);
        const int sendSlot = static_cast<int>(ctx >> RIO_CTX_SLOT_SHIFT);
        const DWORD bytes = results[i].BytesTransferred;

        // 기존 WorkerThread의 완료 판정과 동일: 에러·0바이트·종료 중이면 종료 유도만
        const bool canProcess = (results[i].Status == NO_ERROR && bytes != 0 &&
                                 session->_disconnecting == FALSE);
        if (canProcess)
        {
            if (isRecv)
                ProcessRecv(session, bytes);
            else
                ProcessSend(session, bytes, sendSlot);
        }
        else
        {
            RequestDisconnectSession(session);
        }

        IOCountDecrement(session);   // 이 완료가 들고 있던 pending IO ref 반환

        if (monitorIndex >= 0 && monitorIndex < CMonitorManager::MAX_WORKER_THREADS)
            _monitor._workerCounters[monitorIndex].AddCompletion();
    }
    return static_cast<int>(n);
}

// RIO 워커 — 자기 CQ와 소유 세션(uniqueId % N)의 모든 RQ 조작을 전담한다.
// 루프: 명령 드레인 → CQ 드레인 → (유휴) RIONotify 무장 → 재드레인 → 이벤트 대기.
// v1은 스핀 없이 notify+대기 — 부하클라 동거 머신에서 코어 소모를 피하고, 실측 후 필요 시 추가.
void CIOCPServer::RioWorkerThread(int workerIdx)
{
    CoreAffinity::PinIoThread();   // RIO 워커도 I/O — 게임코어 밖으로 (현 빌드 USE_RIO_TRANSPORT=0라 미컴파일)

    RioWorker& worker = *_rioWorkers[workerIdx];
    t_rioWorkerIndex = workerIdx;

    // [계측] CPU 점유율 측정용 — 기존 IOCP 워커와 동일 패턴 (슬롯 등록 + 실핸들 복제)
    const int monitorIndex = _monitor.RegisterWorkerThread();
    if (monitorIndex >= 0 && monitorIndex < CMonitorManager::MAX_WORKER_THREADS)
    {
        _monitor._workerCounters[monitorIndex].threadHandle = Platform::CaptureCurrentThreadCpu();
    }

    std::vector<RioCmd> localCmds;
    HANDLE waitHandles[2] = { worker.cqEvent, worker.cmdEvent };

    while (true)
    {
        bool didWork = false;

        // ── 1) 명령 드레인 (게임/accept/타이머 → 이 워커) ──
        {
            std::lock_guard<std::mutex> lk(worker.cmdMutex);
            localCmds.swap(worker.cmdQueue);
        }
        if (!localCmds.empty())
        {
            didWork = true;

            // [계측] FlushSend 배치 처리 시간·건수 — 기존 SendWorker의 flushUs/backlog 의미 승계.
            //   슬롯당 단독 writer(이 워커). NewConn/Disconnect가 섞이면 근사치지만 지배 항목은 FlushSend.
            LONG64 flushCount = 0;
            for (const RioCmd& cmd : localCmds)
            {
                if (cmd.type == RioCmd::Type::FlushSend)
                    ++flushCount;
            }
            const auto cmdT0 = std::chrono::steady_clock::now();

            for (RioCmd& cmd : localCmds)
                RioHandleCmd(worker, cmd);

            const auto cmdT1 = std::chrono::steady_clock::now();
            if (workerIdx >= 0 && workerIdx < CMonitorManager::MAX_SEND_WORKERS)
            {
                _monitor._sendCounters[workerIdx].backlog.Store(flushCount);
                _monitor._sendCounters[workerIdx].flushUs.Add(std::chrono::duration_cast<std::chrono::microseconds>(cmdT1 - cmdT0).count());
            }
            localCmds.clear();
        }

        // ── 2) CQ 드레인 — 유저모드 공유 링에서 완료 회수 (시스콜 없음) ──
        int drained = RioDrainCompletions(worker, monitorIndex);
        if (drained < 0)
            break;                    // CQ 손상 — 복구 불가
        if (drained > 0)
            didWork = true;

        if (didWork)
            continue;                 // 일감이 있었다 — 대기 없이 재순회

        // ── 3) 유휴 — 정지 확인 → notify 무장 → 재드레인(무장 전 도착분 회수) → 대기 ──
        // 셧다운은 모든 세션 IOCount==0 이후에만 정지시키므로, 여기 도달 시 잔여 작업이 없다.
        if (_rioStop.load())
            break;

        (void)CRioApi::Rio().RIONotify(worker.cq);   // 중복 무장 안전 (Phase 0 실측 — 0 반환)
        drained = RioDrainCompletions(worker, monitorIndex);
        if (drained < 0)
            break;
        if (drained > 0)
            continue;
        WaitForMultipleObjects(2, waitHandles, FALSE, INFINITE);
    }
}

// Recv I/O 제출 (RIO 판) — 링버퍼 "직선 구간"만 제출한다.
// RIO는 요청당 버퍼가 1개(WSABUF 스캐터 불가)라 감긴 꼬리는 다음 완료 후 제출이 잇는다.
// skipAcquire=true: NewConn 처리의 첫 Recv — Initialize의 IOCount=1을 그대로 사용.
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

    char* writePtr = session->_recvQ.GetWritePtr();
    size_t directWriteSize = session->_recvQ.GetDirectWriteSize();

    if (directWriteSize == 0)
    {
        // 직선 0 ⇔ 링 가득 (GetDirectWriteSize 정의상 동치) — 기존과 동일한 overflow 정책
        _monitor._recvBufferOverflow.Inc();
        LOG_ERROR_STREAM("[Error] Recv buffer full - SessionId: " << sessionId);
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return;
    }

    if (session->_disconnecting == TRUE)
    {
        // 소유 워커 직렬화로 "닫힌 RQ에 제출" 레이스는 없지만, 불필요한 제출은 걸러낸다.
        IOCountDecrement(session);
        return;
    }

    if (session->_rq == RIO_INVALID_RQ)
    {
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return;
    }

    RIO_BUF buf;
    buf.BufferId = _rioSlab.BufferId();
    buf.Offset = _rioSlab.OffsetOf(writePtr);
    buf.Length = static_cast<ULONG>(directWriteSize);

    _monitor._wsaRecvCalls.Inc();
    if (!CRioApi::Rio().RIOReceive(session->_rq, &buf, 1, 0,
                                   reinterpret_cast<void*>(static_cast<uintptr_t>(RIO_CTX_RECV))))
    {
        const int wsaErr = WSAGetLastError();
        if (!shared::ShouldIgnoreWsaError(wsaErr))
        {
            LOG_WSA_ERROR_STREAM("RIOReceive failed - SessionId: " << sessionId << ", WSAError: ", wsaErr);
        }
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return;
    }
    // post-check(CancelIoEx) 불필요 — 제출과 closesocket이 같은 소유 워커에서 직렬화된다.
}

// 미제출 구간의 직선 부분만 내보낸다 — RIO는 요청당 버퍼가 1개다(WSABUF 스캐터 불가).
//   감긴 꼬리는 완료 후 double-check가 이어 보낸다 (in-flight 슬롯 1개 소비).
int CIOCPServer::TransportSubmitSegment(CSession* session, Platform::NetSocket socket,
                                        const CRingBufferMT::SubmitInfo& info,
                                        int slot, int slotsAvailable, size_t* submittedBytes)
{
    (void)socket;           // RIO는 소켓이 아니라 세션의 RQ에 제출한다
    (void)slotsAvailable;   // 직선 구간 한 건이라 항상 슬롯 1개

    if (session->_rq == RIO_INVALID_RQ)
        return 0;

    const size_t len = info.directSize;

    // [계측] 랩 때문에 직선 구간만 싣는 횟수 — 꼬리는 완료를 기다려 재제출하므로 왕복 1회가 붙는다.
    //   이 빈도가 낮으면 배치 제출(RIO_MSG_DEFER)을 넣을 값어치가 없다는 근거가 된다.
    //   (IOCP 팔은 랩을 WSABUF 2개로 한 번에 보내 이 왕복이 없으므로 세지 않는다)
    if (info.size > len)
        _monitor._sendWrapSplits.Inc();

    // 슬롯에 제출량을 심는다 — 완료가 부분 송신 판정에 읽고, 회수가 링 반환량으로 쓴다.
    //   제출 "전"에 심어야 한다(제출 직후 완료가 먼저 처리될 수 있다).
    CSession::SendSlot& s = session->_sendSlots[slot];
    s.bytes = len;

    // 제출 "전"에 경계를 옮긴다 (IOCP 판과 같은 이유 — 완료가 경계를 먼저 봐야 한다)
    if (session->_sendQ.MarkSubmitted(len) != len)
    {
        s.bytes = 0;
        return 0;
    }

    RIO_BUF buf;
    buf.BufferId = _rioSlab.BufferId();
    buf.Offset = _rioSlab.OffsetOf(info.submitPtr);
    buf.Length = static_cast<ULONG>(len);

    // RequestContext에 종류 + 슬롯 번호를 실어 보낸다 (완료가 자기 슬롯을 알아야 한다)
    const ULONGLONG ctx = RIO_CTX_SEND | (static_cast<ULONGLONG>(slot) << RIO_CTX_SLOT_SHIFT);

    _monitor._wsaSendCalls.Inc();
    if (!CRioApi::Rio().RIOSend(session->_rq, &buf, 1, 0,
                                reinterpret_cast<void*>(static_cast<uintptr_t>(ctx))))
    {
        const int wsaErr = WSAGetLastError();
        if (!shared::ShouldIgnoreWsaError(wsaErr))
        {
            LOG_WSA_ERROR_STREAM("RIOSend failed - SessionId: " << session->_sessionId
                << ", WSAError: ", wsaErr);
        }
        return 0;   // 호출자(PostSend)가 세션 종료를 유도한다 — 기존 처리와 동일
    }
    // post-check(CancelIoEx) 불필요 — 소유 워커 직렬화가 대체 (PostRecv와 동일)

    *submittedBytes = len;
    return 1;
}

#endif // USE_RIO_TRANSPORT
