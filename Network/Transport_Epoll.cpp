//
// epoll 전송 계층 — 리눅스 백엔드. 공통 골격(IOCPServer.cpp)이 Transport* 위임 지점으로만 호출한다.
//   Windows 빌드에서는 파일 전체가 비활성 — 빈 번역단위가 된다.
//
//   [IOCP와 무엇이 다른가]
//     IOCP : "이 버퍼로 보내/받아 달라"고 걸어두면 끝났을 때 완료를 돌려준다 (proactor).
//     epoll: "이 fd가 읽을/쓸 수 있게 되면 알려 달라"고 등록하면 준비 상태를 알려준다 (reactor).
//   그래서 완료 통지에 실려오던 OVERLAPPED·전송 바이트 수가 없고, 준비 통지를 받은 쪽이
//   직접 read/write를 호출해 실제 크기를 그 자리에서 얻는다.
//
//   [현재 범위 — 4-D 뼈대]
//     연결 수락·등록·해제까지. 수신 경로는 4-F, 송신 경로(EPOLLOUT 재설계)는 4-G에서 채운다.
//
#include "IOCPServer.h"

#ifndef _WIN32

#include <Common/ErrorLog.h>
#include "CoreAffinity.h"

#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>   // writev (링이 감긴 두 조각을 한 번에)
#include <chrono>
#include <algorithm>

namespace
{
    // epoll_wait 한 번에 회수할 이벤트 상한 — 너무 크면 한 워커가 오래 붙잡고,
    //   너무 작으면 syscall 횟수가 는다. IOCP의 완료 배치 상한과 같은 성격의 값.
    constexpr int EPOLL_EVENT_BATCH = 256;

    // epoll_wait 타임아웃(ms) — 정지 플래그를 확인할 주기다.
    //   무한 대기로 두면 종료 시 워커가 안 깨어난다(IOCP는 PostQueuedCompletionStatus로 깨웠다).
    constexpr int EPOLL_WAIT_TIMEOUT_MS = 100;

    // 논블로킹 전환 — reactor 모델에서는 필수다. 준비 통지를 받고 read를 불러도
    //   그 사이 다른 워커가 먼저 가져갔으면 블로킹될 수 있다.
    bool SetNonBlocking(int fd)
    {
        const int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags < 0)
            return false;
        return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
    }
}

// ============================================================================
// 전송 계층 경계 — 공통 골격이 위임하는 지점 (선언은 IOCPServer.h)
// ============================================================================

// 세션 링버퍼 준비 — epoll은 세션마다 자기 힙 버퍼를 갖는다 (IOCP 팔과 동일).
bool CIOCPServer::TransportInitSessionBuffer(CSession* session)
{
#if USE_LOCKFREE_SENDQ
    return session->_recvQ.Init() && session->_sendQ.Init(256);
#else
    return session->_recvQ.Init() && session->_sendQ.Init();
#endif
}

// 리슨 소켓 "전" 준비 — epoll 인스턴스를 만든다. accept 소켓을 여기에 등록한다.
bool CIOCPServer::TransportPreListen()
{
    // 워커 수 산정 — affinity 가용 코어 수가 기준인 것은 IOCP 팔과 같다.
    //   다만 epoll에는 "동시 실행 워커 상한(concurrency)" 개념이 없다. 커널이 완료를
    //   깨우는 수를 조절해 주던 IOCP와 달리, 여기서는 등록한 워커 전부가 자유롭게 돈다.
    _coreCount = Platform::GetAvailableCoreCount();
    _workerThreadCount = (_configuredWorkers > 0) ? _configuredWorkers : _coreCount;

    _epollFd = ::epoll_create1(EPOLL_CLOEXEC);
    return (_epollFd >= 0);
}

void CIOCPServer::TransportPreListenCleanup()
{
    if (_epollFd >= 0)
    {
        ::close(_epollFd);
        _epollFd = -1;
    }
}

// 리슨 소켓 "후" 준비 — epoll은 할 일이 없다 (RIO만 함수 테이블·슬랩·CQ가 필요).
bool CIOCPServer::TransportPostListen()
{
    return true;
}

// 소켓 생성 플래그 — 리눅스에는 WSASocket의 플래그 개념이 없다(골격이 값을 버린다).
uint32_t CIOCPServer::TransportListenFlags() const
{
    return 0;
}

// 워커 기동 — epoll_wait 루프를 도는 스레드들.
void CIOCPServer::TransportStartWorkers()
{
    for (int i = 0; i < _workerThreadCount; ++i)
        _workerThreads.emplace_back(&CIOCPServer::EpollWorkerThread, this, i);
}

// 기동 완료 로그 — 스모크가 이 문구로 팔(arm)을 판정한다.
void CIOCPServer::TransportLogStarted(const char* modeName) const
{
    SLOG_INFO("[Network] Server started — epoll workers={} (affinity cores={}, Mode: {})",
              _workerThreadCount, _coreCount, modeName);
}

// IOCount 드레인 "전" 정지 — 4-G에서 송신 워커가 생기면 여기서 멈춘다.
void CIOCPServer::TransportStopBeforeDrain()
{
}

// IOCount 드레인 "후" 정지 — 워커 종료·epoll 해제.
//   IOCP는 PostQueuedCompletionStatus로 워커를 깨웠지만, epoll에는 그런 "가짜 완료"가 없다.
//   대신 epoll_wait에 타임아웃을 줘 주기적으로 _running을 확인하게 했다.
void CIOCPServer::TransportStopAfterDrain()
{
    for (auto& thread : _workerThreads)
    {
        if (thread.joinable())
            thread.join();
    }
    _workerThreads.clear();

    if (_epollFd >= 0)
    {
        ::close(_epollFd);
        _epollFd = -1;
    }
}

// 통지 연결 — accept한 소켓을 epoll에 등록한다.
//   IOCP의 BindIOCP에 대응하지만, 여기서는 "무엇을 알려줄지"(EPOLLIN)까지 같이 정한다.
bool CIOCPServer::TransportAttachSession(CSession* session, Platform::NetSocket clientSocket)
{
    if (!SetNonBlocking(clientSocket))
    {
        SLOG_ERROR("[Network] SetNonBlocking failed: {}", Platform::LastSocketError());
        return false;
    }

    epoll_event ev{};
    ev.events   = EPOLLIN | EPOLLRDHUP;   // 읽기 준비 + 상대 종료
    ev.data.ptr = session;                // 통지에서 세션을 바로 찾는다 (IOCP의 CompletionKey와 같은 역할)

    // 등록 표식은 ADD "전"에 세운다 — ADD가 성공한 순간부터 통지가 올 수 있는데, 그 통지로
    //   먼저 종료에 들어간 워커가 표식을 아직 못 보면 등록 ref를 놓지 않아 IOCount가 1에 갇힌다
    //   (세션 인덱스·소켓이 영영 안 돌아옴). 실패 시 되돌리는 것은 안전하다 — 등록 전에는
    //   통지가 없고, 컨텐츠 통보·타이밍 휠 등록도 attach 뒤라 이 창을 볼 스레드가 없다.
    InterlockedExchange(&session->_epollRegistered, TRUE);

    if (::epoll_ctl(_epollFd, EPOLL_CTL_ADD, clientSocket, &ev) != 0)
    {
        SLOG_ERROR("[Network] epoll_ctl(ADD) failed: {}", Platform::LastSocketError());
        InterlockedExchange(&session->_epollRegistered, FALSE);
        return false;
    }
    return true;
}

// 첫 Recv 착수 — epoll에는 "수신을 걸어둔다"는 개념이 없다.
//   TransportAttachSession의 EPOLLIN 등록이 그 자리를 대신하므로 여기서 할 일이 없다.
void CIOCPServer::TransportStartFirstRecv(CSession* session, Platform::NetSocket clientSocket, int64_t sessionId)
{
    (void)session;
    (void)clientSocket;
    (void)sessionId;
}

// 세션의 미전송 구간을 커널에 넘긴다 — epoll 송신의 본체.
//
//   [IOCP와 다른 점] IOCP는 WSASend를 "걸어두고" 완료 통지를 기다렸다. 여기서는 writev가
//   그 자리에서 보낸 바이트를 돌려주므로 제출과 완료가 한 호출에서 끝난다. 그래서 골격의
//   슬롯 링·_sendInFlight(다중 pending 장부)를 쓰지 않는다 — 이미 끝난 일을 적을 곳이 없다.
//
//   [부분 전송] 커널 송신 버퍼가 차면 writev가 요청보다 적게 보내거나 EAGAIN을 준다.
//   그때만 EPOLLOUT을 걸어 "보낼 수 있게 되면 알려 달라"고 부탁하고, 다 보내면 즉시 해제한다.
//   평시에 EPOLLOUT을 걸어 두면 보낼 것이 없어도 통지가 계속 와 워커가 헛돈다.
void CIOCPServer::EpollSendSession(CSession* session)
{
    if (!AcquireSession(session))
        return;

    // 제출 구간 계산과 실제 전송을 한 스레드로 직렬화한다.
    //   게임 스레드(flush)와 epoll 워커(EPOLLOUT)가 같은 세션을 동시에 만질 수 있어,
    //   이 잠금이 없으면 두 writev가 링의 같은 구간을 겹쳐 보낼 수 있다(순서 붕괴).
    if (InterlockedExchange(&session->_sendSubmitBusy, TRUE) == TRUE)
    {
        _monitor._sendContention.Inc();
        IOCountDecrement(session);
        return;
    }

    bool disconnect = false;

    // 제출 라운드 — 해제 후 재확인이 남은 것을 찾으면 한 번 더 돈다.
    while (true)
    {
        bool wantMore = false;      // 아직 남았다 → EPOLLOUT 유지/등록
        bool sawEmpty = false;      // 링이 비어서 끝남 — 이때만 재확인

        while (true)
        {
            if (session->_disconnecting == TRUE)
                break;

            auto info = session->_sendQ.GetSubmitInfo();
            if (info.size == 0)
            {
                sawEmpty = true;
                break;                                  // 보낼 것 없음
            }

            const Platform::NetSocket sock = session->_socket;
            if (sock == Platform::kInvalidSocket)
            {
                disconnect = true;
                break;
            }

            // 링이 감긴 경우 두 조각을 한 번의 syscall로 보낸다 (IOCP가 WSABUF 2개를 쓰던 것과 같은 이유).
            iovec iov[2];
            int iovCount = 0;
            iov[0].iov_base = info.submitPtr;
            iov[0].iov_len  = info.directSize;
            iovCount = 1;
            if (info.size > info.directSize)
            {
                iov[1].iov_base = session->_sendQ._buffer;          // 링 시작으로 감긴 뒷부분
                iov[1].iov_len  = info.size - info.directSize;
                iovCount = 2;
            }

            const ssize_t n = ::writev(sock, iov, iovCount);

            if (n > 0)
            {
                _monitor._wsaSendCalls.Inc();
                _monitor._sendBytes.Add(static_cast<LONG64>(n));

                // 제출 경계와 읽기 위치를 함께 전진 — 즉시 완료 모델이라 두 단계가 붙어 있다.
                session->_sendQ.MarkSubmitted(static_cast<size_t>(n));
                session->_sendQ.ConsumeSubmitted(static_cast<size_t>(n));

                if (static_cast<size_t>(n) < info.size)
                {
                    wantMore = true;                    // 커널 버퍼가 찼다 — 나머지는 EPOLLOUT 뒤에
                    break;
                }
                continue;                               // 다 보냈다 — 그 사이 더 쌓였을 수 있다
            }

            if (n < 0)
            {
                const int err = Platform::LastSocketError();
                if (Platform::WouldBlock(err))
                {
                    wantMore = true;                    // 지금은 못 보낸다 — 정상
                    break;
                }
                if (err == EINTR)
                    continue;

                SLOG_ERROR("[Network] writev failed: {}", err);
                disconnect = true;
                break;
            }

            break;                                      // n == 0 — 보낼 것이 없었다
        }

        // 표식 반영은 잠금 안에서 — 밖이면 두 제출자의 계산·반영 순서가 뒤집혀
        //   잔여가 남은 채 EPOLLOUT이 꺼질 수 있다(_epollWantWrite 불변식).
        if (!disconnect)
            EpollUpdateWriteInterest(session, wantMore);

        InterlockedExchange(&session->_sendSubmitBusy, FALSE);

        if (!sawEmpty)
            break;      // 재확인은 빈 링 종료만 — 부분 전송 잔여는 EPOLLOUT이 잇는다

        // [해제 후 재확인] 잠금 중 enqueue하고 튕긴 데이터는 아무도 안 보낸다(epoll엔 완료 통지의
        //   이어보내기가 없다). 남았으면 다시 잡아 한 라운드 더 — 못 잡으면 새 보유자가 본다.
        if (session->_sendQ.GetSubmitInfo().size == 0)
            break;
        if (InterlockedExchange(&session->_sendSubmitBusy, TRUE) == TRUE)
            break;
    }

    // 종료 유도를 ref 반환보다 "먼저" 한다 — 순서가 바뀌면 이 감소가 마지막 ref였을 때
    //   여기서 세션이 해제·재사용되고(인덱스 반환 → 새 접속이 그 자리 차지), 뒤이은
    //   RequestDisconnectSession이 방금 들어온 새 세션을 끊는다(무고한 접속 강제 종료).
    //   내 pin이 살아 있는 동안 끊으면 IOCount≥1이라 해제가 일어날 수 없다. IOCP 팔의
    //   PostSend(needDisconnect 먼저 → IOCountDecrement 나중)와 같은 순서로 통일한다.
    if (disconnect)
        RequestDisconnectSession(session);

    IOCountDecrement(session);
}

// EPOLLOUT 관심 등록/해제 — 보낼 것이 남았을 때만 켠다.
void CIOCPServer::EpollUpdateWriteInterest(CSession* session, bool wantWrite)
{
    if (session->_epollWantWrite == wantWrite)
        return;                                     // 상태가 같으면 syscall을 아낀다

    const Platform::NetSocket sock = session->_socket;
    if (sock == Platform::kInvalidSocket)
        return;

    epoll_event ev{};
    ev.events   = EPOLLIN | EPOLLRDHUP | (wantWrite ? EPOLLOUT : 0u);
    ev.data.ptr = session;

    if (::epoll_ctl(_epollFd, EPOLL_CTL_MOD, sock, &ev) == 0)
        session->_epollWantWrite = wantWrite;
}

// SendFlush::Immediate — 호출 스레드에서 곧바로 내보낸다.
void CIOCPServer::TransportSendImmediate(CSession* session, int64_t sessionId)
{
    (void)sessionId;
    EpollSendSession(session);
}

// 틱 끝 dirty 배치 → 송신 (게임 스레드 단독 호출)
//   [코얼레싱] 틱 동안 쌓인 것을 세션당 한 번의 writev로 내보낸다. 이 묶음이 syscall 수를
//   결정하므로, 모델이 IOCP든 epoll이든 유지할 가치가 있는 구조다.
void CIOCPServer::TransportFlushDirty()
{
    for (CSession* session : _dirtySessions)
    {
        session->_sendDirty = false;                // 게임 스레드 단독 접근
        EpollSendSession(session);
    }
    _dirtySessions.clear();
}

// 종료 유도 — IOCP는 CancelIoEx로 걸린 I/O를 취소했지만, epoll에는 취소할 "걸린 I/O"가 없다.
//   등록을 빼고 shutdown으로 상대에게 알리면, 남은 통지는 워커가 정리한다.
bool CIOCPServer::TransportRequestDisconnect(CSession* session)
{
    // 다른 스레드가 이미 처리 중이면 여기서 멈춘다 (IOCP 팔과 같은 게이트).
    if (InterlockedExchange(&session->_disconnecting, TRUE) == TRUE)
        return false;

    const Platform::NetSocket sock = session->_socket;
    if (sock != Platform::kInvalidSocket)
    {
        ::epoll_ctl(_epollFd, EPOLL_CTL_DEL, sock, nullptr);   // 더 이상 통지받지 않는다
        ::shutdown(sock, SHUT_RDWR);                           // 상대에게 종료를 알린다
    }

    // [IOCP와 결정적으로 다른 곳]
    //   IOCP는 CancelIoEx가 걸려 있던 I/O를 실패로 완료시키고, 그 완료를 받은 워커가
    //   IOCountDecrement를 불러 IOCount를 0으로 수렴시킨다 — 즉 "완료 통지"가 ref를 놓는다.
    //   epoll에는 걸린 I/O가 없어 그런 통지가 영영 오지 않는다. 세션을 만들 때 세운
    //   IOCount=1은 여기서는 "epoll에 등록되어 있다"는 뜻이므로, 등록을 빼는 이 자리에서
    //   직접 놓아 준다. 위의 _disconnecting 게이트가 이 경로를 한 번만 통과시킨다.
    //
    //   단, 등록한 적 없는 세션에는 놓아 줄 ref가 없다 — attach 실패분은 골격(ProcessAccept)이
    //   직접 감소시키고, 종료 루프가 훑는 미접속 세션은 IOCount가 애초에 0이다. 표식을 집어내며
    //   지워, 등록된 세션만 정확히 한 번 감소시킨다(그냥 감소시키면 0→−1로 언더플로).
    if (InterlockedExchange(&session->_epollRegistered, FALSE) == TRUE)
        IOCountDecrement(session);
    return true;
}

// ============================================================================
// epoll 워커 — 준비 통지를 받아 처리한다 (IOCP의 WorkerThread에 대응)
// ============================================================================
void CIOCPServer::EpollWorkerThread(int workerIndex)
{
    CoreAffinity::PinIoThread();   // I/O 스레드 → 게임코어 밖으로 (격리 off면 no-op)

    const int monitorIndex = _monitor.RegisterWorkerThread();
    if (monitorIndex >= 0 && monitorIndex < CMonitorManager::MAX_WORKER_THREADS)
        _monitor._workerCounters[monitorIndex].threadHandle = Platform::CaptureCurrentThreadCpu();

    epoll_event events[EPOLL_EVENT_BATCH];

    while (_running == TRUE)
    {
        const int n = ::epoll_wait(_epollFd, events, EPOLL_EVENT_BATCH, EPOLL_WAIT_TIMEOUT_MS);
        if (n < 0)
        {
            if (Platform::LastSocketError() == EINTR)   // 시그널로 깨어난 것은 정상
                continue;
            SLOG_ERROR("[Network] epoll_wait failed: {}", Platform::LastSocketError());
            break;
        }

        for (int i = 0; i < n; ++i)
        {
            auto* session = static_cast<CSession*>(events[i].data.ptr);
            if (session == nullptr)
                continue;

            const uint32_t flags = events[i].events;

            // 소켓 파손(HUP·ERR)만 즉시 끊는다. RDHUP는 아님 — "보내고 닫은" 상대의
            //   마지막 데이터가 FIN과 함께 와 있을 수 있다.
            if (flags & (EPOLLHUP | EPOLLERR))
            {
                RequestDisconnectSession(session);
                continue;
            }

            // RDHUP는 수신 경로로 — 잔여를 다 읽은 뒤 recv==0 자리가 끊는다(IOCP와 같은 의미론).
            if (flags & (EPOLLIN | EPOLLRDHUP))
                EpollHandleReadable(session);

            // 쓰기 준비 — 커널 송신 버퍼에 자리가 생겼다. 남은 구간을 이어서 보낸다.
            if (flags & EPOLLOUT)
                EpollSendSession(session);
        }

        if (monitorIndex >= 0 && monitorIndex < CMonitorManager::MAX_WORKER_THREADS)
            _monitor._workerCounters[monitorIndex].AddDequeue();
    }
}

// ── 4-F/4-G에서 채울 자리 ──
//   지금은 링크만 되게 두고, 준비 통지를 실제 read/write로 잇는 일은 다음 페이즈에서 한다.

// 수신 착수 — epoll에는 "걸어두는" 것이 없다. EPOLLIN 통지를 받은 워커가 직접 읽으므로
//   골격이 이 함수를 불러도 할 일이 없다(IOCP 팔의 재제출에 대응하는 자리).
void CIOCPServer::PostRecv(CSession* session, bool skipAcquire)
{
    (void)session;
    (void)skipAcquire;
}

// EPOLLIN 처리 — 준비된 소켓에서 직접 읽어 골격의 ProcessRecv로 넘긴다.
//   ProcessRecv가 링버퍼 쓰기 위치 이동과 패킷 분해를 모두 하므로, 여기서는 "몇 바이트 읽었나"만 넘기면 된다.
void CIOCPServer::EpollHandleReadable(CSession* session)
{
    // 처리 중 세션이 반환되지 않도록 pin한다.
    //   4-E에서 확인했듯 epoll에는 ref를 놓아 주는 완료 통지가 없으므로, 잡고 놓는 짝을
    //   이 함수가 직접 맞춘다. AcquireSession이 실패하면 이미 정리 중인 세션이다.
    if (!AcquireSession(session))
        return;

    // 수신 직렬화 — 한 세션의 _recvQ는 한 워커만 만진다.
    //   level-trigger는 "커널 버퍼에 데이터가 남았는가"만 보고 알리므로, 앞선 워커가 아직 읽는
    //   중에도 다른 워커에게 같은 소켓을 또 알린다. 게이트가 없으면 둘이 같은 링버퍼의 쓰기
    //   위치를 겹쳐 밀어 수신 스트림이 깨진다(TSan 실측).
    //   [물러나도 안전한 이유] 통지를 흘려도 데이터는 커널 버퍼에 그대로 남고, level-trigger가
    //   다음 epoll_wait에서 다시 알려 준다 — 잡은 쪽이 마저 읽거나, 다음 라운드가 가져간다.
    if (InterlockedExchange(&session->_recvBusy, TRUE) == TRUE)
    {
        _monitor._recvContention.Inc();
        IOCountDecrement(session);
        return;
    }

    while (true)
    {
        char* writePtr = session->_recvQ.GetWritePtr();
        const size_t writable = session->_recvQ.GetDirectWriteSize();
        if (writable == 0)
            break;   // 링이 찼다 — 게임 스레드가 비우면 다음 통지에서 이어 읽는다

        const ssize_t n = ::recv(session->_socket, writePtr, writable, 0);

        if (n > 0)
        {
            _monitor._wsaRecvCalls.Inc();          // 지표 이름은 양 팔 공용(수신 syscall 횟수)
            ProcessRecv(session, static_cast<DWORD>(n));

            if (session->_disconnecting == TRUE)
                break;                              // 파싱이 종료를 유도한 경우

            if (static_cast<size_t>(n) < writable)
                break;                              // 커널 버퍼를 다 비웠다
            continue;                               // 꽉 채워 읽었으면 더 남았을 수 있다
        }

        if (n == 0)
        {
            RequestDisconnectSession(session);      // 상대의 우아한 종료(FIN)
            break;
        }

        const int err = Platform::LastSocketError();
        if (Platform::WouldBlock(err))
            break;                                  // 더 읽을 것이 없다 — 정상 종료 조건
        if (err == EINTR)
            continue;                               // 시그널로 끊긴 호출은 재시도

        SLOG_ERROR("[Network] recv failed: {}", err);
        RequestDisconnectSession(session);
        break;
    }

    // 게이트를 먼저 놓고 ref를 나중에 — 순서가 반대면 이 감소로 세션이 해제·재사용된 뒤
    //   새 세션의 게이트를 열어 주게 된다(F2와 같은 종류의 순서 문제).
    InterlockedExchange(&session->_recvBusy, FALSE);

    IOCountDecrement(session);
}

// 세그먼트 제출 — 4-G에서 write + EPOLLOUT 재등록으로 채운다.
//   반환값 규약은 IOCP 팔과 같다(제출 바이트 수, 실패는 음수).
int CIOCPServer::TransportSubmitSegment(CSession* session, Platform::NetSocket socket,
                                        const CRingBufferMT::SubmitInfo& info,
                                        int slot, int slotsAvailable, size_t* submittedBytes)
{
    (void)session; (void)socket; (void)info; (void)slot; (void)slotsAvailable;
    if (submittedBytes)
        *submittedBytes = 0;
    return -1;
}

#endif  // !_WIN32
