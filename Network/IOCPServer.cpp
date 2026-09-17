#include "IOCPServer.h"
#include "Crash/CrashDump.h"   // CRASH 매크로 (RIO CQ 오염 시 즉사+덤프)
#include "Platform/Platform.h"   // 플랫폼 격리 경계 (타이머 해상도 등)
#include <Common/ErrorLog.h>
#include <iostream>
#include <chrono>
#include <algorithm>
#include <cassert>   // 도달불가 불변식 검증 (릴리즈에서는 사라짐)

#include "CoreAffinity.h"

extern void SignalProcessShutdown(); // main 쪽에 정의된 종료 알림 함수


// CSession Implementation
CSession::CSession()
    : _ioCount(0)
    , _disconnecting(FALSE)
    , _socket(INVALID_SOCKET)
    , _sessionId(0)
    , _sendSubmitBusy(FALSE)
    , _sendInFlight(0)
{
#ifdef _WIN32
    ZeroMemory(&_recvOverlapped.overlapped, sizeof(OVERLAPPED));
#endif
    _recvOverlapped.operation = IOOperation::RECV;
    ResetSendSlots();

#if USE_LOCKFREE_SENDQ
    _pendingSendCount = 0;
    _pendingSendBytes = 0;
    memset(_pendingSendBufs, 0, sizeof(_pendingSendBufs));
#endif
}

void CSession::Initialize(SOCKET socket, int64_t sessionId)
{
    _socket = socket;
    _sessionId = sessionId;
    _disconnecting = FALSE;
    _sendSubmitBusy = FALSE;
    _sendInFlight = 0;
    _sendDirty = false;
    _queuedForSend = FALSE;
    _recvQ.Clear();
#if USE_LOCKFREE_SENDQ
    // 방어코드 : 정상 흐름에서는 ReleaseSession에서 SendQ 비움
    // CLockFreeQueue::Clear()는 내부 노드만 free list로 반환하므로
    // <T>타입인 CSerialBuffer의 SubRef를 명시 호출한다.
    {
        CSerialBuffer* stale = nullptr;
        while (_sendQ.Dequeue(&stale))
            stale->SubRef();
    }
    _pendingSendCount = 0;
    _pendingSendBytes = 0;
#else
    _sendQ.Clear();
#endif

    // 세션 고정 Overlapped 방식: IO 요청마다 재사용하므로 요청 전 OVERLAPPED만 초기화한다.
#ifdef _WIN32
    ZeroMemory(&_recvOverlapped.overlapped, sizeof(OVERLAPPED));
#endif
    _recvOverlapped.operation = IOOperation::RECV;
    ResetSendSlots();   // 송신 슬롯 링 전체 리셋 (이전 세션의 in-flight 잔재 제거)

    ResetTransportState();   // 팔별 재사용 리셋 (RIO: 이전 소켓의 RQ 잔재 제거 — 새 RQ는 NewConn에서 생성)

    // _ioCount를 마지막에 설정 — 첫 번째 Recv IO의 ref (base ref 아님).
    // InterlockedExchange가 full barrier를 제공하므로 위의 모든 쓰기가 이 시점 전에 완료된다.
    // 이 시점부터 세션이 외부에 공개된다.
    InterlockedExchange(&_ioCount, 1);
}

CSession::~CSession()
{
    Close();
}

void CSession::Close()
{
    // 소켓과 세션 상태를 정리한다. 버퍼 등 나머지는 Initialize()에서 초기화한다.
    // IOCount=0 시점에 단일 스레드에서만 호출되므로 Interlocked 불필요.
    _sendSubmitBusy = FALSE;
    _sendInFlight = 0;
    _sessionId = 0;

    SOCKET socket = _socket;
    _socket = INVALID_SOCKET;
    if (socket != INVALID_SOCKET)
    {
        Platform::CloseSocket(socket);
    }
}

// CIOCPServer Implementation

CIOCPServer::CIOCPServer(int port, int maxClients, ServerMode mode,
                         CMonitorManager& monitor, int workerThreads, int sendWorkers, int rioWorkers,
                         int completionBatch, int sendDepth)
    : _port(port)
    , _maxClients(maxClients)
    , _serverMode(mode)
    , _configuredWorkers(workerThreads)
    , _configuredSendWorkers(sendWorkers)
    , _configuredRioWorkers(rioWorkers)
    , _configuredCompletionBatch(completionBatch)
    , _configuredSendDepth(sendDepth)
    , _monitor(monitor)
    , _running(FALSE)
    , _sessionIdCounter(1)  // 0은 사용하지 않음
    , _listenSocket(INVALID_SOCKET)
#ifdef _WIN32
    , _iocpHandle(NULL)
#endif
{
    // 멤버 변수만 초기화
}

CIOCPServer::~CIOCPServer()
{
    ShutdownServer();
}


bool CIOCPServer::Start()
{
    if (_maxClients <= 0 || _maxClients > CSession::SESSION_MAX_COUNT)
    {
        LOG_ERROR_STREAM("[Error] maxClients(" << _maxClients << ") out of range. valid: 1~" << CSession::SESSION_MAX_COUNT);
        return false;
    }

    // 송신 깊이 확정 — 뒤따르는 모든 준비물이 이 값을 읽는다(RIO의 요청 큐 깊이·완료 큐 크기,
    //   워커·완료 처리의 게이트). 그래서 어떤 자원보다 먼저 정하고 이후 불변으로 둔다.
    //   2의 거듭제곱만 쓰는 이유: 슬롯 인덱스를 head/tail 카운터의 비트마스크로 구하므로
    //   카운터가 오버플로로 wrap해도 인덱스 관계가 유지돼야 한다. 그 외 값은 아래쪽 거듭제곱으로
    //   내린다(3→2, 5~7→4, 9↑→8). A/B를 재빌드가 아니라 INI 한 줄로 돌리려고 런타임 값으로 뒀다.
    {
        int depth = std::clamp((_configuredSendDepth > 0) ? _configuredSendDepth : 1,
                               1, CSession::MAX_SEND_DEPTH);
        int pow2 = 1;
        while (pow2 * 2 <= depth)
            pow2 *= 2;
        _sendDepth = pow2;
        _sendDepthMask = pow2 - 1;

        _monitor._sendDepth = _sendDepth;   // 수집 스크립트가 아암 라벨과 대조하는 게이지

        if (_sendDepth != _configuredSendDepth)
            SLOG_INFO("[Network] Send depth = {} (INI {} → 2의 거듭제곱으로 내림)", _sendDepth, _configuredSendDepth);
        else
            SLOG_INFO("[Network] Send depth = {} (세션당 동시 송신 제출 상한)", _sendDepth);
    }

    // 세션 객체는 서버 시작 시 동접자 수만큼 고정 생성하고 이후 index만 재사용한다.
    _sessions.resize(_maxClients);
    for (uint16_t i = 0; i < _maxClients; ++i)
    {
        // INVALID_SOCKET과 0 세션ID로 미리 생성
        _sessions[i] = std::make_unique<CSession>();
        // 링버퍼 준비는 팔별 — RIO는 등록 슬랩의 슬라이스라 여기서 하지 않는다(TransportPostListen).
        if (!TransportInitSessionBuffer(_sessions[i].get()))
        {
            LOG_ERROR_STREAM("Failed to init session RingBuffer [index=" << i << "]");
            return false;
        }
    }


    // SerialBuffer 메모리풀 초기화
    // 클라이언트당 동시 송신 대기 버퍼 1~2개 가정, 부족 시 청크 자동 증가
    // 생성자 인자 0 = 기본 워밍업 생략. 실제 사전 확보량은 바로 아래 Init()이 정한다.
    CSerialBuffer::_TlsMsgFreeList = new LockFree::CExternalTlsFreeList<CSerialBuffer>(0);
    if (!CSerialBuffer::_TlsMsgFreeList->Init(_maxClients * 2))
    {
        LOG_ERROR_STREAM("Failed to init SerialBuffer FreeList");
        return false;
    }

    // 인덱스 스택 초기화 (maxClients만큼 사전 할당)
    if (!_availableIndices.Init(_maxClients))
    {
        SLOG_ERROR("[Error] Failed to init available indices stack");
        return false;
    }

    // Start 재호출 시 이전 lock-free 스택에 남은 index를 비운다.
    uint16_t staleIndex = 0;
    while (_availableIndices.Pop(&staleIndex))
    {
    }

    // 빈 인덱스 스택 초기화 (0번부터 maxClients-1까지)
    for (uint16_t i = 0; i < _maxClients; ++i)
    {
        _availableIndices.Push(i);
    }

    if (!Platform::SocketStartup())
    {
        LOG_ERROR_STREAM("Socket library startup failed");
        return false;
    }

    // 리슨 소켓 "전" 준비 — IOCP 팔은 완료 포트 핸들이 여기서 필요하다(accept 소켓 바인드 대상).
    if (!TransportPreListen())
    {
        Platform::SocketCleanup();
        return false;
    }

    // Listen 소켓 생성
    if (!CreateListenSocket())
    {
        TransportPreListenCleanup();
        Platform::SocketCleanup();
        return false;
    }

    // 리슨 소켓 "후" 준비 — RIO 팔은 함수 테이블 probe에 소켓이 필요하고, 슬랩 등록·세션 링
    // 슬라이스·워커 CQ를 여기서 만든다. 실패 시 내부에서 정리하고 false를 돌린다.
    if (!TransportPostListen())
        return false;

    InterlockedExchange(&_running, TRUE);

    // 시스템 타이머 해상도를 1ms로 설정 (Sleep, WaitForSingleObject 등 정밀도 향상)
    Platform::SetHighResolutionTimer(true);

    // 타이밍 휠 생성 및 시작
    _timingWheel = std::make_unique<CTimingWheel>();
    if (!_timingWheel->Init(_maxClients, SESSION_TIMEOUT_SEC, TIMER_TICK_INTERVAL_MS))
    {
        SLOG_ERROR("[TimingWheel] Init failed");
        return false;
    }
    _timingWheel->Start(OnSessionTimeout, this);

    // 완료 워커·송신 워커 기동 (팔별) — 워커가 읽는 설정은 기동 "전"에 확정된다.
    TransportStartWorkers();

    // Accept 스레드 생성
    _acceptThread = std::thread(&CIOCPServer::AcceptThread, this);

    const char* modeName = "Unknown";
    switch (_serverMode)
    {
    case ServerMode::GameCodiEchoTest:    modeName = "GameCodiEchoTest";    break;
    case ServerMode::NetWorkLib_EchoTest: modeName = "NetWorkLib_EchoTest"; break;
    case ServerMode::GameServer:          modeName = "GameServer";          break;
    }
    TransportLogStarted(modeName);
    return true;
}


bool CIOCPServer::CreateListenSocket()
{
#ifdef _WIN32
    _listenSocket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, TransportListenFlags());
#else
    // 리눅스에는 WSASocket의 플래그(OVERLAPPED/REGISTERED_IO) 개념이 없다.
    //   비동기 성격은 소켓이 아니라 이벤트 루프(epoll) 쪽에서 정해진다.
    (void)TransportListenFlags();
    _listenSocket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#endif
    if (_listenSocket == INVALID_SOCKET)
    {
        const int wsaErr = Platform::LastSocketError();
        SLOG_ERROR("WSASocket failed: {}", wsaErr);
        return false;
    }

    SOCKADDR_IN serverAddr;
    ZeroMemory(&serverAddr, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serverAddr.sin_port = htons(_port);

    if (bind(_listenSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR)
    {
        const int wsaErr = Platform::LastSocketError();
        SLOG_ERROR("bind failed: {}", wsaErr);
        Platform::CloseSocket(_listenSocket);
        return false;
    }

    if (listen(_listenSocket, SOMAXCONN_HINT(1024)) == SOCKET_ERROR)
    {
        const int wsaErr = Platform::LastSocketError();
        SLOG_ERROR("listen failed: {}", wsaErr);
        Platform::CloseSocket(_listenSocket);
        return false;
    }

    return true;
}


// 대부분 연결별 동작에 영향을 주는 옵션은 accept후에 설정해야한다.
bool CIOCPServer::SetSocketOptions(SOCKET socket)
{
    // LINGER 옵션: 연결 종료 시 RST 전송 (즉시 종료)
    LINGER lingerOpt;
    lingerOpt.l_onoff = 1;
    lingerOpt.l_linger = 0;
    setsockopt(socket, SOL_SOCKET, SO_LINGER, reinterpret_cast<char*>(&lingerOpt), sizeof(lingerOpt));

    // TCP_NODELAY 옵션: Nagle 알고리즘 비활성화 (지연 없이 송신)
    // 2026-08-14 실측으로 ❌기각 — 켜면 끊김 35.6배(0.35→12.4/s)·RTT p99 +26%·송신워커 98.6% 포화.
    // Nagle이 세그먼트를 묶어 커널 비용을 아껴주고 있었다. 되살리지 말 것.
    //int flag = 1;
    //setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));

#if USE_ZERO_SNDBUF
    // [실험] 커널 송신버퍼 0 → WSASend 시 커널 복사(③) 제거, 유저버퍼에서 직접 송신(zero-copy).
    //        ③가 미미하다는 가정의 실측 검증용 (CoreConfig.h 토글).
    //   커널이 요청을 받아들였는지 반드시 되읽어 확인한다 — setsockopt이 성공을 돌려줘도 스택이
    //   값을 그대로 쓴다는 보장은 없다. 확인 없이 재면 실험 팔이 안 걸린 A/A 를 A/B 로 착각한다.
    int sndBufSize = 0;
    if (setsockopt(socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&sndBufSize), sizeof(sndBufSize)) == SOCKET_ERROR)
    {
        LOG_ERROR_STREAM("[Experiment] SO_SNDBUF=0 설정 실패. WSA=" << WSAGetLastError());
    }
    else
    {
        // 세션마다 찍으면 로그가 폭주하므로 첫 소켓 한 번만 남긴다.
        static std::atomic<bool> sndBufReported{ false };
        if (!sndBufReported.exchange(true))
        {
            int applied = -1;
            int appliedLen = sizeof(applied);
            if (getsockopt(socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&applied), &appliedLen) == 0)
                SLOG_INFO("[Experiment] SO_SNDBUF=0 요청 → 커널 적용값 {} (0이 아니면 실험이 안 걸린 것)", applied);
            else
                LOG_ERROR_STREAM("[Experiment] SO_SNDBUF 적용값 확인 실패. WSA=" << WSAGetLastError());
        }
    }
#endif

    // 필요시 추가 옵션 설정 가능
    return true;
}


// 새 I/O 제출을 막고 pending I/O 완료를 유도한다. 실제 closesocket은 IOCount 0에서 수행한다.
void CIOCPServer::ShutdownServer()
{
    if (InterlockedExchange(&_running, FALSE) == FALSE)
    {
        return;
    }

    // 1. Listen 소켓 정지 — 새 연결 차단 + AcceptThread 깨우기
    //    윈도우: closesocket이 블록된 accept를 실패로 깨운다.
    //    리눅스: close는 못 깨운다 — 진행 중인 accept가 파일 객체를 붙들고 있어 fd 테이블에서
    //            빼도 대기자에게 통지가 가지 않는다(영원히 블록). shutdown이 리슨 소켓 상태를
    //            바꿔 accept를 EINVAL로 깨우므로 이쪽을 쓴다.
    //            close를 join 뒤로 미루는 이유 — accept 스레드가 _running 검사를 통과한 직후
    //            멈췄다가 나중에 accept를 부를 수 있다. fd가 살아 있으면 EINVAL로 즉시 돌아오지만,
    //            닫아 두면 그사이 재사용된 fd 번호를 덮친다.
    if (_listenSocket != INVALID_SOCKET)
    {
#ifdef _WIN32
        Platform::CloseSocket(_listenSocket);
        _listenSocket = INVALID_SOCKET;
#else
        ::shutdown(_listenSocket, SHUT_RDWR);
#endif
    }

    if (_acceptThread.joinable())
    {
        _acceptThread.join();
    }

#ifndef _WIN32
    // accept 스레드가 나간 뒤에야 실제로 닫는다 (위의 fd 재사용 이유).
    if (_listenSocket != INVALID_SOCKET)
    {
        Platform::CloseSocket(_listenSocket);
        _listenSocket = INVALID_SOCKET;
    }
#endif

    // IOCount 드레인 "전" 정지 (팔별) — IOCP는 송신 워커를 먼저 멈춰야 pin한 IOCount가 풀린다.
    TransportStopBeforeDrain();

    // 타이밍 휠 정지 (더 이상 타임아웃 disconnect 발생하지 않음)
    if (_timingWheel)
    {
        _timingWheel->Stop();
    }

    // 2. 모든 세션에 종료 유도 — CancelIoEx로 pending IO 완료를 촉진
    for (auto& session : _sessions)
    {
        if (session)
        {
            RequestDisconnectSession(session.get());
        }
    }

    // 3. 모든 세션의 IOCount가 0이 될 때까지 대기
    //    워커 스레드가 cancelled IO 완료 통지를 처리하여 IOCount를 감소시킨다.
    for (auto& session : _sessions)
    {
        if (!session)
            continue;

        while (session->_ioCount > 0)
        {
            Sleep(1);
        }
    }

    // 4. 모든 IO 정리 완료 — 완료 워커 정지·자원 해제 (팔별)
    TransportStopAfterDrain();

    Platform::SocketCleanup();

    // 타이머 해상도 복원
    Platform::SetHighResolutionTimer(false);

    // 네트워크 종료 완료 → 프로세스 종료 알림
    SignalProcessShutdown();
}



// accept에는 timeout 기능이 없음. 종료 시 ShutdownServer가 리슨 소켓을 정지시켜 깨운다
// (윈도우는 closesocket, 리눅스는 shutdown — 리눅스 close는 블록된 accept를 못 깨운다).
void CIOCPServer::AcceptThread()
{
    CoreAffinity::PinIoThread();   // Accept 스레드 → 게임코어 밖으로 (격리 off면 no-op)

    while (_running == TRUE)
    {
        SOCKADDR_IN clientAddr;
#ifdef _WIN32
        int addrLen = sizeof(clientAddr);
#else
        socklen_t addrLen = sizeof(clientAddr);   // POSIX accept는 socklen_t*를 받는다
#endif

        SOCKET clientSocket = accept(_listenSocket, (SOCKADDR*)&clientAddr, &addrLen);


        if (clientSocket == INVALID_SOCKET)
        {
            if (_running == TRUE)
            {
                const int wsaErr = Platform::LastSocketError();
                LOG_WSA_ERROR_STREAM("accept failed: ", wsaErr);
            }
            continue;
        }

        SetSocketOptions(clientSocket);
        ProcessAccept(clientSocket);
    }
}

void CIOCPServer::ProcessAccept(SOCKET clientSocket)
{
    // 과부하 차단 — 게임 스레드가 유입을 못 따라가면 새 접속을 받지 않는다.
    //   드롭(이동 패킷 유실)도 워커 정지(전 세션 수신 멎음)도 못 쓰니 남는 수단은 이것뿐이다.
    //   값은 게임루프가 매 틱 갱신하는 스냅샷이라 락 없이 읽고, 로그 대신 카운터만 올린다.
    if (_eventQueueAcceptLimit > 0 &&
        _monitor._gameLoop._eventQueueSize >= _eventQueueAcceptLimit)
    {
        _monitor._acceptRejectedByQueue.Inc();
        Platform::CloseSocket(clientSocket);
        return;
    }

    // 빈 인덱스 확인 (여유가 없다면 동접 max)
    uint16_t index = 0;
    if (!_availableIndices.Pop(&index))
    {
        LOG_ERROR_STREAM("[Error] No free session index available");
        _monitor._acceptFailed.Inc();
        Platform::CloseSocket(clientSocket);
        return;
    }

    // 고유 ID 생성 (하위 48비트만 사용)
    int64_t uniqueId = InterlockedIncrement64(&_sessionIdCounter) & CSession::SESSION_UNIQUE_MASK;

    // SessionID 생성 [16bit Index][48bit UniqueID]
    int64_t sessionId = CSession::MakeSessionId(index, uniqueId);

    // session 초기화. 사용가능한 상태가 됨
    _sessions[index]->Initialize(clientSocket, sessionId);

    // 세션 지표 기록 — Initialize 직후에 올린다.
    //   아래 BindIOCP(RIO 모드에서는 워커의 RQ 생성)가 실패하면 ReleaseSession 경로로 빠지는데,
    //   거기서는 sessionId만 보고 무조건 감소시킨다. 증가를 성공 경로에만 두면 실패 시
    //   감소만 일어나 동접 게이지가 음수 방향으로 영구히 밀린다.
    _monitor._sessionCreated.Inc();
    _monitor._sessionCount.Inc();

    // 완료 통지 연결 (팔별) — IOCP는 소켓을 완료 포트에 붙인다. RIO는 소유 워커가 RQ 생성 시 처리.
    if (!TransportAttachSession(_sessions[index].get(), clientSocket))
    {
        LOG_ERROR_STREAM("Failed to bind client socket to IOCP");
        // RequestDisconnectSession → ReleaseSession(IOCount→0)으로 정리
        RequestDisconnectSession(_sessions[index].get());
        IOCountDecrement(_sessions[index].get());
        return;
    }

    // 컨텐츠쪽 전달
    switch (_serverMode)
    {
    case ServerMode::GameCodiEchoTest:
    case ServerMode::NetWorkLib_EchoTest:
        // 에코 모드: 컨텐츠 레이어 없음
        break;
    case ServerMode::GameServer:
        PushNetworkEvent(NetworkEvent(NetworkEvent::Type::CONNECTED, sessionId));
        break;
    }

    // 타이밍 휠에 세션 등록 (타임아웃 카운트 시작)
    _timingWheel->RequestRegister(CSession::ExtractIndex(sessionId), sessionId);

    // 첫 Recv 착수 (팔별) — Initialize의 IOCount=1이 이 IO의 ref. AcquireSession 불필요.
    TransportStartFirstRecv(_sessions[index].get(), clientSocket, sessionId);
}

// Recv 완료 통지 처리
void CIOCPServer::ProcessRecv(CSession* session, DWORD bytesTransferred)
{
    // [불변식] 0바이트 완료(상대의 우아한 종료)는 호출자 2곳(WorkerThread·RioDrainCompletions)의
    //   canProcess가 먼저 걸러낸다.
    assert(bytesTransferred != 0);

    // 수신 바이트 지표 기록
    _monitor._recvBytes.Add(static_cast<LONG64>(bytesTransferred));

    // 타이밍 휠 수명 갱신 (데이터 수신 = 세션 활성 상태)
    _timingWheel->RequestRefresh(CSession::ExtractIndex(session->_sessionId), session->_sessionId);

    // 링버퍼 쓰기 포인터 이동
    // [불변식] PostRecv 게시량 = 게시 시점 freeSize이고, recv 1-pending이라 완료까지 freeSize가
    //   줄지 않는다 → bytesTransferred <= freeSize.
    //   ※ 호출을 assert 안에 넣지 말 것 — 쓰기 위치를 옮기는 부작용이다.
    const size_t movedSize = session->_recvQ.MoveWritePtr(bytesTransferred);
    assert(movedSize == bytesTransferred);
    (void)movedSize;   // 릴리즈(NDEBUG) 미사용 경고 억제

    // 패킷 파싱
    ParsePackets(session);

    // 다음 Recv 요청
    PostRecv(session);
}



// ParsePackets: 링버퍼에서 완성된 패킷 추출 → CSerialBuffer에 적재
void CIOCPServer::ParsePackets(CSession* session)
{
    // 특정 더미타입에 맞게 헤더사이즈 조정
    const size_t headerSize = (_serverMode == ServerMode::GameCodiEchoTest)
        ? sizeof(EchoMsgHeader)  // 2byte (size만, 페이로드 크기)
        : sizeof(MsgHeader);     // 4byte (size + type, 전체 크기)

    int64_t parsedPackets = 0;   // [계측 배치] per-packet 원자증가 → 종료 시 1회
    while (true)
    {
        size_t dataSize = session->_recvQ.GetDataSize();

        // 1. 헤더 크기 체크
        if (dataSize < headerSize)
        {
            break; // 데이터 부족
        }

        // 2. 헤더에서 size 필드 peek (size는 항상 첫 2byte)
        // [불변식] 1번이 dataSize >= headerSize(2 또는 4)를 보장했고 요청은 2바이트뿐이다.
        //   ※ 호출을 assert 안에 넣지 말 것 — packetSize를 채우는 부작용이다.
        uint16_t packetSize = 0;
        const size_t peekedSize = session->_recvQ.Peek(&packetSize, sizeof(uint16_t));
        assert(peekedSize == sizeof(uint16_t));
        (void)peekedSize;   // 릴리즈(NDEBUG) 미사용 경고 억제

        // 3. 전체 패킷 크기 계산
        // GameCodiEchoTest: size = 페이로드 크기 → 헤더 크기를 더해야 전체 크기
        // 그 외: size = 전체 크기 (헤더 포함) → 그대로 사용
        size_t totalPacketSize = (_serverMode == ServerMode::GameCodiEchoTest)
            ? static_cast<size_t>(packetSize) + sizeof(EchoMsgHeader)
            : static_cast<size_t>(packetSize);

        // 4. 패킷 크기 검증
        if (totalPacketSize < headerSize || totalPacketSize > MAX_PACKET_SIZE)
        {
            LOG_ERROR_STREAM("[Error] Invalid packet size: " << totalPacketSize
                << " - SessionId: " << session->_sessionId);
            _monitor._packetErrors.Inc();
            if (parsedPackets != 0)
                _monitor._recvPackets.Add(parsedPackets);
            RequestDisconnectSession(session);
            return;
        }

        // 5. 전체 패킷이 수신되었는지 확인
        if (dataSize < totalPacketSize)
        {
            break; // 데이터 부족 - 다음 Recv 대기
        }

        // 6. CSerialBuffer에 패킷 전체 적재 (헤더 포함)
        CSerialBuffer* pMsg = CSerialBuffer::Alloc();
        // [불변식] 5번이 dataSize >= totalPacketSize를 확인했고, 그 뒤 이 링을 소비하는 주체가 없다.
        //   ※ 호출을 assert 안에 넣지 말 것 — 읽기 위치를 옮기는 부작용이다.
        const size_t dequeuedSize = session->_recvQ.Dequeue(pMsg->GetWriteBufferPtr(), totalPacketSize);
        assert(dequeuedSize == totalPacketSize);
        (void)dequeuedSize;   // 릴리즈(NDEBUG) 미사용 경고 억제
        // [불변식] 적재는 항상 성공한다 — 실패 조건인 IsFull(size)는
        //   _DataSize + size > _BufferSize - HEADER_SIZE 인데, pMsg는 바로 위에서 Alloc한 것이라
        //   _DataSize=0이고 _BufferSize=MSG_DEFAULT_SIZE(1460) 고정, totalPacketSize는 위 4번
        //   검증에서 MAX_PACKET_SIZE(=MSG_DEFAULT_SIZE-HEADER_SIZE=1458) 이하가 보장된다
        //   → 1458 > 1458 이 되어 성립할 수 없다.
        //   이 세 상수의 관계가 다시 어긋나면(원래 Critical: 할당 밖 2B 침범) 여기서 먼저 터진다.
        //   ※ 호출을 assert 안에 넣지 말 것 — 쓰기 위치를 실제로 옮기는 부작용이라, 릴리즈에서
        //     식째로 사라지면 _DataSize=0인 빈 버퍼가 상위로 흘러간다.
        const int movedSize = pMsg->MoveWritePos(static_cast<int>(totalPacketSize));
        assert(movedSize == static_cast<int>(totalPacketSize));
        (void)movedSize;   // 릴리즈(NDEBUG) 미사용 경고 억제

        // 수신 패킷 카운트 (지역 누적, 종료 시 1회 반영)
        ++parsedPackets;

        // 수신 버퍼는 단일 소비자(GameLogicThread)이므로 Seal 불필요
        // Seal하면 operator>>/GetData가 차단되어 역직렬화 불가
        // 소유권 1은 Alloc()/Clear()에서 이미 확보됨(RefCount=1) → 여기서 AddRef 불필요

        // 7. 컨텐츠쪽 전달 또는 처리
        switch (_serverMode)
        {
        case ServerMode::GameCodiEchoTest:
        case ServerMode::NetWorkLib_EchoTest:
            EchoTestSend(session, pMsg);
            break;
        case ServerMode::GameServer:
            PushNetworkEvent(NetworkEvent(NetworkEvent::Type::RECEIVED,
                session->_sessionId, pMsg));
            break;
        default:
            // 알 수 없는 모드 — 어느 경로로도 소비되지 않으므로 소유권 회수 (누수 방어)
            pMsg->SubRef();
            break;
        }

    }

    // 파싱한 패킷 수 1회 반영
    if (parsedPackets != 0)
        _monitor._recvPackets.Add(parsedPackets);
}

// Send 완료 통지 처리
void CIOCPServer::ProcessSend(CSession* session, DWORD bytesTransferred, int slot)
{
    if (!session || session->_disconnecting == TRUE)
        return;

    // 송신 지표 기록
    _monitor._sendBytes.Add(static_cast<LONG64>(bytesTransferred));
    _monitor._wsaSendCompletions.Inc();

#if USE_LOCKFREE_SENDQ
    (void)slot;   // 락프리 큐 경로는 깊이를 쓰지 않는다 (슬롯 링 미사용)
    if (static_cast<int>(bytesTransferred) == session->_pendingSendBytes)
    {
        // 정상 완료: 전체 해제
        session->ReleasePendingSendBufs();
    }
    else
    {
        // Partial send → 비정상 완료로 간주, 세션 종료
        _monitor._partialSend.Inc();
        LOG_ERROR_STREAM("[Error] Partial send - SessionId: " << session->_sessionId
            << ", Expected: " << session->_pendingSendBytes
            << ", Transferred: " << bytesTransferred);
        session->ReleasePendingSendBufs();
        RequestDisconnectSession(session);
        InterlockedExchange(&session->_sendSubmitBusy, FALSE);
        return;
    }

    InterlockedExchange(&session->_sendSubmitBusy, FALSE);

    // Double-check: 잠금 해제 직후 큐 확인 (다른 스레드가 Enqueue했을 수 있음)
    if (!session->_sendQ.IsEmpty())
        PostSend(session);
#else
    if (slot < 0 || slot >= CSession::MAX_SEND_DEPTH)
    {
        LOG_ERROR_STREAM("[Error] Send completion with bad slot - SessionId: " << session->_sessionId
            << ", Slot: " << slot);
        RequestDisconnectSession(session);
        return;
    }

    CSession::SendSlot& s = session->_sendSlots[slot];
    const size_t expected = s.bytes;

    // 부분 송신 — 이 제출이 요청한 만큼 다 나가지 않았다.
    if (static_cast<size_t>(bytesTransferred) < expected)
    {
        if (_sendDepth == 1)
        {
            // 깊이 1이면 뒤에 떠 있는 제출이 없다 → 잔여를 미제출로 되돌려 재전송한다.
            //   제출 경계가 없던 1-pending 시절에 readPos만 밀면 자동으로 됐던 동작의 재현.
            session->_sendQ.ConsumeSubmitted(bytesTransferred);
            session->_sendQ.RewindSubmitted();
            s.bytes = 0;
            InterlockedIncrement(&session->_slotHead);
            InterlockedDecrement(&session->_sendInFlight);

            if (session->_sendQ.GetUnsubmittedSize() > 0)
                PostSend(session);
            return;
        }

        // 깊이 2 이상이면 이 구간 뒤의 제출이 이미 와이어에 실렸을 수 있어 되돌릴 수 없다 —
        //   복구되는 척하며 스트림을 망치는 대신 끊는다.
        _monitor._partialSend.Inc();
        LOG_ERROR_STREAM("[Error] Partial send (depth>1, unrecoverable) - SessionId: " << session->_sessionId
            << ", Expected: " << expected << ", Transferred: " << bytesTransferred);
        RequestDisconnectSession(session);
        return;
    }

    // 표식만 세우고 링 반환은 head부터 연속으로 — 완료 통지 순서가 제출 순서와 어긋나도
    //   읽기 포인터는 오래된 구간부터 전진한다.
    InterlockedExchange(&s.done, TRUE);
    ReapSendSlots(session);

    // 자리가 비었으니 남은 미제출분을 이어 보낸다 (그사이 다른 스레드가 Enqueue했을 수 있음).
    // RIO: 직선 구간만 보내므로 감긴 꼬리도 이 재확인이 즉시 이어 보낸다.
    if (session->_sendQ.GetUnsubmittedSize() > 0)
    {
        // [계측] 완료 왕복이 끼어 "이어 보낸" 횟수 — 깊이를 올려 이 값이 줄면 깊이가 흡수한 것이다.
        _monitor._sendFollowUp.Inc();
        PostSend(session);
    }
#endif
}

#if !USE_LOCKFREE_SENDQ
// 완료된 슬롯을 head부터 "연속으로" 회수한다 — 링 읽기 포인터는 오래된 구간부터만 전진해야 한다.
//   IOCP는 완료 워커가 여럿이라 뒤에 제출한 것이 먼저 통지될 수 있다. 그때 바로 링을 밀면
//   아직 안 나간 앞 구간을 소비해 버리므로, 도착한 슬롯에는 표식만 세우고 여기서 순서를 맞춘다.
void CIOCPServer::ReapSendSlots(CSession* session)
{
    while (true)
    {
        // 회수 구간 직렬화 — 못 잡으면 다른 워커가 회수 중이다. done을 먼저 세워 뒀으므로
        //   그쪽 루프가 내 슬롯까지 함께 가져간다(완료를 흘리지 않는다).
        if (InterlockedExchange(&session->_sendReapBusy, TRUE) == TRUE)
            return;

        while (true)
        {
            const int head = static_cast<int>(static_cast<ULONG>(session->_slotHead) & _sendDepthMask);
            CSession::SendSlot& s = session->_sendSlots[head];
            if (s.done == FALSE)
                break;                 // 가장 오래된 제출이 아직 안 왔다 — 뒤가 왔어도 기다린다

            const size_t bytes = s.bytes;
            s.bytes = 0;
            InterlockedExchange(&s.done, FALSE);
            InterlockedIncrement(&session->_slotHead);

            if (bytes > 0 && session->_sendQ.ConsumeSubmitted(bytes) != bytes)
            {
                LOG_ERROR_STREAM("[Error] Send consume mismatch - SessionId: " << session->_sessionId
                    << ", Expected: " << bytes);
                RequestDisconnectSession(session);
            }

            InterlockedDecrement(&session->_sendInFlight);   // 슬롯이 비었다 = 깊이 게이트 한 칸 열림
        }

        InterlockedExchange(&session->_sendReapBusy, FALSE);

        // 잠금을 놓은 틈에 도착한 완료를 흘리지 않는다 — head가 done이면 한 번 더 돈다.
        const int head = static_cast<int>(static_cast<ULONG>(session->_slotHead) & _sendDepthMask);
        if (session->_sendSlots[head].done == FALSE)
            return;
    }
}
#endif // !USE_LOCKFREE_SENDQ

#if !USE_LOCKFREE_SENDQ
// 송신 제출 — 미제출 구간을 깊이 상한(_sendDepth)까지 내보낸다. 팔별 차이는 TransportSubmitSegment 하나뿐.
//   [게이트 분해] 옛 _sending 하나가 "제출 잠금"과 "in-flight 상한"을 겸했다. 여기서는
//     _sendSubmitBusy(제출 구간만)와 _sendInFlight(미완료 수)로 나눈다. 깊이 1이면 옛 동작과 같다.
//   [순서 보장] busy 잠금이 "세그먼트 계산 ~ 제출 호출"을 덮으므로, 제출자가 둘인 IOCP 팔
//     (송신 워커 + 완료 워커의 이어보내기)에서도 제출 순서가 와이어 순서와 일치한다.
//   [pin] 제출 전 IOCount로 세션을 pin한다 (기존 PostSend와 동일 계약).
void CIOCPServer::PostSend(CSession* session)
{
    if (!session)
        return;

    if (!AcquireSession(session))
        return;

    // 이미 다른 스레드가 제출 중이면 물러난다 — 그 스레드가 이번 enqueue분까지 함께 내보내거나,
    //   못 봤다면 완료 시 double-check가 이어 보낸다 (옛 _sending 경합과 같은 성질).
    if (InterlockedExchange(&session->_sendSubmitBusy, TRUE) == TRUE)
    {
        _monitor._sendContention.Inc();
        IOCountDecrement(session);
        return;
    }

    bool needDisconnect = false;

    while (session->_sendInFlight < _sendDepth)
    {
        if (session->_disconnecting == TRUE)
            break;

        auto info = session->_sendQ.GetSubmitInfo();
        if (info.size == 0)
            break;                     // 미제출 구간 없음

        const SOCKET socket = session->_socket;
        if (socket == INVALID_SOCKET)
        {
            needDisconnect = true;
            break;
        }

        // [수명] 제출 1건당 IO ref 1개 — 완료가 IOCountDecrement로 반환한다.
        //   위에서 잡은 pin은 이 함수가 도는 동안의 수명 보장용이라 따로 잡는다.
        //   (1-pending 시절에는 pin 하나가 두 역할을 겸해 성공 시 감소시키지 않았다)
        if (!AcquireSession(session))
            break;

        // 슬롯 할당 — tail은 제출 잠금 안에서만 움직인다. 게이트(inFlight < depth)를 통과했다면
        //   tail 슬롯은 [head..tail) 구간 밖이므로 반드시 비어 있다.
        const int slot = static_cast<int>(static_cast<ULONG>(session->_slotTail) & _sendDepthMask);
        assert(session->_sendSlots[slot].done == FALSE);

        // [순서] in-flight를 제출 "전"에 올린다 — loopback처럼 완료가 빠르면 제출 호출이
        //   돌아오기 전에 완료 워커가 회수를 시작한다. 회수가 증가를 앞지르면 카운터가 음수로
        //   돌아 깊이 게이트가 무너지고(중복 제출) 같은 슬롯을 겹쳐 쓰게 된다.
        //   슬롯의 bytes는 팔별 제출 함수가 제출 직전에 심는다(제출량이 팔마다 다르다).
        const int slotsFree = _sendDepth - session->_sendInFlight;
        InterlockedIncrement(&session->_sendInFlight);
        ++session->_slotTail;

        size_t submitted = 0;
        const int slots = TransportSubmitSegment(session, socket, info, slot, slotsFree, &submitted);
        if (slots <= 0)
        {
            --session->_slotTail;                             // 슬롯 반납
            session->_sendSlots[slot].bytes = 0;
            InterlockedDecrement(&session->_sendInFlight);     // 올린 것을 되돌린다
            IOCountDecrement(session);                         // 이 제출 몫의 IO ref 반환
            needDisconnect = true;                             // 연결 이상 (기존 PostSend와 동일 처리)
            break;
        }

        // [현재 계약] 양팔 모두 한 번에 슬롯 1개만 쓴다 (IOCP는 랩까지 한 방, RIO는 직선 한 건).
        //   깊이>1에서 슬롯 여러 개를 쓰는 팔을 넣을 때는 ref 추가 확보가 실패할 수 있음을
        //   함께 설계해야 한다 — AcquireSession은 _disconnecting이 서면 실패하고, ref가 완료
        //   수보다 적으면 IOCount가 먼저 0이 되어 살아 있는 세션이 해제된다.
        assert(slots == 1);
    }

    // 종료 확정은 잠금을 놓기 "전"에 한다 — 놓은 뒤에 하면 그 틈에 다른 제출자가 앞서 간 경계
    //   뒤부터 보내 와이어에 구멍이 생긴다(제출 실패로 실제로는 나가지 않은 구간을 건너뜀).
    //   RequestDisconnectSession은 이 잠금을 쓰지 않으므로 보유한 채 불러도 안전하다.
    if (needDisconnect)
        RequestDisconnectSession(session);

    InterlockedExchange(&session->_sendSubmitBusy, FALSE);

    IOCountDecrement(session);
}
#endif // !USE_LOCKFREE_SENDQ

void CIOCPServer::EchoTestSend(CSession* session, CSerialBuffer* pMsg)
{
    // 에코 테스트: 받은 패킷을 그대로 돌려보냄
    // 송신 메트릭은 호출자(여기)가 집계 (RequestSendMsg는 enqueue 성공 여부만 반환)
    const int dataSize = pMsg->GetDataSize();
    if (RequestSendMsg(session->_sessionId, pMsg))
    {
        _monitor._sendPackets.Inc();
        _monitor._sendEnqueuedBytes.Add(static_cast<LONG64>(dataSize));
    }
}

// 게임 로직 레이어가 사용할 송신 인터페이스 (콘텐츠가 조립한 CSerialBuffer를 그대로 전달)
//
// 소유권 계약: 호출자는 RefCount >= 1인 pMsg를 넘긴다. 이 함수는 해당 1개 ref를 소비한다.
//  - LockFreeQ : Enqueue 성공 → ref가 sendQ로 이전, ProcessSend 완료 시 SubRef()로 반환
//  - RingBuffer: 바이트를 세션 링버퍼에 복사 후 즉시 SubRef()
//  - 실패(세션 무효, ABA, Enqueue 실패 등) → 즉시 SubRef()로 반환
bool CIOCPServer::RequestSendMsg(int64_t sessionId, CSerialBuffer* pMsg, [[maybe_unused]] SendFlush flush)
{
    // ── Session ABA 검증 (3-step) — 두 모드 공통 ──
    // Step 1: index로 세션 조회 + sessionId 일치 확인
    auto session = FindSession(sessionId);
    if (!session) { pMsg->SubRef(); return false; }

    // Step 2: IOCount pin (해제 진행 중이면 실패)
    if (!AcquireSession(session)) { pMsg->SubRef(); return false; }

    // Step 3: pin 후 sessionId 재확인 (pin 사이 재할당 검출)
    if (session->_sessionId != sessionId) { IOCountDecrement(session); pMsg->SubRef(); return false; }

    // 송신 메트릭(_sendPackets/_sendEnqueuedBytes)은 호출자가 집계 (broadcast 타겟별 원자연산 배치)
    [[maybe_unused]] const int dataSize = pMsg->GetDataSize();

#if USE_LOCKFREE_SENDQ
    // SendQ 깊이 상한 체크 (OOM 방어 — CLockFreeQueue는 Enqueue 실패를 반환하지 않으므로)
    if (session->_sendQ.GetApproxSize() >= CSession::MAX_SENDQ_DEPTH)
    {
        LOG_ERROR_STREAM("[Error] SendQ depth limit reached - SessionId: " << sessionId
            << ", Depth: " << session->_sendQ.GetApproxSize());
        _monitor._sendQueueOverflow.Inc();
        pMsg->SubRef();
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return false;
    }

    // 포인터 직접 Enqueue — 소유권이 sendQ로 이전 (SubRef는 ProcessSend에서)
    if (!session->_sendQ.Enqueue(pMsg))
    {
        LOG_ERROR_STREAM("[Error] Send queue enqueue failed - SessionId: " << sessionId);
        _monitor._sendQueueOverflow.Inc();
        pMsg->SubRef();
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return false;
    }
#else
    // RingBuffer 경로: 버퍼 바이트를 세션 링버퍼에 복사 (ref는 복사 직후 소비)
    size_t enqueued = session->_sendQ.Enqueue(pMsg->GetReadBufferPtr(), dataSize);
    if (enqueued != static_cast<size_t>(dataSize))
    {
        LOG_ERROR_STREAM("[Error] Send buffer overflow - SessionId: " << sessionId
            << ", Requested: " << dataSize << ", Enqueued: " << enqueued);
        _monitor._sendQueueOverflow.Inc();
        pMsg->SubRef();
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return false;
    }

    pMsg->SubRef();   // 링버퍼가 바이트 사본 보유 → 버퍼 소유권(ref) 소비
#endif

#if USE_SEND_COALESCING
    if (flush == SendFlush::Immediate)
    {
        TransportSendImmediate(session, sessionId);
    }
    else if (!session->_sendDirty)   // SendFlush::Deferred — 틱 끝 FlushPendingSends에서 묶어 송신
    {
        // 이번 틱 첫 enqueue 세션만 dirty 등록(중복 방지)
        session->_sendDirty = true;
        _dirtySessions.push_back(session);
    }
#else
    PostSend(session);   // coalescing off: Deferred 요청도 즉시 송신 (flush 인자 무시)
#endif
    IOCountDecrement(session);
    return true;
}

// raw 바이트 송신 — 세션 핀/검증(3-step)은 RequestSendMsg와 동일, 차이는 두 가지뿐:
//   ① CSerialBuffer가 아닌 raw 포인터를 받아 ref 소비가 없다 (호출자 소유의 연접 버퍼)
//   ② 항상 Deferred (틱 끝에서만 호출되는 전제 → 직후 FlushPendingSends가 묶어 송신)
// 송신 메트릭(_sendPackets/_sendEnqueuedBytes)은 호출자가 집계 (RequestSendMsg와 동일 계약).
bool CIOCPServer::RequestSendRaw(int64_t sessionId, const char* data, int size)
{
    // ── Session ABA 검증 (3-step) — RequestSendMsg와 동일 ──
    auto session = FindSession(sessionId);
    if (!session) return false;

    if (!AcquireSession(session)) return false;

    if (session->_sessionId != sessionId) { IOCountDecrement(session); return false; }

    // 링버퍼 적재 — digest 전체가 못 들어가면 기존과 동일한 과부하 정책(끊기)
    size_t enqueued = session->_sendQ.Enqueue(data, size);
    if (enqueued != static_cast<size_t>(size))
    {
        LOG_ERROR_STREAM("[Error] Send buffer overflow (digest) - SessionId: " << sessionId
            << ", Requested: " << size << ", Enqueued: " << enqueued);
        _monitor._sendQueueOverflow.Inc();
        RequestDisconnectSession(session);
        IOCountDecrement(session);
        return false;
    }

#if USE_SEND_COALESCING
    if (!session->_sendDirty)   // 틱 끝 FlushPendingSends에서 묶어 송신
    {
        session->_sendDirty = true;
        _dirtySessions.push_back(session);
    }
#else
    PostSend(session);   // (도달 불가 — BuildConfig가 COALESCING=1을 강제하지만 방어적으로 유지)
#endif
    IOCountDecrement(session);
    return true;
}

// [coalescing] 게임 루프가 틱 끝에 1회 호출 — 이번 틱에 송신 데이터가 쌓인 세션을 한 번에 flush.
// PostSend가 내부에서 AcquireSession/_disconnecting을 재검증하므로 dirty 등록 후 세션이 끊겨도 안전.
void CIOCPServer::FlushPendingSends()
{
    TransportFlushDirty();
}


// ParsePackets 쪽에서 호출
void CIOCPServer::PushNetworkEvent(NetworkEvent&& event)
{
    // enqueue 시각 스탬프 (게임루프에서 handle-latency = 처리완료시각 - 이 값 으로 계산)
    // steady_clock은 프로세스 전역 단조 → 워커 push / 게임루프 pop 간 비교 안전
    event.enqueueTimeNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    _eventQueue.Push(std::move(event));
}

// GameLogicThread 쪽에서 호출
bool CIOCPServer::PopNetworkEvent(NetworkEvent& event)
{
    return _eventQueue.TryPop(event);
}

ServerMode CIOCPServer::GetServerMode() const
{
    return _serverMode;
}

// 순수 종료 유도 — releaseFlag 설정 + pending IO 완료 유도로 IOCount 0 수렴을 이끈다.
//   IOCP: CancelIoEx가 pending을 에러 완료시킨다.
//   RIO : CancelIoEx가 없다 — 소유 워커의 closesocket이 그 역할 (Phase 0 스모크 실측).
bool CIOCPServer::RequestDisconnectSession(CSession* session)
{
    if (!session)
        return false;

    return TransportRequestDisconnect(session);
}

// 세션 포인터의 lifetime만 pin한다. SessionID 검증은 외부 진입점에서 별도로 수행한다.
// CAS로 IOCount가 0인 세션(해제 완료/진행 중)은 절대 증가시키지 않는다.
bool CIOCPServer::AcquireSession(CSession* session)
{
    if (!session)
        return false;

    if (session->_disconnecting == TRUE)
        return false;

    // CAS 루프: IOCount > 0일 때만 +1. 0이면 즉시 실패.
    while (true)
    {
        LONG current = session->_ioCount;
        if (current <= 0)
            return false;

        if (InterlockedCompareExchange(&session->_ioCount, current + 1, current) == current)
            break;
    }

    // CAS 성공 후 releaseFlag 재확인.
    // IOCount >= 2이므로 세션 해제/재할당 불가
    if (session->_disconnecting == TRUE)
    {
        IOCountDecrement(session);
        return false;
    }

    return true;
}

// IOCount를 1 감소시킨다. 0이 되면 ReleaseSession을 호출한다.
// AcquireSession의 CAS가 IOCount=0 세션 증가를 차단하므로, 0 도달 스레드는 유일하다.
void CIOCPServer::IOCountDecrement(CSession* session)
{
    if (!session)
        return;

    const LONG count = InterlockedDecrement(&session->_ioCount);
    if (count < 0)
    {
        // 언더플로 복구 — 0으로 리셋 후 세션 해제하여 인덱스 누수를 방지한다.
        // TODO: 크래시 로직 도입 후 여기서 즉시 중단하는 것이 안전함
        InterlockedExchange(&session->_ioCount, 0);
        LOG_ERROR_STREAM("[Error] IOCount underflow - SessionId: " << session->_sessionId);
        ReleaseSession(session);
        return;
    }

    if (count == 0)
    {
        ReleaseSession(session);
    }
}

// 세션 최종 정리 — IOCount==0 시점에 단일 스레드에서만 호출된다.
// 컨텐츠 알림, 소켓 종료, 인덱스 반환을 수행한다.
void CIOCPServer::ReleaseSession(CSession* session)
{
    // RequestDisconnectSession을 거치지 않고 IOCount==0에 도달한 비정상 경로 방어.
    // releaseFlag를 강제 설정하여 이후 RequestDisconnectSession 재진입을 차단한다.
    if (InterlockedExchange(&session->_disconnecting, TRUE) == FALSE)
    {
        LOG_ERROR_STREAM("[Error] ReleaseSession without RequestDisconnectSession - SessionId: " << session->_sessionId);
    }

    const int64_t sessionId = session->_sessionId;

    // 컨텐츠 알림 — IOCount==0이므로 이후 RECEIVED 이벤트 불가. 이벤트 순서 보장.
    // 서버 종료 중(_running==false)에는 알림 생략.
    if (sessionId != 0 && _running == TRUE)
    {
        switch (_serverMode)
        {
        case ServerMode::GameServer:
            PushNetworkEvent(NetworkEvent(NetworkEvent::Type::DISCONNECTED, sessionId));
            break;
        default:
            break;
        }
    }

    // 타이밍 휠에서 세션 제거 (이중 만료 방지)
    _timingWheel->RequestUnregister(CSession::ExtractIndex(sessionId), sessionId);

    // Disconnect 시 SendQ 잔여 바이트를 discard 카운터에 적산 (체류량 보정)
    // IOCount==0 단일 스레드 호출이므로 SendQ 경합 없음
#if USE_LOCKFREE_SENDQ
    size_t residual = 0;
    CSerialBuffer* orphan = nullptr;
    while (session->_sendQ.Dequeue(&orphan))
    {
        residual += orphan->GetDataSize();
        orphan->SubRef();
    }
    residual += session->_pendingSendBytes;  // WSASend 미완료 버퍼도 discard 집계
    session->ReleasePendingSendBufs();
    if (residual > 0)
        _monitor._sendDiscardedBytes.Add(static_cast<LONG64>(residual));
#else
    size_t residual = session->_sendQ.GetDataSize();
    if (residual > 0)
        _monitor._sendDiscardedBytes.Add(static_cast<LONG64>(residual));
#endif

    session->Close();

    if (sessionId != 0)
    {
        // 세션 지표 기록
        _monitor._sessionDestroyed.Inc();
        _monitor._sessionCount.Add(-1);

        _availableIndices.Push(CSession::ExtractIndex(sessionId));
    }
}

bool CIOCPServer::RequestDisconnectSession(int64_t sessionId)
{
    // Session ABA 검증 (3-step)
    // Step 1: index로 세션 조회 + sessionId 일치 확인
    auto session = FindSession(sessionId);
    if (!session)
        return false;

    // Step 2: IOCount pin (해제 진행 중이면 실패)
    if (!AcquireSession(session))
        return false;

    // Step 3: pin 후 sessionId 재확인 (pin 사이 재할당 검출)
    if (session->_sessionId != sessionId)
    {
        IOCountDecrement(session);
        return false;
    }

    const bool disconnected = RequestDisconnectSession(session);
    IOCountDecrement(session);
    return disconnected;
}

// 타이밍 휠 타임아웃 콜백 — 타이머 스레드에서 호출된다.
// sessionId를 통해 ABA-safe 경로(FindSession → AcquireSession → sessionId 재검증)를 사용한다.
void CIOCPServer::OnSessionTimeout(void* context, int64_t sessionId)
{
    auto* server = static_cast<CIOCPServer*>(context);
    server->_monitor._sessionTimedOut.Inc();
    server->RequestDisconnectSession(sessionId);
}

// 여러 스레드에서 접근가능 — ref를 잡지 않으므로 반환된 포인터는 잠정적이다.
// 반드시 AcquireSession + sessionId 재확인 후 사용해야 한다.
CSession* CIOCPServer::FindSession(int64_t sessionId)
{
    uint16_t index = CSession::ExtractIndex(sessionId);

    if (index >= _sessions.size())
        return nullptr;

    auto& session = _sessions[index];
    if (!session)
        return nullptr;

    if (session->_disconnecting == FALSE && session->_sessionId == sessionId)
        return session.get();

    return nullptr;
}
