#pragma once

#include <CoreConfig.h>   // USE_LOCKFREE_SENDQ 등 전송 토글 (가장 먼저 include)

#ifdef _WIN32
// WinSock2는 Windows.h보다 먼저 와야 한다(구버전 winsock.h가 딸려와 sockaddr이 재정의된다).
//   Platform.h가 Windows.h를 들이므로 그보다 앞에 둔다. 리눅스 소켓 헤더는 Platform.h가 제공한다.
#include <WinSock2.h>
#include <WS2tcpip.h>
#endif
#include "Platform/Platform.h"   // 소켓 핸들 타입·소켓 API 경계 (리눅스는 소켓 헤더까지)
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <queue>
#include <functional>
#include <array>
#include <atomic>

#include <RingBuffer.h>
#include "SerialBuffer.h"
#include "NetHeader.h"   // 와이어 헤더만 — 게임 패킷 정의(Protocol.h)는 보지 않는다
#include "LockFreeConfig.h"      // 락프리 스택/큐 (형제 저장소 MyGit\LockFree 참조)
#if USE_RIO_TRANSPORT
#include "RioApi.h"      // RIO 함수 테이블 + 등록 슬랩 (전송 교체 경로 전용)
#endif
#include "TimingWheel.h"
#include "MonitorManager.h"
#include "Common.h"

#pragma comment(lib, "ws2_32.lib")

// CSerialBuffer의 "실제로 쓸 수 있는" 용량과 일치시킨다.
//   버퍼는 MSG_DEFAULT_SIZE(1460B)를 잡지만 선두 HEADER_SIZE(2B)는 길이 헤더 자리라
//   GetWriteBufferPtr()이 _Buff+2를 돌려준다 → 가용은 1458B.
//   여기에 1460을 두면 ParsePackets의 Dequeue가 할당 밖 2B를 침범한다.
constexpr size_t MAX_PACKET_SIZE = MSG_DEFAULT_SIZE - HEADER_SIZE;  // 1458B
constexpr size_t MIN_PACKET_SIZE = sizeof(EchoMsgHeader);  // 최소 패킷 크기 (가장 작은 헤더 기준)

// I/O 작업 종류
enum class IOOperation
{
    RECV,
    SEND,
    ACCEPT
};



class CSession
{
public:

    // 세션에 고정 보관되는 OVERLAPPED 확장 구조체
    struct OverlappedEx
    {
#ifdef _WIN32
        OVERLAPPED overlapped;      // 반드시 첫 번째 멤버 (완료 통지가 이 주소를 돌려준다)
#endif
        IOOperation operation;      // I/O 타입 (RECV, SEND, ACCEPT 등)
        int slot = -1;              // [다중 pending 송신] SEND일 때 몇 번 슬롯인가 (RECV는 -1)
    };
    // epoll은 "제출하고 완료를 돌려받는" 모델이 아니라 "fd가 준비됐다"는 통지라
    //   OVERLAPPED에 해당하는 것이 없다. 나머지 필드(operation/slot)는 양쪽 공통이다.

    explicit CSession();
    virtual ~CSession();

    void Initialize(SOCKET socket, int64_t sessionId);
    void Close();


    // SessionID 구조 헬퍼
    static constexpr int SESSION_INDEX_BITS = 16;
    static constexpr int SESSION_MAX_COUNT = 0xFFFF; // index 0~65534, 65535(0xFFFF)는 예약
    static constexpr int64_t SESSION_INDEX_MASK = 0xFFFF000000000000LL;
    static constexpr int64_t SESSION_UNIQUE_MASK = 0x0000FFFFFFFFFFFFLL;

    static uint16_t ExtractIndex(int64_t sessionId)
    {
        return static_cast<uint16_t>((sessionId >> 48) & 0xFFFF);
    }

    static int64_t ExtractUniqueId(int64_t sessionId)
    {
        return sessionId & SESSION_UNIQUE_MASK;
    }

    static int64_t MakeSessionId(uint16_t index, int64_t uniqueId)
    {
        return (static_cast<int64_t>(index) << 48) | (uniqueId & SESSION_UNIQUE_MASK);
    }

public:
    volatile LONG _ioCount;         // CAS로 0→증가 방지. pending I/O 개수 (base ref 없음)
    volatile LONG _disconnecting;   // 종료 진행 플래그. InterlockedExchange로 1회만 설정
    volatile SOCKET _socket;
    volatile int64_t _sessionId;    // Initialize에서 설정, IOCount>0 동안 유효
    // [다중 pending 송신] 1-pending 시절의 _sending 하나를 두 역할로 나눈 것 —
    //   _sendSubmitBusy : 제출 구간만 잠근다(세그먼트 계산~제출 호출). 제출자가 둘인 IOCP 팔
    //                     (송신 워커 + 완료 워커의 이어보내기)에서 제출 순서=와이어 순서를 지킨다.
    //   _sendInFlight   : 미완료 제출 수. 완료가 감소시키며, 상한이 곧 송신 깊이(_sendDepth).
    //   깊이 1이면 두 역할이 다시 겹쳐 옛 _sending과 같은 동작이 된다.
    //   (USE_LOCKFREE_SENDQ 경로는 깊이를 쓰지 않고 _sendSubmitBusy만 옛 _sending처럼 쓴다)
    volatile LONG _sendSubmitBusy;  // InterlockedExchange 사용 — 완료 워커가 고빈도 갱신
    volatile LONG _sendInFlight;

    // 게임 스레드가 만지는 송신 표식 묶음 — 위쪽 송신 게이트(완료 워커 고빈도 갱신)와
    // 캐시라인 분리하여 false sharing 방지. (_queuedForSend는 송신 스레드도 clear)
    alignas(64) bool _sendDirty = false;   // [coalescing] 틱 내 송신 대기 표식 (게임 스레드 전용, Initialize에서 리셋)
    volatile LONG _queuedForSend = FALSE;  // [send-worker] 워커 queue 잔류 표식 — 틱을 넘는 중복 push 방지.
                                           // 게임 스레드(push 시 set)·송신 스레드(처리 전 clear) 공유 → Interlocked 필수.

    CRingBufferST _recvQ; // 한 스레드에서만 접근

#if USE_LOCKFREE_SENDQ
    LockFree::CLockFreeQueue<CSerialBuffer*, false, true> _sendQ;

    static constexpr int MAX_SEND_BUFS = 64;
    static constexpr INT64 MAX_SENDQ_DEPTH = 512;  // SendQ 깊이 상한 (OOM 방어)
    CSerialBuffer* _pendingSendBufs[MAX_SEND_BUFS];
    int _pendingSendCount = 0;
    int _pendingSendBytes = 0;

    void ReleasePendingSendBufs()
    {
        for (int i = 0; i < _pendingSendCount; ++i)
        {
            if (_pendingSendBufs[i])
            {
                _pendingSendBufs[i]->SubRef();
                _pendingSendBufs[i] = nullptr;
            }
        }
        _pendingSendCount = 0;
        _pendingSendBytes = 0;
    }
#else
    CRingBufferMT _sendQ; // 다중 스레드에서 접근
#endif


    // IOCount가 0이 되어 세션이 재사용되기 전까지 OVERLAPPED 주소는 유지된다.
    OverlappedEx _recvOverlapped;

    // [다중 pending 송신] 세션당 in-flight 슬롯 링.
    //   제출마다 슬롯 하나를 쓰고 완료가 그 슬롯을 비운다. 깊이 1이면 슬롯 0만 돌려쓰므로
    //   옛 단일 _sendOverlapped와 동작이 같다.
    //   [왜 done 플래그가 필요한가] IOCP는 완료 워커가 여럿이라 완료 통지 순서가 제출 순서와
    //     어긋날 수 있다. 링 읽기 포인터는 반드시 오래된 구간부터 전진해야 하므로,
    //     도착한 슬롯에 표식만 남기고 head부터 "연속으로 done인 구간"만 반환한다.
    //     (RIO는 단일 CQ를 소유 워커가 단독 드레인해 항상 순서대로 오지만 같은 코드를 쓴다)
    static constexpr int MAX_SEND_DEPTH = 8;

    struct SendSlot
    {
        OverlappedEx  ov;                // IOCP 제출용 (RIO는 세션 RQ에 제출하므로 미사용)
        size_t        bytes = 0;         // 이 제출의 크기 — 부분 송신 판정·링 반환량
        volatile LONG done = FALSE;      // 완료 도착 표식
    };
    SendSlot      _sendSlots[MAX_SEND_DEPTH];
    ULONG         _slotTail = 0;         // 다음 제출이 쓸 슬롯 — 제출 잠금(_sendSubmitBusy) 안에서만.
                                         //   unsigned인 이유: 오버플로 wrap이 정의된 동작이어야 한다
                                         //   (signed는 표준상 UB). 마스크 인덱싱이 wrap을 넘어도 성립.
    volatile LONG _slotHead = 0;         // 가장 오래된 미완료 슬롯 — 수거자가 전진
    volatile LONG _sendReapBusy = FALSE; // 링 반환 구간 직렬화 (완료 워커가 여럿이라 필요)

    // 슬롯 링 초기화 — 생성 시와 세션 재사용(Initialize) 시 모두 호출한다.
    //   ov.slot은 고정 인덱스라 여기서 한 번만 심으면 완료 통지가 자기 슬롯을 알 수 있다.
    void ResetSendSlots()
    {
        for (int i = 0; i < MAX_SEND_DEPTH; ++i)
        {
#ifdef _WIN32
            ZeroMemory(&_sendSlots[i].ov.overlapped, sizeof(OVERLAPPED));
#endif
            _sendSlots[i].ov.operation = IOOperation::SEND;
            _sendSlots[i].ov.slot = i;
            _sendSlots[i].bytes = 0;
            _sendSlots[i].done = FALSE;
        }
        _slotTail = 0;
        _slotHead = 0;
        _sendReapBusy = FALSE;
    }

#if USE_RIO_TRANSPORT
    // RIO 요청 큐 — 생성·제출·closesocket 전부 소유 워커 스레드에서만 접근 (불변식).
    // 소켓 수명을 따르므로 별도 해제 API 없음. 세션 재사용 시 NewConn 처리에서 재생성.
    RIO_RQ _rq = RIO_INVALID_RQ;

    // 재사용 리셋의 팔별 부분 — Initialize가 호출한다 (.cpp에서 전송 계층 #if를 없애기 위한 위임).
    void ResetTransportState() { _rq = RIO_INVALID_RQ; }
#else
    // RIO가 아닌 팔(IOCP·epoll)의 재사용 리셋.
    //   epoll 표식은 리눅스에만 있으므로 조건부로 만진다 — Windows IOCP 빌드도 이 분기를 탄다.
    void ResetTransportState()
    {
#ifndef _WIN32
        _epollWantWrite = false;
        _epollRegistered = FALSE;
        _recvBusy = FALSE;
#endif
    }
#endif
#ifndef _WIN32
    // EPOLLOUT을 지금 걸어 두었는가 — 같은 상태로 epoll_ctl을 반복 호출하지 않기 위한 표식.
    //   제출 잠금(_sendSubmitBusy) 안에서만 갱신되므로 별도 원자화가 필요 없다.
    bool _epollWantWrite = false;

    // epoll에 등록되어 있는가 — "IOCount=1(등록 ref)을 놓아 줄 사람이 아직 남았다"는 뜻이다.
    //   등록한 적 없는 세션(attach 실패분·종료 루프가 훑는 미접속 세션)에서 감소가 일어나
    //   0→−1이 되는 것을 막는다. TransportRequestDisconnect가 집어내며 지우므로 감소는 한 번뿐.
    //   ※ 선언부에서 초기화하는 이유 — 미접속 세션은 Initialize(=ResetTransportState)를 한 번도
    //     타지 않는다. 리셋에만 맡기면 그 세션의 값이 초기화되지 않은 채 판정에 쓰인다.
    volatile LONG _epollRegistered = FALSE;

    // 수신 직렬화 게이트 — 이 세션을 지금 읽고 있는 워커가 있는가.
    //   _recvQ는 CRingBufferST(잠금 없음)라 "한 스레드만 만진다"가 전제인데, epoll은 아직 다 비우지
    //   않은 소켓을 여러 워커에게 거듭 알려(level-trigger) 그 전제를 깬다. IOCP는 recv를 세션당
    //   하나만 걸어 두어 완료도 워커 하나에만 가므로 이 게이트가 필요 없었다.
    //   [임시] 워커별 epoll(SO_REUSEPORT)로 세션 소유를 고정하면 이 게이트는 통째로 사라진다.
    volatile LONG _recvBusy = FALSE;
#endif
};


// 네트워크 레이어에서 게임 로직으로 전달하는 이벤트
struct NetworkEvent
{
    enum class Type
    {
        CONNECTED,
        DISCONNECTED,
        RECEIVED
    };

    Type type;
    int64_t sessionId;
    CSerialBuffer* pMsg;
    int64_t enqueueTimeNs = 0;   // PushNetworkEvent에서 스탬프 (handle-latency 측정용 steady_clock ns)

    NetworkEvent(Type t, int64_t id)
        : type(t), sessionId(id), pMsg(nullptr)
    {
    }

    NetworkEvent(Type t, int64_t id, CSerialBuffer* msg)
        : type(t), sessionId(id), pMsg(msg)
    {
    }
};


// 스레드 안전한 큐
template<typename T>
class ThreadSafeQueue
{
public:
    void Push(T&& item)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _queue.push(std::move(item));
    }

    bool TryPop(T& item)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_queue.empty())
        {
            return false;
        }
        item = std::move(_queue.front());
        _queue.pop();
        return true;
    }

    // 공유 큐를 통째로 빼고 즉시 해제 (단일 소비자 전용 — 락 보유 구간을 swap 1회로 축소)
    // out에 남은 잔여가 있으면 그대로 두고 뒤에 이어붙지 않으므로, 소비자는 비운 큐를 넘길 것
    void SwapOut(std::queue<T>& out)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _queue.swap(out);
    }

    bool IsEmpty() const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _queue.empty();
    }

    size_t GetSize() const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _queue.size();
    }

private:
    std::queue<T> _queue;
    mutable std::mutex _mutex;
};

//TODO: 모드별 설계..

// 송신 디스패치 시점 — 호출부는 의도만 표시하고, Deferred의 실제 지연 여부는
// USE_SEND_COALESCING(CoreConfig.h)이 결정한다. (coalescing off면 Deferred도 즉시 송신)
//   Immediate : 즉시 PostSend (echo·워커 등 단발 송신)
//   Deferred  : 묶어 보내도 되는 게임루프 송신 → 틱 끝 FlushPendingSends에서 일괄
enum class SendFlush { Immediate, Deferred };

// IOCP 기반 네트워크 서버
class CIOCPServer
{
public:
    explicit CIOCPServer(int port, int maxClients, ServerMode mode,
                        CMonitorManager& monitor, int workerThreads = 0, int sendWorkers = 0,
                        int rioWorkers = 0, int completionBatch = 0, int sendDepth = 1);
    virtual ~CIOCPServer();

    bool Start();
    void ShutdownServer();

    // 게임 로직 레이어가 사용할 인터페이스 (직접 호출)
    // thread-safe하다면 굳이 큐방식으로 부하를 줄 필요가 없음.
    // 송신은 콘텐츠가 조립한 CSerialBuffer 단일 경로로 통일 (모드 분기는 .cpp 내부 #if)
    // 반환: enqueue 성공 true / 실패(세션무효·ABA·큐오버플로) false — 송신 메트릭은 호출자가 집계 (broadcast 배치)
    bool RequestSendMsg(int64_t sessionId, CSerialBuffer* pMsg, SendFlush flush = SendFlush::Immediate);

    // raw 바이트 송신 — RequestSendMsg의 링버퍼 경로에서 버퍼 소유권(ref 소비)만 뺀 변형.
    // 연접된 바이트열을 세션당 "핀 1회 + 링 적재 1회"로 넣는다.
    // 소유권 없음: 링이 즉시 복사하므로 data는 호출 동안만 유효하면 됨. 송신은 Deferred(틱 끝 flush) 고정.
    //
    // 항상 제공한다 — 게임 토글(USE_BROADCAST_BUNDLE)로 켜고 끄던 것을 걷어냈다.
    // 전송 계층은 "누가 이걸 쓰는가"를 알 필요가 없고, 알면 게임 쪽 헤더에 묶인다.
    // 안 쓰는 빌드에서는 호출자가 없어 그냥 안 불릴 뿐이다.
    bool RequestSendRaw(int64_t sessionId, const char* data, int size);

    bool RequestDisconnectSession(int64_t sessionId);

    // [coalescing] 게임 루프가 틱 끝에 1회 호출 — 이번 틱에 enqueue된 세션을 한 번에 flush (게임 스레드 단독)
    void FlushPendingSends();

    // 게임 로직 레이어로 전달할 이벤트 가져오기 (QUEUE_BASED 모드용)
    bool PopNetworkEvent(NetworkEvent& event);

    // 프레임 진입 시점의 이벤트를 통째로 스왑해 가져오기 (게임 루프 단일 소비자 전용)
    // 건당 락(N회/프레임)을 swap 1회/프레임로 축소. out은 비운 큐를 넘길 것
    void SwapOutNetworkEvents(std::queue<NetworkEvent>& out) { _eventQueue.SwapOut(out); }

    // 이벤트 큐 현재 크기 (모니터링용)
    size_t GetEventQueueSize() const { return _eventQueue.GetSize(); }

    // 이벤트 큐 과부하 시 신규 accept를 거절할 임계 (0=끄기). Start 전에 설정할 것.
    void SetEventQueueAcceptLimit(int limit) { _eventQueueAcceptLimit = (limit > 0) ? limit : 0; }

    // 서버 모드 가져오기
    ServerMode GetServerMode() const;

    // 내부에서 사용할 함수
private:
    bool RequestDisconnectSession(CSession* session);
    void ReleaseSession(CSession* session);

private:

    void EchoTestSend(CSession* session, CSerialBuffer* pMsg);

    // 게임 로직으로 이벤트 전달 (QUEUE_BASED 모드용)
    void PushNetworkEvent(NetworkEvent&& event);

    void AcceptThread();
#if !USE_RIO_TRANSPORT
    void WorkerThread();

    // 완료 1건 처리 — 수거 경로 두 개(GQCS/GQCSEx)가 완전히 같은 판정을 쓰도록 떼어냈다.
    //   ioFailed = GQCS의 result==FALSE에 해당. GQCSEx는 항목별 성공/실패 BOOL을 주지 않아
    //   호출자가 OVERLAPPED::Internal(NTSTATUS)로 만들어 넘긴다.
#ifdef _WIN32
    // 완료 통지를 돌려받는 모델은 IOCP뿐이라 Windows 전용이다 (epoll은 4-D에서 별도 경로).
    void HandleCompletion(OVERLAPPED* overlapped, ULONG_PTR completionKey,
                          DWORD bytesTransferred, bool ioFailed, int workerIndex);
#endif  // _WIN32
#endif  // !USE_RIO_TRANSPORT
#ifndef _WIN32
    // epoll 워커 — 준비 통지(EPOLLIN/EPOLLOUT)를 받아 처리한다. IOCP의 WorkerThread에 대응.
    void EpollWorkerThread(int workerIndex);
    void EpollHandleReadable(CSession* session);   // EPOLLIN — 직접 읽어 ProcessRecv로 넘긴다
    void EpollSendSession(CSession* session);      // 미전송 구간을 writev로 내보낸다 (즉시 완료)
    void EpollUpdateWriteInterest(CSession* session, bool wantWrite);   // EPOLLOUT 등록/해제
#endif
#if USE_SEND_THREAD && !USE_RIO_TRANSPORT
    void SendWorkerThread(int workerIdx);   // 전용 송신 워커 — 자기 워커의 dirty 배치를 받아 WSASend 수행
#endif

    bool CreateListenSocket();
    bool SetSocketOptions(SOCKET socket);
#if !USE_RIO_TRANSPORT
    bool BindIOCP(SOCKET socket, ULONG_PTR completionKey);
#endif

    void ProcessAccept(SOCKET clientSocket);
    void ProcessRecv(CSession* session, DWORD bytesTransferred);
    // slot = 이 완료가 쓴 in-flight 슬롯 (IOCP는 OverlappedEx.slot, RIO는 RequestContext에서 얻는다)
    void ProcessSend(CSession* session, DWORD bytesTransferred, int slot);

    // 전송 계층 제출 — 선언은 공통, 구현은 팔별 파일(Transport_Iocp.cpp / Transport_Rio.cpp).
    //   RIO 판은 요청당 버퍼가 1개라 링버퍼의 직선 구간만 제출한다 — 나머지 계약은 동일.
    void PostRecv(CSession* session, bool skipAcquire = false);
    void PostSend(CSession* session);

#if !USE_LOCKFREE_SENDQ
    // 미제출 구간 한 덩이를 실제로 내보낸다 (팔별). 반환 = 소비한 in-flight 슬롯 수(0 = 제출 실패).
    //   IOCP: 직선+랩을 WSABUF 2개로 한 번에 (슬롯 1)
    //   RIO : 요청당 버퍼가 1개라 직선 구간만 (슬롯 1)
    //   *submittedBytes 에 실제 제출한 바이트를 돌려준다.
    int TransportSubmitSegment(CSession* session, Platform::NetSocket socket,
                               const CRingBufferMT::SubmitInfo& info,
                               int slot, int slotsAvailable, size_t* submittedBytes);

    // 완료된 슬롯을 head부터 "연속으로" 회수한다 — 링 읽기 포인터는 오래된 구간부터만 전진해야 하고,
    //   IOCP는 완료 통지 순서가 제출 순서와 어긋날 수 있어 표식(done)과 회수를 분리한다.
    void ReapSendSlots(CSession* session);
#endif

    // ── 전송 계층 경계 — 공통 골격이 팔(arm)을 모르게 하는 위임 지점 ──────────────
    //    구현은 Transport_Iocp.cpp(완료 포트)와 Transport_Rio.cpp(RIO) 중 하나만 컴파일된다.
    bool  TransportInitSessionBuffer(CSession* session);   // 세션 링버퍼 준비 (RIO는 슬랩 슬라이스라 no-op)
    bool  TransportPreListen();                            // 리슨 소켓 "전" 준비 (IOCP: 완료 포트 생성)
    void  TransportPreListenCleanup();                     // 리슨 실패 시 위 준비물 되돌리기
    bool  TransportPostListen();                           // 리슨 소켓 "후" 준비 (RIO: 테이블·슬랩·CQ)
    uint32_t TransportListenFlags() const;                 // 소켓 생성 플래그 (RIO: REGISTERED_IO 추가)
    void  TransportStartWorkers();                         // 완료 워커·송신 워커 기동
    void  TransportLogStarted(const char* modeName) const; // 기동 로그 (스모크가 이 문구로 팔 판정)
    void  TransportStopBeforeDrain();                      // IOCount 드레인 "전" 정지 (IOCP: 송신 워커)
    void  TransportStopAfterDrain();                       // IOCount 드레인 "후" 정지 (워커 join·자원 해제)
    bool  TransportAttachSession(CSession* session, Platform::NetSocket clientSocket);   // 완료 통지 연결 (IOCP: BindIOCP)
    void  TransportStartFirstRecv(CSession* session, Platform::NetSocket clientSocket, int64_t sessionId);  // 첫 Recv 착수
    void  TransportSendImmediate(CSession* session, int64_t sessionId);     // SendFlush::Immediate 경로
    void  TransportFlushDirty();                           // 틱 끝 dirty 배치 → 송신 (팔별 핸드오프)
    bool  TransportRequestDisconnect(CSession* session);    // 종료 유도 (IOCP: CancelIoEx / RIO: 소유 워커 close)

    void ParsePackets(CSession* session);

    CSession* FindSession(int64_t sessionId);

    // 세션 재사용 방지용 pin/ref 인터페이스
    bool AcquireSession(CSession* session);
    void IOCountDecrement(CSession* session);

private:
    int _port;
    int _maxClients;
    ServerMode _serverMode;
    int _configuredWorkers;   // INI 지정 워커 수 (0=affinity 코어 수로 자동 산정)
    int _configuredSendWorkers;   // INI 지정 송신 워커 수 (0/1=단일)
    int _configuredRioWorkers;    // INI 지정 RIO 워커 수 (0=자동 2, RIO 빌드 전용)
    int _configuredCompletionBatch;   // INI 지정 완료 수거 방식 (0=GQCS, N>0=GQCSEx 상한; IOCP 빌드 전용)
    int _configuredSendDepth;         // INI 지정 송신 깊이 (Start가 2의 거듭제곱으로 내려 _sendDepth 확정)
    int _completionBatch = 0;         // 위 값을 clamp한 실효값 — Start()가 워커 기동 "전"에 확정, 이후 불변
    static constexpr int MAX_COMPLETION_BATCH = 256;   // OVERLAPPED_ENTRY 스택 배열 상한 (256×16B=4KB)
    int _sendDepth = 1;               // 세션당 동시 송신 제출 상한 — 1 = 기존 1-pending 동작 (INI 배선은 후속 단계)
    int _sendDepthMask = 0;           // _sendDepth-1. 깊이를 2의 거듭제곱(1/2/4/8)으로만 허용하는 이유:
                                      //   슬롯 인덱스를 head/tail 카운터의 나머지로 구하는데, 카운터가
                                      //   오버플로로 wrap할 때 2의 거듭제곱이 아니면 head와 tail의 인덱스
                                      //   관계가 어긋난다(엉뚱한 슬롯을 회수). 마스크면 wrap에도 일관하다.
    int _coreCount = 0;               // affinity 가용 코어 수 — TransportPreListen이 산정 (concurrency·로그 공용)
    int _workerThreadCount = 0;       // 실제 기동한 완료 워커 수 (IOCP 팔) — 기동 로그가 읽는다
    CMonitorManager& _monitor;
    volatile LONG _running;
    volatile LONGLONG _sessionIdCounter;  // 고유 ID용 (하위 48비트)

    SOCKET _listenSocket;
#ifdef _WIN32
    HANDLE _iocpHandle;
#else
    // epoll 인스턴스 — IOCP의 완료 포트에 대응하는 자리다. 다만 성격이 다르다:
    //   완료 포트는 "끝난 I/O"를 돌려주고, epoll은 "할 수 있게 된 fd"를 알려준다.
    int _epollFd = -1;
#endif

    std::vector<std::thread> _workerThreads;
    std::thread _acceptThread;

    std::vector<std::unique_ptr<CSession>> _sessions;  // Index 기반 접근가능
    LockFree::CLockFreeStack<uint16_t> _availableIndices;  // 재사용 가능한 인덱스 스택

    // [coalescing] 틱 내 송신 대기 세션 목록 (게임 스레드 단독 접근 → 무락)
    std::vector<CSession*> _dirtySessions;

#if USE_SEND_THREAD && !USE_RIO_TRANSPORT
    // 송신 워커 풀 — 게임루프가 dirty 배치(sessionId)를 uniqueId%K 워커에 넘기고 각 워커가 WSASend.
    //   한 세션은 항상 같은 워커(FIFO 보장). 완료(WSASend 결과)는 기존 IOCP 워커가 처리, 제출만 송신 워커 담당.
    struct alignas(64) SendWorker                      // alignas: 워커 간 false sharing 차단(측정 변수 제거용)
    {
        std::mutex              mutex;
        std::condition_variable cv;
        std::vector<int64_t>    queue;               // 게임스레드 push(lock) / 워커스레드 swap-out
        std::thread             thread;
    };
    std::vector<std::unique_ptr<SendWorker>> _sendWorkers;   // mutex/cv 이동불가 → 힙 고정 + 포인터만 보관
    std::atomic<bool>                        _sendStop{ false };
    int                                      _sendWorkerCount = 1;  // 분배 모듈러(=워커 수)
#endif

#if USE_RIO_TRANSPORT
    // ── RIO 전송 계층 (토글 ON 시 WT 풀·SendWorker 풀을 대체) ─────────────────
    // 세션 소유 워커 = uniqueId % N 고정 (기존 SendWorker 분배와 동일 근거 — index 비트 누수 없음).
    // 불변식: 한 세션의 RQ 조작(생성·RIOReceive/RIOSend 제출·closesocket)은 소유 워커
    //         스레드에서만 수행한다 — CancelIoEx 부재를 이 직렬화가 대체한다.
    // 워커 루프: 명령 드레인 → CQ 드레인(RIODequeueCompletion 배치) → 유휴 시 RIONotify 무장 → 재드레인 → 대기.
    //   (v1은 스핀 없음 — 부하클라 동거 머신 코어 소모 회피. 실측 후 필요 시 추가. 본체 주석 RioWorkerThread 참조)
    struct RioCmd
    {
        enum class Type { NewConn, FlushSend, Disconnect };
        Type      type = Type::FlushSend;
        int64_t   sessionId = 0;                // FlushSend: FindSession 재검증용 (못 찾으면 스킵 = 기존 SendWorker 동작)
        SOCKET    socket = INVALID_SOCKET;      // NewConn 전용 — accept 스레드가 넘긴 새 소켓
        CSession* session = nullptr;            // NewConn: Initialize 완료 세션 (IOCount=1이 pin 역할)
                                                // Disconnect: 요청 스레드가 pin(IOCount+1) 보유 채로 전달.
                                                //   FindSession은 _disconnecting 세션을 숨기므로 id 재조회로는
                                                //   워커가 세션을 못 찾아 closesocket 누락(좀비) — 그래서 포인터+pin.
    };

    struct alignas(64) RioWorker              // alignas: 워커 간 false sharing 차단 (SendWorker와 동일)
    {
        RIO_CQ              cq = RIO_INVALID_CQ;
        HANDLE              cqEvent = nullptr;    // RIONotify 통지용 (CQ 생성 시 등록, auto-reset)
        HANDLE              cmdEvent = nullptr;   // 명령 핸드오프 깨움 (auto-reset)
        std::mutex          cmdMutex;
        std::vector<RioCmd> cmdQueue;             // 외부 push(lock) / 워커 swap-out
        std::thread         thread;
    };
    std::vector<std::unique_ptr<RioWorker>> _rioWorkers;   // mutex 이동불가 → 힙 고정 (SendWorker와 동일)
    std::atomic<bool> _rioStop{ false };
    int               _rioWorkerCount = 1;
    CRioSlab          _rioSlab;               // 전 세션 recv/send 링버퍼 슬랩 (등록 1회 = 물리 고정)

    void RioWorkerThread(int workerIdx);
    void RioHandleCmd(RioWorker& worker, RioCmd& cmd);               // 명령 1건 처리 (소유 워커 위)
    int  RioDrainCompletions(RioWorker& worker, int monitorIndex);   // CQ 한 배치 처리 (−1 = CQ 손상)
    void RioCloseSocketOnOwner(CSession* session);                   // 소유 워커 전용 closesocket (CancelIoEx 대체)
    void RioEnqueueCmd(int ownerIdx, RioCmd&& cmd);                  // 명령 push + cmdEvent Set
    int  RioOwnerIndex(int64_t sessionId) const                      // uniqueId 분배 (48비트 마스크 → 음수 없음)
    {
        return static_cast<int>(CSession::ExtractUniqueId(sessionId) % _rioWorkerCount);
    }
#endif

    // 레이어 간 통신 큐 (QUEUE_BASED 모드용)
    ThreadSafeQueue<NetworkEvent> _eventQueue;    // 네트워크 -> 게임 로직

    // 이 큐가 이 길이를 넘으면 신규 accept를 거절한다 (0=끄기). 설정은 기동 전 1회, 이후 읽기 전용.
    int _eventQueueAcceptLimit = 0;

    // 세션 무활동 타임아웃 (타이밍 휠)
    static constexpr int SESSION_TIMEOUT_SEC = 60;
    static constexpr int TIMER_TICK_INTERVAL_MS = 1000;
    std::unique_ptr<CTimingWheel> _timingWheel;

    static void OnSessionTimeout(void* context, int64_t sessionId);
};
