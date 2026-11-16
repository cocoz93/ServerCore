#pragma once

#include <cstdint>

class CSerialBuffer;

//==================================================
// 받은 것 넘길 곳 — 접속 · 받음 · 끊김 세 알림을 받는 부품
//
//   안 주면(기본) 지금 길 그대로다: 세 알림을 이벤트 큐에 넣고 쓰는 쪽 스레드가 PopNetworkEvent 로 꺼낸다
//     (받은 것은 세션별 큐 SetSessionRecvQueue 가 정해져 있으면 그리로 간다).
//   주면 그 일이 난 스레드가 그 자리에서 부른다 — 큐를 안 거친다.
//     접속 = 접속 받는 스레드(ConnectTo 로 건 것이면 그 함수를 부른 스레드) · 받음 = 워커 · 끊김 = 세션을 놓는 스레드(대개 워커)
//     오래 붙잡으면 그 스레드의 다른 일(다음 접속 · 다른 세션의 완료)이 밀린다.
//   쓰는 쪽이 Start 전에 고른다(CIOCPServer::SetRecvSink).
//
//   왜 가상 함수 부품인가 — 자르기 부품(MessageFramer.h)과 같다. 이 라이브러리가 쓰는 쪽 처리를 모르게 둔다.
//     부품이 없을 때는 가상 호출 없이 큐 길로 간다(분기 하나).
//   왜 알림을 구조체로 넘기나 — 칸을 더해도(접속한 주소 · 끊긴 까닭 등) 부르는 꼴이 안 바뀐다.
//==================================================

// 끊긴 까닭 — 세션마다 먼저 일어난 하나만 남는다(뒤에 겹친 것은 버린다).
//   쓰는 쪽은 기록 줄 · 셈에 쓴다. 코어의 끊기 동작은 까닭에 따라 안 바뀐다.
enum class DisconnectReason : uint8_t
{
    IoError = 0,    // 입출력 실패 · 상대의 강제 끊기 — 아래 어느 표식도 없이 끊긴 것
    PeerClosed,     // 상대가 정상으로 닫음(받기가 0바이트)
    LocalRequest,   // 쓰는 쪽이 RequestDisconnectSession 을 부름
    IdleTimeout,    // 쉬는 접속 끊기(SetIdleTimeoutSec)
    BadMessage,     // 자르기 부품이 받은 바이트를 틀렸다고 함(규약 위반 · 최대 넘음)
    SendOverflow,   // 보낼 바이트가 보내기 링에 다 안 들어감
    AfterSend,      // 보낸 뒤 끊기(RequestDisconnectAfterSend) — 넣어 둔 보내기가 다 나간 뒤 끊음
};

// 접속을 받자마자 닫은 까닭 — 세션을 안 만들어 세션 번호가 없다
enum class RefuseReason : uint8_t
{
    SessionTableFull = 0,   // 세션 표에 빈자리가 없다(생성자 maxClients)
    UntaggedLimit,          // 구분 값 0 세션 상한(SetUntaggedSessionLimit)
    EventQueueBusy,         // 이벤트 큐 과부하(SetEventQueueAcceptLimit)
};

// 주소는 IPv4 · 주소는 네트워크 바이트 순서(in_addr.s_addr 그대로) · 포트는 호스트 바이트 순서
struct SinkConnected
{
    int64_t  sessionId = 0;
    int      tag       = 0;       // 접속을 받은 포트의 구분 값(AddListener) · ConnectTo 로 건 것이면 거기 준 값 · 생성자 포트는 0
    bool     outbound  = false;   // ConnectTo 로 건 접속이면 true
    uint32_t peerIp    = 0;       // 상대 주소
    uint16_t peerPort  = 0;
    uint32_t localIp   = 0;       // 이쪽 주소(getsockname — 여러 주소에 연 포트면 접속이 실제로 닿은 주소)
    uint16_t localPort = 0;
};

struct SinkReceived
{
    int64_t        sessionId = 0;
    int            tag       = 0;
    CSerialBuffer* msg       = nullptr;   // ref 하나를 넘긴다 — 받는 쪽이 SubRef 하거나 RequestSendMsg 로 넘겨 돌려준다
};

struct SinkDisconnected
{
    int64_t          sessionId = 0;
    int              tag       = 0;
    DisconnectReason reason    = DisconnectReason::IoError;
};

struct SinkRefused
{
    int          tag      = 0;   // 접속이 온 포트의 구분 값
    RefuseReason reason   = RefuseReason::SessionTableFull;
    uint32_t     peerIp   = 0;
    uint16_t     peerPort = 0;
};

class CRecvSink
{
public:
    virtual ~CRecvSink() = default;

    virtual void OnConnected(const SinkConnected& info) = 0;

    // 한 세션의 받음은 그 세션의 끊김보다 먼저 온다(큐 길과 같다)
    virtual void OnReceived(const SinkReceived& info) = 0;

    // 서버 종료 중(ShutdownServer)에는 부르지 않는다(큐 길과 같다)
    virtual void OnDisconnected(const SinkDisconnected& info) = 0;

    // 듣는 포트로 온 접속을 상한에 걸려 바로 닫았다 — 접속 받는 스레드가 부른다.
    //   기본은 아무것도 안 한다(코어 셈 _acceptFailed · _acceptRejectedByQueue 는 이것과 상관없이 는다).
    //   ConnectTo 로 건 접속의 거절은 ConnectTo 가 0 을 돌려 알리므로 여기로 안 온다.
    virtual void OnRefused(const SinkRefused& info) { (void)info; }
};
