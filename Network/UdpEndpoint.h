#pragma once

// ==========================================================================
// CUdpEndpoint — UDP 소켓 하나를 다루는 부품 (세션 없음)
//
//  「데이터그램 + 주소」만 다룬다. 접속 절차 · 순번 · 다시 보내기는 쓰는 쪽 몫이다.
//  CIOCPServer 뼈대에 넣지 않은 까닭 — 그 뼈대의 세션은 TCP 접속 단위다(링 버퍼 · 입출력 수 · 끊김 깃발).
//
//  팔 셋 — 실행 때 Config::rio 로 고른다(한 실행 파일로 두 팔을 나란히 잴 수 있게):
//    Windows 보통  recvfrom 을 WSAEWOULDBLOCK 까지 · Send 에서 바로 sendto · Flush 는 할 일 없음
//    Windows RIO   받기를 칸 수만큼 미리 걸어 두고 거둠 · 보내기는 쌓았다가 Flush 에서 한 번에
//    리눅스        recvmmsg 로 묶어 받기 · Flush 에서 sendmmsg (rio = true 면 Open 실패)
//  Windows 에서 보내기를 묶는 팔은 RIO 뿐이다 — 보통 팔은 데이터그램마다 커널 호출이 그대로다.
//
//  스레드:
//    Open · Close · Receive · Send · Flush · Wait 는 주인 스레드 하나만 부른다
//      (RIO 「한 요청 큐는 한 스레드」 규칙 — IOCPServer.h 의 RIO 워커와 같다).
//    Wake · GetStats 는 아무 스레드나 부른다(Open 뒤 · Close 앞).
//
//  기다리는 법:
//    Wait(ms) 가 Timeout 이 아니면 Receive 를 0 이 될 때까지 부른다 — 깨움과 받을 것이 한 번에 올 수 있다.
//    다른 것과 같이 기다리려면 WaitHandle()(Windows 이벤트 · 리눅스 epoll fd)을 쓰고,
//    그것이 깨면 Wait(0) 으로 깨움을 거둔 뒤 Receive 를 비운다. 깸은 헛것일 수 있다(Receive 가 0).
// ==========================================================================

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

// IPv4 주소 — 호스트 바이트 차례(127.0.0.1 = 0x7F000001)
struct UdpAddr
{
    uint32_t ip   = 0;
    uint16_t port = 0;

    // 주소 하나를 숫자 하나로 — 지도 열쇠용
    uint64_t Key() const { return (static_cast<uint64_t>(ip) << 16) | port; }
};

// 받은 것 하나 — data 는 다음 Receive 까지 유효하다
struct UdpDatagram
{
    UdpAddr        from;
    const uint8_t* data = nullptr;
    uint32_t       size = 0;
};

class CUdpEndpoint
{
public:
#ifdef _WIN32
    using NativeWait = void*;   // HANDLE — 이벤트 하나(받을 것 · 깨움 둘 다 이것을 세운다)
#else
    using NativeWait = int;     // epoll fd(소켓 · 깨움 eventfd 를 함께 본다)
#endif

    // 받을 것 · 깨움 · 시간 끝. 깨움이 우선이다(둘이 함께 와도 Woken)
    enum class WaitResult { Timeout, Ready, Woken };

    // UDP 몸통 한도(IPv4) = 65,535 - IP 머리 20 - UDP 머리 8
    static constexpr uint32_t MAX_UDP_PAYLOAD = 65507;

    struct Config
    {
        uint16_t port   = 0;      // 0 = OS 가 고름(LocalPort 로 읽는다)
        uint32_t bindIp = 0;      // 0 = 모든 주소 · 호스트 바이트 차례

        // 받기 한 칸 — 이보다 큰 것은 버리고 센다(oversizeDropped). 딱 이 크기는 받는다
        uint32_t maxDatagram = 2048;
        // 보내기 한 건의 상한 — 넘으면 Send 가 거절하고 센다. maxDatagram 이상 · MAX_UDP_PAYLOAD 이하
        uint32_t maxSendDatagram = MAX_UDP_PAYLOAD;

        // 칸 수 — 0 이면 Open 실패(쓰는 쪽이 정한다: 한 번 깸에 받는 수 · 한 번 Flush 에 내는 수의 최대).
        //   받기: 한 Receive 가 돌려주는 상한이기도 하다(세 팔 같음). 보내기: 이만큼 쌓이면 Send 가 먼저 내보낸다
        //   RIO 팔은 받기를 이만큼만 걸어 두므로, Receive 사이에 이보다 많이 몰리면 나머지는 커널이 버린다
        //   (이 부품의 셈엔 안 잡힌다). 보통 · 리눅스 팔은 커널 받기 버퍼(socketRecvBytes)에 쌓였다가 다음 Receive 로 온다
        uint32_t recvSlots = 0;
        uint32_t sendSlots = 0;
        // RIO 팔에서 maxDatagram 보다 큰 보내기에 쓰는 칸(크기 maxSendDatagram) 수.
        //   드물게 오는 큰 것만 받친다 — 다 차면 끝난 것을 거둘 때까지 Send 가 기다린다
        uint32_t bigSendSlots = 4;

        int  socketRecvBytes = 0;   // SO_RCVBUF — 0 = OS 기본
        int  socketSendBytes = 0;   // SO_SNDBUF — 0 = OS 기본
        bool rio = false;           // Windows 만 · RIO 함수 표를 못 얻으면 Open 실패(말없이 보통 팔로 바꾸지 않음)
    };

    // 아무 스레드나 읽는다(relaxed). Open 이 0 으로 되돌린다
    struct Stats
    {
        std::atomic<uint64_t> received{0};          // Receive 가 돌려준 데이터그램
        std::atomic<uint64_t> sent{0};              // Send 가 받아 커널로 넘겼거나 쌓은 데이터그램
        std::atomic<uint64_t> oversizeDropped{0};   // maxDatagram 보다 커서 버린 받기
        std::atomic<uint64_t> recvFail{0};          // 받기 오류(닫힌 포트 알림 등) — 다음 것을 계속 받는다
        std::atomic<uint64_t> sendFail{0};          // 보내기 실패(바로 · 끝난 뒤 알림 · 상한 넘음)
        std::atomic<int>      lastRecvError{0};
        std::atomic<int>      lastSendError{0};     // Windows 10035 / 리눅스 EAGAIN 이면 커널 보내기 버퍼가 꽉 참
        std::atomic<uint64_t> wakes{0};             // Wake 부른 수
        std::atomic<uint64_t> flushes{0};           // 쌓인 것을 실제로 내보낸 Flush 수(보통 팔은 0)
    };

    class Arm;   // 팔 셋의 바탕 — 구현 파일에만(UdpArm.h)

    CUdpEndpoint();
    ~CUdpEndpoint();
    CUdpEndpoint(const CUdpEndpoint&) = delete;
    CUdpEndpoint& operator=(const CUdpEndpoint&) = delete;

    bool Open(const Config& config);   // 이미 열렸거나 설정이 틀리면 false(까닭은 로그)
    void Close();
    bool IsOpen() const { return _arm != nullptr; }
    uint16_t LocalPort() const;        // 묶인 포트(Config::port = 0 이었을 때 OS 가 고른 것)

    // ↓ 주인 스레드만
    size_t Receive(UdpDatagram* out, size_t max);                // 막지 않는다 · 있는 만큼(최대 recvSlots)
    bool   Send(const UdpAddr& to, const uint8_t* p, uint32_t n); // 쌓는다 · 칸이 없으면 먼저 내보낸다
    void   Flush();                                              // 쌓인 것을 한 번에
    WaitResult Wait(uint32_t ms);
    NativeWait WaitHandle() const;

    // ↓ 아무 스레드나
    void Wake();
    const Stats& GetStats() const { return _stats; }

private:
    std::unique_ptr<Arm> _arm;
    Stats _stats;
};
