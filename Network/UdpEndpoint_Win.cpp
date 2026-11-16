// CUdpEndpoint 의 Windows 팔 둘 — 보통(recvfrom · sendto) · RIO(받기를 미리 걸어 두고 거둠 · 보내기를 쌓아 한 번에)
#ifdef _WIN32

#include <WinSock2.h>   // Windows.h 보다 먼저(구버전 winsock.h 가 딸려오면 sockaddr 이 재정의된다)
#include <WS2tcpip.h>
#include <MSWSock.h>

#include "RioApi.h"
#include "UdpArm.h"
#include <Common/ErrorLog.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#pragma comment(lib, "ws2_32.lib")

namespace
{
    using WaitResult = CUdpEndpoint::WaitResult;

    sockaddr_in ToSockaddr(const UdpAddr& a)
    {
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_addr.s_addr = htonl(a.ip);
        sa.sin_port = htons(a.port);
        return sa;
    }

    UdpAddr FromSockaddr(const sockaddr_in& sa)
    {
        return UdpAddr{ ntohl(sa.sin_addr.s_addr), ntohs(sa.sin_port) };
    }

    size_t AlignUp(size_t n, size_t a) { return (n + a - 1) / a * a; }

    // 두 팔이 같이 쓰는 소켓 준비 — 커널 버퍼 크기 · 묶기 · 묶인 포트 · 기동 로그(커널 버퍼 실제 값을 남긴다)
    bool SetupSocket(SOCKET s, const CUdpEndpoint::Config& c, const char* armName, uint16_t& boundPort)
    {
        if (c.socketRecvBytes > 0
            && setsockopt(s, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&c.socketRecvBytes), sizeof(int)) == SOCKET_ERROR)
            SLOG_WARN("[UDP] SO_RCVBUF {} failed — WSAError: {}", c.socketRecvBytes, WSAGetLastError());
        if (c.socketSendBytes > 0
            && setsockopt(s, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&c.socketSendBytes), sizeof(int)) == SOCKET_ERROR)
            SLOG_WARN("[UDP] SO_SNDBUF {} failed — WSAError: {}", c.socketSendBytes, WSAGetLastError());

        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(c.bindIp);
        local.sin_port = htons(c.port);
        if (bind(s, reinterpret_cast<const sockaddr*>(&local), sizeof(local)) == SOCKET_ERROR)
        {
            SLOG_ERROR("[UDP] bind port {} failed — WSAError: {}", c.port, WSAGetLastError());
            return false;
        }

        sockaddr_in bound{};
        int boundLen = sizeof(bound);
        getsockname(s, reinterpret_cast<sockaddr*>(&bound), &boundLen);
        boundPort = ntohs(bound.sin_port);

        int rcv = 0, snd = 0;
        int len = sizeof(int);
        getsockopt(s, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&rcv), &len);
        len = sizeof(int);
        getsockopt(s, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&snd), &len);
        SLOG_INFO("[UDP] Open — arm={} port={} maxDatagram={} recvSlots={} sendSlots={} SO_RCVBUF={} SO_SNDBUF={}",
                  armName, boundPort, c.maxDatagram, c.recvSlots, c.sendSlots, rcv, snd);
        return true;
    }

    // 받을 것 · 깨움이 같이 세우는 자동 이벤트 하나를 기다린다(두 팔 같음).
    //   깨움 깃발을 먼저 본다 — 바깥에서 WaitHandle 로 신호를 먹은 뒤 부르는 Wait(0) 도 깨움을 놓치지 않게.
    WaitResult WaitEvent(HANDLE ev, std::atomic<bool>& wakePending, uint32_t ms)
    {
        if (wakePending.exchange(false))
        {
            WaitForSingleObject(ev, 0);   // 깨움이 세운 신호를 거둔다(받을 것의 신호였어도 쓰는 쪽이 Receive 를 비운다)
            return WaitResult::Woken;
        }
        if (WaitForSingleObject(ev, ms) != WAIT_OBJECT_0)
            return WaitResult::Timeout;
        return wakePending.exchange(false) ? WaitResult::Woken : WaitResult::Ready;
    }

    // ───────────────────────── 보통 팔 ─────────────────────────
    class CUdpArmWinsock final : public CUdpEndpoint::Arm
    {
    public:
        CUdpArmWinsock(const CUdpEndpoint::Config& c, CUdpEndpoint::Stats& st) : _cfg(c), _stats(st) {}

        ~CUdpArmWinsock() override
        {
            if (_sock != INVALID_SOCKET)
                closesocket(_sock);
            if (_event != nullptr)
                CloseHandle(_event);
            if (_wsa)
                WSACleanup();
        }

        bool Open()
        {
            WSADATA wsaData;
            if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
            {
                SLOG_ERROR("[UDP] WSAStartup failed");
                return false;
            }
            _wsa = true;

            _sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
            if (_sock == INVALID_SOCKET)
            {
                SLOG_ERROR("[UDP] socket failed — WSAError: {}", WSAGetLastError());
                return false;
            }
            if (!SetupSocket(_sock, _cfg, "winsock", _port))
                return false;

            // 자동으로 되돌아가는 이벤트 하나 — 소켓 읽기와 깨움이 같이 세운다(WaitHandle 하나로 둘 다 기다리게).
            //   WSAEventSelect 가 소켓을 막지 않는 모드로 바꾼다(받을 것을 WSAEWOULDBLOCK 까지 비운다)
            _event = CreateEventW(nullptr, FALSE, FALSE, nullptr);
            if (_event == nullptr || WSAEventSelect(_sock, _event, FD_READ) == SOCKET_ERROR)
            {
                SLOG_ERROR("[UDP] WSAEventSelect failed — WSAError: {}", WSAGetLastError());
                return false;
            }

            // 칸 하나 = maxDatagram + 1 — 딱 하나 넘는 것까지 길이로 가린다(그보다 크면 WSAEMSGSIZE)
            _slotSize = _cfg.maxDatagram + 1;
            _slotStride = AlignUp(_slotSize, 64);
            _recvBuf.resize(static_cast<size_t>(_cfg.recvSlots) * _slotStride);
            return true;
        }

        uint16_t LocalPort() const override { return _port; }

        size_t Receive(UdpDatagram* out, size_t max) override
        {
            const size_t want = std::min<size_t>(max, _cfg.recvSlots);
            size_t got = 0;
            while (got < want)
            {
                uint8_t* buf = _recvBuf.data() + got * _slotStride;
                sockaddr_in from{};
                int fromLen = sizeof(from);
                const int n = recvfrom(_sock, reinterpret_cast<char*>(buf), static_cast<int>(_slotSize), 0,
                                       reinterpret_cast<sockaddr*>(&from), &fromLen);
                if (n == SOCKET_ERROR)
                {
                    const int e = WSAGetLastError();
                    if (e == WSAEWOULDBLOCK)
                        break;
                    if (e == WSAEMSGSIZE)   // 칸보다 크다 — 잘린 채 왔다
                    {
                        _stats.oversizeDropped.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                    _stats.recvFail.fetch_add(1, std::memory_order_relaxed);
                    _stats.lastRecvError.store(e, std::memory_order_relaxed);
                    if (e == WSAECONNRESET)   // 앞서 보낸 곳의 닫힌 포트 알림 — 다음 것을 계속 받는다
                        continue;
                    break;
                }
                if (static_cast<uint32_t>(n) > _cfg.maxDatagram)
                {
                    _stats.oversizeDropped.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                out[got] = UdpDatagram{ FromSockaddr(from), buf, static_cast<uint32_t>(n) };
                ++got;
            }
            _stats.received.fetch_add(got, std::memory_order_relaxed);
            return got;
        }

        bool Send(const UdpAddr& to, const uint8_t* p, uint32_t n) override
        {
            if (n > _cfg.maxSendDatagram)
                return Fail(WSAEMSGSIZE);
            const sockaddr_in sa = ToSockaddr(to);
            if (sendto(_sock, reinterpret_cast<const char*>(p), static_cast<int>(n), 0,
                       reinterpret_cast<const sockaddr*>(&sa), sizeof(sa)) == SOCKET_ERROR)
                return Fail(WSAGetLastError());
            _stats.sent.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        void Flush() override {}   // 보낼 때마다 바로 냈다

        WaitResult Wait(uint32_t ms) override { return WaitEvent(_event, _wakePending, ms); }
        CUdpEndpoint::NativeWait WaitHandle() const override { return _event; }

        void Wake() override
        {
            _wakePending.store(true);
            SetEvent(_event);
        }

    private:
        bool Fail(int e)
        {
            _stats.sendFail.fetch_add(1, std::memory_order_relaxed);
            _stats.lastSendError.store(e, std::memory_order_relaxed);
            return false;
        }

        CUdpEndpoint::Config _cfg;
        CUdpEndpoint::Stats& _stats;
        bool _wsa = false;
        SOCKET _sock = INVALID_SOCKET;
        HANDLE _event = nullptr;
        std::atomic<bool> _wakePending{ false };
        uint16_t _port = 0;
        size_t _slotSize = 0;
        size_t _slotStride = 0;
        std::vector<uint8_t> _recvBuf;
    };

    // ───────────────────────── RIO 팔 ─────────────────────────
    //   등록 메모리 한 덩이(CRioSlab)를 칸으로 나눈다:
    //     [받기 칸 × recvSlots][보내기 칸 × sendSlots][큰 칸 × bigSendSlots][주소 칸 × 셋의 합]
    //   받기는 칸 수만큼 미리 걸어 두고, 거둔 칸은 「다음 Receive 까지 유효」 약속대로 다음 Receive 에서 다시 건다.
    //   걸기 · 보내기는 RIO_MSG_DEFER 로 쌓고 마지막에 한 번 RIO_MSG_COMMIT_ONLY — 커널 호출을 묶는 것이 이 팔의 까닭이다.
    class CUdpArmRio final : public CUdpEndpoint::Arm
    {
    public:
        CUdpArmRio(const CUdpEndpoint::Config& c, CUdpEndpoint::Stats& st) : _cfg(c), _stats(st) {}

        ~CUdpArmRio() override
        {
            if (_sock != INVALID_SOCKET)
                closesocket(_sock);   // 걸린 요청은 오류 끝으로 CQ 에 온다 — CQ 를 닫기 전에
            if (_recvCq != RIO_INVALID_CQ)
                CRioApi::Rio().RIOCloseCompletionQueue(_recvCq);
            if (_sendCq != RIO_INVALID_CQ)
                CRioApi::Rio().RIOCloseCompletionQueue(_sendCq);
            _slab.Release();   // 등록 해제 → VirtualFree (WSACleanup 전)
            if (_event != nullptr)
                CloseHandle(_event);
            if (_wsa)
                WSACleanup();
        }

        bool Open()
        {
            WSADATA wsaData;
            if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
            {
                SLOG_ERROR("[UDP] WSAStartup failed");
                return false;
            }
            _wsa = true;

            _sock = WSASocketW(AF_INET, SOCK_DGRAM, IPPROTO_UDP, nullptr, 0, WSA_FLAG_REGISTERED_IO);
            if (_sock == INVALID_SOCKET)
            {
                SLOG_ERROR("[UDP] RIO socket failed — WSAError: {}", WSAGetLastError());
                return false;
            }
            if (!CRioApi::Load(_sock))
            {
                SLOG_ERROR("[UDP] RIO function table unavailable — WSAError: {}", WSAGetLastError());
                return false;
            }
            if (!SetupSocket(_sock, _cfg, "rio", _port))
                return false;

            // 칸 자리 — 64바이트 단위로 맞춘다(칸끼리 캐시 줄을 안 나눠 쓰게)
            const size_t sendTotal = static_cast<size_t>(_cfg.sendSlots) + _cfg.bigSendSlots;
            _recvStride = AlignUp(static_cast<size_t>(_cfg.maxDatagram) + 1, 64);   // + 1: 딱 하나 넘는 것까지 길이로 가린다
            _sendStride = AlignUp(_cfg.maxDatagram, 64);
            _bigStride  = AlignUp(_cfg.maxSendDatagram, 64);
            _addrStride = AlignUp(sizeof(SOCKADDR_INET), 32);
            _sendBase = static_cast<size_t>(_cfg.recvSlots) * _recvStride;
            _bigBase  = _sendBase + static_cast<size_t>(_cfg.sendSlots) * _sendStride;
            _addrBase = _bigBase + static_cast<size_t>(_cfg.bigSendSlots) * _bigStride;
            const size_t total = _addrBase + (_cfg.recvSlots + sendTotal) * _addrStride;
            if (!_slab.Init(total))
            {
                SLOG_ERROR("[UDP] RIO slab {} bytes failed — error: {}", total, GetLastError());
                return false;
            }

            // 받기 끝 알림 = 자동 이벤트(깨움도 같이 세운다). NotifyReset = FALSE — RIONotify 가 이벤트를 되돌리면
            //   그 사이에 온 깨움이 지워진다(바깥에서 WaitHandle 로 기다리는 쪽이 놓친다)
            _event = CreateEventW(nullptr, FALSE, FALSE, nullptr);
            if (_event == nullptr)
            {
                SLOG_ERROR("[UDP] CreateEvent failed — error: {}", GetLastError());
                return false;
            }
            RIO_NOTIFICATION_COMPLETION nc = {};
            nc.Type = RIO_EVENT_COMPLETION;
            nc.Event.EventHandle = _event;
            nc.Event.NotifyReset = FALSE;
            _recvCq = CRioApi::Rio().RIOCreateCompletionQueue(_cfg.recvSlots, &nc);
            _sendCq = CRioApi::Rio().RIOCreateCompletionQueue(static_cast<DWORD>(sendTotal), nullptr);   // 보내기 끝은 알림 없이 거둔다
            if (_recvCq == RIO_INVALID_CQ || _sendCq == RIO_INVALID_CQ)
            {
                SLOG_ERROR("[UDP] RIOCreateCompletionQueue failed — WSAError: {}", WSAGetLastError());
                return false;
            }
            _rq = CRioApi::Rio().RIOCreateRequestQueue(_sock, _cfg.recvSlots, 1, static_cast<ULONG>(sendTotal), 1,
                                                       _recvCq, _sendCq, nullptr);
            if (_rq == RIO_INVALID_RQ)
            {
                SLOG_ERROR("[UDP] RIOCreateRequestQueue failed — WSAError: {}", WSAGetLastError());
                return false;
            }

            _results.resize(std::max<size_t>(_cfg.recvSlots, sendTotal));
            _lent.reserve(_cfg.recvSlots);
            _freeSend.reserve(_cfg.sendSlots);
            for (uint32_t i = _cfg.sendSlots; i > 0; --i)
                _freeSend.push_back(i - 1);
            _freeBig.reserve(_cfg.bigSendSlots);
            for (uint32_t i = _cfg.bigSendSlots; i > 0; --i)
                _freeBig.push_back(i - 1);

            for (uint32_t slot = 0; slot < _cfg.recvSlots; ++slot)
            {
                if (!PostRecv(slot, RIO_MSG_DEFER))
                    return false;
            }
            CommitRecv();
            Notify();
            return true;
        }

        uint16_t LocalPort() const override { return _port; }

        size_t Receive(UdpDatagram* out, size_t max) override
        {
            // 지난번에 빌려준 칸을 다시 건다 — 그 데이터는 「다음 Receive 까지」만 유효하다고 약속했다
            bool posted = false;
            for (uint32_t slot : _lent)
                posted |= PostRecv(slot, RIO_MSG_DEFER);
            _lent.clear();

            const size_t want = std::min<size_t>(max, _cfg.recvSlots);
            size_t got = 0;
            bool drained = false;
            while (got < want)
            {
                const ULONG n = CRioApi::Rio().RIODequeueCompletion(_recvCq, _results.data(), static_cast<ULONG>(want - got));
                if (n == RIO_CORRUPT_CQ)
                {
                    SLOG_ERROR("[UDP] RIO receive CQ corrupt");
                    break;
                }
                if (n == 0)
                {
                    drained = true;
                    break;
                }
                for (ULONG i = 0; i < n; ++i)
                {
                    const RIORESULT& r = _results[i];
                    const uint32_t slot = static_cast<uint32_t>(r.RequestContext);
                    if (r.Status != 0 || r.BytesTransferred > _cfg.maxDatagram)
                    {
                        if (r.Status == 0 || r.Status == WSAEMSGSIZE)   // 칸보다 크다
                            _stats.oversizeDropped.fetch_add(1, std::memory_order_relaxed);
                        else
                        {
                            _stats.recvFail.fetch_add(1, std::memory_order_relaxed);
                            _stats.lastRecvError.store(r.Status, std::memory_order_relaxed);
                        }
                        posted |= PostRecv(slot, RIO_MSG_DEFER);   // 버린 칸은 바로 다시 건다
                        continue;
                    }
                    out[got++] = UdpDatagram{ AddrOf(slot), reinterpret_cast<const uint8_t*>(_slab.Base() + RecvOffset(slot)),
                                              r.BytesTransferred };
                    _lent.push_back(slot);
                }
            }
            if (posted)
                CommitRecv();
            _stats.received.fetch_add(got, std::memory_order_relaxed);
            if (drained)
                Notify();   // 다 비웠다 — 다음 끝에 이벤트가 서게 건다(이미 걸렸으면 그대로)
            return got;
        }

        bool Send(const UdpAddr& to, const uint8_t* p, uint32_t n) override
        {
            if (n > _cfg.maxSendDatagram)
                return Fail(WSAEMSGSIZE);
            const bool big = n > _cfg.maxDatagram;
            std::vector<uint32_t>& freeList = big ? _freeBig : _freeSend;
            if (freeList.empty())
            {
                Flush();   // 쌓인 것을 내고 끝난 것을 거둔다
                // 끝남은 커널이 내자마자 와서 보통 바로 풀린다 — SLOT_WAIT 넘게 안 풀리면 커널 쪽이 막힌 것으로 보고 실패로 센다
                const auto until = std::chrono::steady_clock::now() + SLOT_WAIT;
                while (freeList.empty() && std::chrono::steady_clock::now() < until)
                {
                    std::this_thread::yield();
                    Reap();
                }
                if (freeList.empty())
                    return Fail(WSAENOBUFS);
            }
            const uint32_t idx = freeList.back();
            freeList.pop_back();

            const size_t dataOffset = big ? _bigBase + idx * _bigStride : _sendBase + idx * _sendStride;
            const uint32_t addrSlot = big ? _cfg.recvSlots + _cfg.sendSlots + idx : _cfg.recvSlots + idx;
            std::memcpy(_slab.Base() + dataOffset, p, n);
            auto* addr = reinterpret_cast<SOCKADDR_INET*>(_slab.Base() + AddrOffset(addrSlot));
            std::memset(addr, 0, sizeof(*addr));
            addr->Ipv4 = ToSockaddr(to);

            RIO_BUF dataBuf = { _slab.BufferId(), static_cast<ULONG>(dataOffset), n };
            RIO_BUF addrBuf = { _slab.BufferId(), static_cast<ULONG>(AddrOffset(addrSlot)), sizeof(SOCKADDR_INET) };
            const uint32_t ctx = (big ? BIG_FLAG : 0u) | idx;
            if (!CRioApi::Rio().RIOSendEx(_rq, &dataBuf, 1, nullptr, &addrBuf, nullptr, nullptr, RIO_MSG_DEFER,
                                          reinterpret_cast<void*>(static_cast<uintptr_t>(ctx))))
            {
                freeList.push_back(idx);
                return Fail(WSAGetLastError());
            }
            ++_pendingCommit;
            _stats.sent.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        void Flush() override
        {
            if (_pendingCommit > 0)
            {
                if (!CRioApi::Rio().RIOSend(_rq, nullptr, 0, RIO_MSG_COMMIT_ONLY, nullptr))
                    Fail(WSAGetLastError());
                _pendingCommit = 0;
                _stats.flushes.fetch_add(1, std::memory_order_relaxed);
            }
            Reap();
        }

        WaitResult Wait(uint32_t ms) override
        {
            Notify();
            return WaitEvent(_event, _wakePending, ms);
        }

        CUdpEndpoint::NativeWait WaitHandle() const override { return _event; }

        void Wake() override
        {
            _wakePending.store(true);
            SetEvent(_event);
        }

    private:
        static constexpr uint32_t BIG_FLAG = 0x80000000u;   // 보내기 끝의 칸 번호에 「큰 칸」 표시
        static constexpr std::chrono::milliseconds SLOT_WAIT{ 100 };

        size_t RecvOffset(uint32_t slot) const { return static_cast<size_t>(slot) * _recvStride; }
        size_t AddrOffset(uint32_t addrSlot) const { return _addrBase + static_cast<size_t>(addrSlot) * _addrStride; }

        UdpAddr AddrOf(uint32_t slot) const
        {
            const auto* a = reinterpret_cast<const SOCKADDR_INET*>(_slab.Base() + AddrOffset(slot));
            return a->si_family == AF_INET ? FromSockaddr(a->Ipv4) : UdpAddr{};
        }

        bool PostRecv(uint32_t slot, DWORD flags)
        {
            RIO_BUF dataBuf = { _slab.BufferId(), static_cast<ULONG>(RecvOffset(slot)), _cfg.maxDatagram + 1 };
            RIO_BUF addrBuf = { _slab.BufferId(), static_cast<ULONG>(AddrOffset(slot)), sizeof(SOCKADDR_INET) };
            if (!CRioApi::Rio().RIOReceiveEx(_rq, &dataBuf, 1, nullptr, &addrBuf, nullptr, nullptr, flags,
                                             reinterpret_cast<void*>(static_cast<uintptr_t>(slot))))
            {
                const int e = WSAGetLastError();
                _stats.recvFail.fetch_add(1, std::memory_order_relaxed);
                _stats.lastRecvError.store(e, std::memory_order_relaxed);
                SLOG_ERROR("[UDP] RIOReceiveEx failed — slot {} WSAError: {}", slot, e);
                return false;
            }
            return true;
        }

        void CommitRecv()
        {
            if (!CRioApi::Rio().RIOReceive(_rq, nullptr, 0, RIO_MSG_COMMIT_ONLY, nullptr))
                SLOG_ERROR("[UDP] RIO receive commit failed — WSAError: {}", WSAGetLastError());
        }

        // 받기 끝 알림을 건다 — 이미 걸렸으면 WSAEALREADY(그대로 둔다). CQ 에 이미 있으면 바로 이벤트가 선다
        void Notify()
        {
            const INT r = CRioApi::Rio().RIONotify(_recvCq);
            if (r != ERROR_SUCCESS && r != WSAEALREADY)
                SLOG_ERROR("[UDP] RIONotify failed — {}", r);
        }

        // 끝난 보내기를 거둬 칸을 돌려놓는다 — 실패 끝은 센다
        void Reap()
        {
            for (;;)
            {
                const ULONG n = CRioApi::Rio().RIODequeueCompletion(_sendCq, _results.data(), static_cast<ULONG>(_results.size()));
                if (n == 0)
                    return;
                if (n == RIO_CORRUPT_CQ)
                {
                    SLOG_ERROR("[UDP] RIO send CQ corrupt");
                    return;
                }
                for (ULONG i = 0; i < n; ++i)
                {
                    const uint32_t ctx = static_cast<uint32_t>(_results[i].RequestContext);
                    ((ctx & BIG_FLAG) ? _freeBig : _freeSend).push_back(ctx & ~BIG_FLAG);
                    if (_results[i].Status != 0)
                        Fail(_results[i].Status);
                }
            }
        }

        bool Fail(int e)
        {
            _stats.sendFail.fetch_add(1, std::memory_order_relaxed);
            _stats.lastSendError.store(e, std::memory_order_relaxed);
            return false;
        }

        CUdpEndpoint::Config _cfg;
        CUdpEndpoint::Stats& _stats;
        bool _wsa = false;
        SOCKET _sock = INVALID_SOCKET;
        HANDLE _event = nullptr;
        std::atomic<bool> _wakePending{ false };
        uint16_t _port = 0;

        CRioSlab _slab;
        RIO_CQ _recvCq = RIO_INVALID_CQ;
        RIO_CQ _sendCq = RIO_INVALID_CQ;
        RIO_RQ _rq = RIO_INVALID_RQ;
        size_t _recvStride = 0, _sendStride = 0, _bigStride = 0, _addrStride = 0;
        size_t _sendBase = 0, _bigBase = 0, _addrBase = 0;

        std::vector<RIORESULT> _results;
        std::vector<uint32_t> _lent;       // 지난 Receive 가 빌려준 받기 칸
        std::vector<uint32_t> _freeSend;   // 빈 보내기 칸
        std::vector<uint32_t> _freeBig;    // 빈 큰 칸
        uint32_t _pendingCommit = 0;       // 쌓고 아직 안 낸 보내기
    };
}

std::unique_ptr<CUdpEndpoint::Arm> CreateUdpArm(const CUdpEndpoint::Config& config, CUdpEndpoint::Stats& stats)
{
    if (config.rio)
    {
        auto arm = std::make_unique<CUdpArmRio>(config, stats);
        if (!arm->Open())
            return nullptr;
        return arm;
    }
    auto arm = std::make_unique<CUdpArmWinsock>(config, stats);
    if (!arm->Open())
        return nullptr;
    return arm;
}

#endif // _WIN32
