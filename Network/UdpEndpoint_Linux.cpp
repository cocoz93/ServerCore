// CUdpEndpoint 의 리눅스 팔 — recvmmsg 로 묶어 받기 · Flush 에서 sendmmsg · epoll + eventfd 로 기다림 · 깨움
#ifndef _WIN32

#include "UdpArm.h"
#include <Common/ErrorLog.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <utility>
#include <vector>

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

    class CUdpArmEpoll final : public CUdpEndpoint::Arm
    {
    public:
        CUdpArmEpoll(const CUdpEndpoint::Config& c, CUdpEndpoint::Stats& st) : _cfg(c), _stats(st) {}

        ~CUdpArmEpoll() override
        {
            if (_sock >= 0)
                ::close(_sock);
            if (_wake >= 0)
                ::close(_wake);
            if (_epoll >= 0)
                ::close(_epoll);
        }

        bool Open()
        {
            _sock = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_UDP);
            if (_sock < 0)
            {
                SLOG_ERROR("[UDP] socket failed — errno: {}", errno);
                return false;
            }
            if (_cfg.socketRecvBytes > 0
                && ::setsockopt(_sock, SOL_SOCKET, SO_RCVBUF, &_cfg.socketRecvBytes, sizeof(int)) != 0)
                SLOG_WARN("[UDP] SO_RCVBUF {} failed — errno: {}", _cfg.socketRecvBytes, errno);
            if (_cfg.socketSendBytes > 0
                && ::setsockopt(_sock, SOL_SOCKET, SO_SNDBUF, &_cfg.socketSendBytes, sizeof(int)) != 0)
                SLOG_WARN("[UDP] SO_SNDBUF {} failed — errno: {}", _cfg.socketSendBytes, errno);

            sockaddr_in local{};
            local.sin_family = AF_INET;
            local.sin_addr.s_addr = htonl(_cfg.bindIp);
            local.sin_port = htons(_cfg.port);
            if (::bind(_sock, reinterpret_cast<const sockaddr*>(&local), sizeof(local)) != 0)
            {
                SLOG_ERROR("[UDP] bind port {} failed — errno: {}", _cfg.port, errno);
                return false;
            }
            sockaddr_in bound{};
            socklen_t boundLen = sizeof(bound);
            ::getsockname(_sock, reinterpret_cast<sockaddr*>(&bound), &boundLen);
            _port = ntohs(bound.sin_port);

            int rcv = 0, snd = 0;
            socklen_t len = sizeof(int);
            ::getsockopt(_sock, SOL_SOCKET, SO_RCVBUF, &rcv, &len);
            len = sizeof(int);
            ::getsockopt(_sock, SOL_SOCKET, SO_SNDBUF, &snd, &len);
            SLOG_INFO("[UDP] Open — arm=epoll port={} maxDatagram={} recvSlots={} sendSlots={} SO_RCVBUF={} SO_SNDBUF={}",
                      _port, _cfg.maxDatagram, _cfg.recvSlots, _cfg.sendSlots, rcv, snd);

            // epoll 하나에 소켓 · 깨움 eventfd 둘 — WaitHandle 하나로 둘 다 기다리게(수준 알림: 남은 것이 있으면 또 알린다)
            _epoll = ::epoll_create1(EPOLL_CLOEXEC);
            _wake = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (_epoll < 0 || _wake < 0)
            {
                SLOG_ERROR("[UDP] epoll/eventfd failed — errno: {}", errno);
                return false;
            }
            epoll_event ev{};
            ev.events = EPOLLIN;
            ev.data.u32 = TAG_SOCKET;
            if (::epoll_ctl(_epoll, EPOLL_CTL_ADD, _sock, &ev) != 0)
            {
                SLOG_ERROR("[UDP] epoll_ctl socket failed — errno: {}", errno);
                return false;
            }
            ev.data.u32 = TAG_WAKE;
            if (::epoll_ctl(_epoll, EPOLL_CTL_ADD, _wake, &ev) != 0)
            {
                SLOG_ERROR("[UDP] epoll_ctl eventfd failed — errno: {}", errno);
                return false;
            }

            // 받기 칸 = maxDatagram + 1 — 딱 하나 넘는 것까지 길이로 가린다(그보다 크면 MSG_TRUNC)
            _recvStride = AlignUp(static_cast<size_t>(_cfg.maxDatagram) + 1, 64);
            _recvBuf.resize(static_cast<size_t>(_cfg.recvSlots) * _recvStride);
            _recvNames.resize(_cfg.recvSlots);
            _recvIov.resize(_cfg.recvSlots);
            _recvHdr.resize(_cfg.recvSlots);
            _recvIds.resize(_cfg.recvSlots);

            _sendStride = AlignUp(_cfg.maxDatagram, 64);
            _sendBuf.resize(static_cast<size_t>(_cfg.sendSlots) * _sendStride);
            _sendNames.resize(_cfg.sendSlots);
            _sendIov.resize(_cfg.sendSlots);
            _sendHdr.resize(_cfg.sendSlots);
            return true;
        }

        uint16_t LocalPort() const override { return _port; }

        size_t Receive(UdpDatagram* out, size_t max) override
        {
            const size_t want = std::min<size_t>(max, _cfg.recvSlots);
            // 자리 → 칸 번호. 받은 것을 앞으로 모으며 칸 번호를 바꿔 끼운다 — 버린 칸은 뒤에 남아 다음 recvmmsg 가 다시 쓴다
            for (uint32_t i = 0; i < _cfg.recvSlots; ++i)
                _recvIds[i] = i;

            size_t got = 0;
            while (got < want)
            {
                const size_t k = want - got;
                for (size_t pos = got; pos < want; ++pos)
                {
                    const uint32_t slot = _recvIds[pos];
                    _recvIov[pos].iov_base = _recvBuf.data() + slot * _recvStride;
                    _recvIov[pos].iov_len = _cfg.maxDatagram + 1;
                    msghdr& h = _recvHdr[pos].msg_hdr;
                    std::memset(&h, 0, sizeof(h));
                    h.msg_name = &_recvNames[slot];
                    h.msg_namelen = sizeof(sockaddr_in);
                    h.msg_iov = &_recvIov[pos];
                    h.msg_iovlen = 1;
                    _recvHdr[pos].msg_len = 0;
                }
                const int r = ::recvmmsg(_sock, &_recvHdr[got], static_cast<unsigned>(k), MSG_DONTWAIT, nullptr);
                if (r < 0)
                {
                    const int e = errno;
                    if (e == EAGAIN || e == EWOULDBLOCK)
                        break;
                    if (e == EINTR)
                        continue;
                    _stats.recvFail.fetch_add(1, std::memory_order_relaxed);
                    _stats.lastRecvError.store(e, std::memory_order_relaxed);
                    break;
                }
                if (r == 0)
                    break;

                size_t write = got;
                for (size_t pos = got; pos < got + static_cast<size_t>(r); ++pos)
                {
                    const unsigned len = _recvHdr[pos].msg_len;
                    if ((_recvHdr[pos].msg_hdr.msg_flags & MSG_TRUNC) != 0 || len > _cfg.maxDatagram)
                    {
                        _stats.oversizeDropped.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                    std::swap(_recvIds[write], _recvIds[pos]);
                    const uint32_t slot = _recvIds[write];
                    out[write] = UdpDatagram{ FromSockaddr(_recvNames[slot]), _recvBuf.data() + slot * _recvStride, len };
                    ++write;
                }
                got = write;
                if (static_cast<size_t>(r) < k)
                    break;   // 다 비웠다
            }
            _stats.received.fetch_add(got, std::memory_order_relaxed);
            return got;
        }

        bool Send(const UdpAddr& to, const uint8_t* p, uint32_t n) override
        {
            if (n > _cfg.maxSendDatagram)
                return Fail(EMSGSIZE);
            if (n > _cfg.maxDatagram)
            {
                // 칸보다 큰 것은 드물다 — 쌓인 것을 먼저 내 차례를 지키고 바로 보낸다
                Flush();
                const sockaddr_in sa = ToSockaddr(to);
                if (::sendto(_sock, p, n, 0, reinterpret_cast<const sockaddr*>(&sa), sizeof(sa)) < 0)
                    return Fail(errno);
                _stats.sent.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            if (_queued == _cfg.sendSlots)
                Flush();   // 칸이 없다 — 먼저 내보낸다

            const uint32_t slot = _queued++;
            uint8_t* dst = _sendBuf.data() + slot * _sendStride;
            std::memcpy(dst, p, n);
            _sendNames[slot] = ToSockaddr(to);
            _sendIov[slot].iov_base = dst;
            _sendIov[slot].iov_len = n;
            msghdr& h = _sendHdr[slot].msg_hdr;
            std::memset(&h, 0, sizeof(h));
            h.msg_name = &_sendNames[slot];
            h.msg_namelen = sizeof(sockaddr_in);
            h.msg_iov = &_sendIov[slot];
            h.msg_iovlen = 1;
            _stats.sent.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        void Flush() override
        {
            if (_queued == 0)
                return;
            uint32_t done = 0;
            while (done < _queued)
            {
                const int r = ::sendmmsg(_sock, &_sendHdr[done], _queued - done, 0);
                if (r < 0)
                {
                    const int e = errno;
                    if (e == EINTR)
                        continue;
                    Fail(e);   // 맨 앞 하나가 안 나갔다(EAGAIN 이면 커널 보내기 버퍼가 꽉 참) — 버리고 다음 것부터
                    ++done;
                    continue;
                }
                done += static_cast<uint32_t>(r);
            }
            _queued = 0;
            _stats.flushes.fetch_add(1, std::memory_order_relaxed);
        }

        WaitResult Wait(uint32_t ms) override
        {
            epoll_event evs[2];
            const int timeout = ms > static_cast<uint32_t>(INT_MAX) ? INT_MAX : static_cast<int>(ms);
            const int n = ::epoll_wait(_epoll, evs, 2, timeout);
            if (n <= 0)
                return WaitResult::Timeout;   // 시간 끝 · 시그널(EINTR)
            bool woken = false;
            for (int i = 0; i < n; ++i)
            {
                if (evs[i].data.u32 == TAG_WAKE)
                {
                    uint64_t v = 0;
                    while (::read(_wake, &v, sizeof(v)) == static_cast<ssize_t>(sizeof(v)))
                        ;   // 깨움을 거둔다(여러 번 불렸어도 한 번)
                    woken = true;
                }
            }
            return woken ? WaitResult::Woken : WaitResult::Ready;
        }

        CUdpEndpoint::NativeWait WaitHandle() const override { return _epoll; }

        void Wake() override
        {
            const uint64_t one = 1;
            const ssize_t w = ::write(_wake, &one, sizeof(one));
            (void)w;   // 꽉 찼으면(EAGAIN) 이미 깨움이 서 있다
        }

    private:
        static constexpr uint32_t TAG_SOCKET = 0;
        static constexpr uint32_t TAG_WAKE = 1;

        bool Fail(int e)
        {
            _stats.sendFail.fetch_add(1, std::memory_order_relaxed);
            _stats.lastSendError.store(e, std::memory_order_relaxed);
            return false;
        }

        CUdpEndpoint::Config _cfg;
        CUdpEndpoint::Stats& _stats;
        int _sock = -1;
        int _epoll = -1;
        int _wake = -1;
        uint16_t _port = 0;

        size_t _recvStride = 0;
        std::vector<uint8_t> _recvBuf;
        std::vector<sockaddr_in> _recvNames;   // 칸마다
        std::vector<iovec> _recvIov;           // 자리마다
        std::vector<mmsghdr> _recvHdr;         // 자리마다
        std::vector<uint32_t> _recvIds;        // 자리 → 칸

        size_t _sendStride = 0;
        std::vector<uint8_t> _sendBuf;
        std::vector<sockaddr_in> _sendNames;
        std::vector<iovec> _sendIov;
        std::vector<mmsghdr> _sendHdr;
        uint32_t _queued = 0;                  // 쌓고 아직 안 낸 보내기
    };
}

std::unique_ptr<CUdpEndpoint::Arm> CreateUdpArm(const CUdpEndpoint::Config& config, CUdpEndpoint::Stats& stats)
{
    if (config.rio)
    {
        SLOG_ERROR("[UDP] Open refused — rio arm is Windows only");
        return nullptr;
    }
    auto arm = std::make_unique<CUdpArmEpoll>(config, stats);
    if (!arm->Open())
        return nullptr;
    return arm;
}

#endif // !_WIN32
