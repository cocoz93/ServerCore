#include "UdpArm.h"
#include <Common/ErrorLog.h>

CUdpEndpoint::CUdpEndpoint() = default;

CUdpEndpoint::~CUdpEndpoint()
{
    Close();
}

bool CUdpEndpoint::Open(const Config& config)
{
    if (_arm)
    {
        SLOG_ERROR("[UDP] Open refused — already open (port {})", _arm->LocalPort());
        return false;
    }
    if (config.maxDatagram == 0 || config.maxDatagram > MAX_UDP_PAYLOAD
        || config.maxSendDatagram < config.maxDatagram || config.maxSendDatagram > MAX_UDP_PAYLOAD)
    {
        SLOG_ERROR("[UDP] Open refused — maxDatagram {} / maxSendDatagram {} (1 <= maxDatagram <= maxSendDatagram <= {})",
                   config.maxDatagram, config.maxSendDatagram, MAX_UDP_PAYLOAD);
        return false;
    }
    if (config.recvSlots == 0 || config.sendSlots == 0 || config.bigSendSlots == 0)
    {
        SLOG_ERROR("[UDP] Open refused — recvSlots {} / sendSlots {} / bigSendSlots {} (all must be > 0)",
                   config.recvSlots, config.sendSlots, config.bigSendSlots);
        return false;
    }

    _stats.received.store(0);
    _stats.sent.store(0);
    _stats.oversizeDropped.store(0);
    _stats.recvFail.store(0);
    _stats.sendFail.store(0);
    _stats.lastRecvError.store(0);
    _stats.lastSendError.store(0);
    _stats.wakes.store(0);
    _stats.flushes.store(0);

    _arm = CreateUdpArm(config, _stats);
    return _arm != nullptr;
}

void CUdpEndpoint::Close()
{
    _arm.reset();
}

uint16_t CUdpEndpoint::LocalPort() const
{
    return _arm ? _arm->LocalPort() : 0;
}

size_t CUdpEndpoint::Receive(UdpDatagram* out, size_t max)
{
    if (!_arm || out == nullptr || max == 0)
        return 0;
    return _arm->Receive(out, max);
}

bool CUdpEndpoint::Send(const UdpAddr& to, const uint8_t* p, uint32_t n)
{
    if (!_arm || (p == nullptr && n > 0))
        return false;
    return _arm->Send(to, p, n);
}

void CUdpEndpoint::Flush()
{
    if (_arm)
        _arm->Flush();
}

CUdpEndpoint::WaitResult CUdpEndpoint::Wait(uint32_t ms)
{
    if (!_arm)
        return WaitResult::Timeout;
    return _arm->Wait(ms);
}

CUdpEndpoint::NativeWait CUdpEndpoint::WaitHandle() const
{
#ifdef _WIN32
    return _arm ? _arm->WaitHandle() : nullptr;
#else
    return _arm ? _arm->WaitHandle() : -1;
#endif
}

void CUdpEndpoint::Wake()
{
    if (!_arm)
        return;
    _stats.wakes.fetch_add(1, std::memory_order_relaxed);
    _arm->Wake();
}
