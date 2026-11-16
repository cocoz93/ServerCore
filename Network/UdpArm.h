#pragma once

// CUdpEndpoint 의 팔 바탕 — 구현 파일만 본다(UdpEndpoint.cpp · UdpEndpoint_Win.cpp · UdpEndpoint_Linux.cpp).
//   CUdpEndpoint 는 설정을 확인하고 통계를 쥔 채 팔에 넘기기만 한다. 소켓 · 버퍼 · 기다림은 팔이 쥔다.

#include "UdpEndpoint.h"

class CUdpEndpoint::Arm
{
public:
    virtual ~Arm() = default;

    virtual uint16_t   LocalPort() const = 0;
    virtual size_t     Receive(UdpDatagram* out, size_t max) = 0;
    virtual bool       Send(const UdpAddr& to, const uint8_t* p, uint32_t n) = 0;
    virtual void       Flush() = 0;
    virtual WaitResult Wait(uint32_t ms) = 0;
    virtual NativeWait WaitHandle() const = 0;
    virtual void       Wake() = 0;
};

// 팔을 만들며 소켓까지 연다 — 실패면 nullptr(까닭은 로그). 설정은 CUdpEndpoint::Open 이 이미 확인했다
std::unique_ptr<CUdpEndpoint::Arm> CreateUdpArm(const CUdpEndpoint::Config& config, CUdpEndpoint::Stats& stats);
