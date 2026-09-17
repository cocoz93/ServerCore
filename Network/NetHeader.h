#pragma once

#include <cstdint>

//==================================================
// 네트워크 계층이 아는 와이어 헤더 규약
//   게임 패킷 정의(Protocol.h)와 분리한다 — 전송 계층은 길이/타입 헤더만 알면 되고
//   게임별 패킷 구조체를 알 필요가 없다.
//   Protocol.h 가 이 파일을 include 하므로 기존 사용처는 수정 불필요.
//==================================================

// 패킷 타입 (혼용 방지를 위해 L7 Msg로 표기) — 정의는 Protocol.h
enum class MsgType : uint16_t;

#pragma pack(push, 1)

// 패킷 헤더 (모든 패킷 공통)
struct MsgHeader
{
    uint16_t size;        // 패킷 전체 크기 (헤더 포함)
    MsgType type;         // 패킷 타입
};

// 에코 테스트용 헤더 (GameCodiEchoTest 더미 클라이언트 호환)
struct EchoMsgHeader
{
    uint16_t size;        // 페이로드 크기 (헤더 미포함)
};

#pragma pack(pop)
