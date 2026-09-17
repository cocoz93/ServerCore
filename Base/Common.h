#pragma once

// ==========================================================================
// 프로젝트 공용 타입 / 상수
// ==========================================================================

// 서버 동작 모드 (GameServer, IOCPServer 공용)
enum class ServerMode
{
    GameCodiEchoTest,    // GameCodi 에코 더미 클라이언트 테스트용 - 헤더: EchoMsgHeader(2byte), size=페이로드크기
    NetWorkLib_EchoTest, // 네트워크 라이브러리 검증용 에코 - 헤더: MsgHeader(4byte), size=전체크기(헤더포함)
    GameServer,          // MMO 게임 서버 (네트워크 + 게임 로직) - 헤더: MsgHeader(4byte), size=전체크기(헤더포함)
};
