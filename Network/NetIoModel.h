#pragma once
// ==========================================================================
// NetIoModel — 네트워크 I/O 모델 이름표
//
//   골격 클래스는 하나(CIOCPServer)이고, 실제 I/O는 전송 계층 파일 하나가 구현한다.
//     Windows : Transport_Iocp.cpp (완료 포트)  또는 Transport_Rio.cpp (Registered I/O)
//     Linux   : Transport_Epoll.cpp
//   즉 OS나 토글이 바뀌어도 게임 로직이 보는 타입은 그대로다 — 이 별칭이 그 사실을 드러낸다.
//
//   [옛 설계와 달라진 점] 처음에는 OS마다 서버 클래스를 따로 두고(CEpollServer 등) 여기서
//   고르려 했다. 그 사이 전송 계층이 Transport_*.cpp로 분리되면서, 클래스를 나눌 이유가
//   사라졌다 — 갈리는 것은 "제출·수거 방식"뿐이고 세션 관리·패킷 분해는 공통이다.
//
//   지금 도는 모델은 서버 부팅 로그 "Network I/O model: ..."로 확인한다.
// ==========================================================================
#include "IOCPServer.h"

using NetIoModel = CIOCPServer;

#ifdef _WIN32
    #if USE_RIO_TRANSPORT
        inline constexpr const char* kNetIoModelName = "RIO";
    #else
        inline constexpr const char* kNetIoModelName = "IOCP";
    #endif
#else
    inline constexpr const char* kNetIoModelName = "epoll";
#endif
