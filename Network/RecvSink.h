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

struct SinkConnected
{
    int64_t sessionId = 0;
    int     tag       = 0;   // 접속을 받은 포트의 구분 값(AddListener) · ConnectTo 로 건 것이면 거기 준 값 · 생성자 포트는 0
};

struct SinkReceived
{
    int64_t        sessionId = 0;
    int            tag       = 0;
    CSerialBuffer* msg       = nullptr;   // ref 하나를 넘긴다 — 받는 쪽이 SubRef 하거나 RequestSendMsg 로 넘겨 돌려준다
};

struct SinkDisconnected
{
    int64_t sessionId = 0;
    int     tag       = 0;
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
};
