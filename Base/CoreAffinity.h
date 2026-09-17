#pragma once
#include <cstdint>

// ==========================================================================
// CoreAffinity — 게임스레드 코어 격리 (실험용)
//
// [무엇]
//   프로세스 affinity(ServerCores) 안에서 게임루프 스레드만 전용 코어로 고정하고,
//   I/O 스레드(IOCP 워커·송신 워커·Accept·DB·타이머·모니터)는 나머지 코어로 몰아낸다.
//
// [왜]
//   게임루프는 연산·hot 데이터(플레이어·섹터)를 쥔 캐시 간섭의 "피해자",
//   송신/IOCP는 전부 I/O로 WSASend 메모리트래픽을 뿌리는 "원인"이다.
//   전용 코어로 게임루프의 L2를 보호해, 균등화로 잃은 캐시 간섭분(~10%)을 회복 시도.
//
// [사용]
//   main이 프로세스 affinity 설정 "직후"(스레드 생성 전) SetIsolationMasks를 1회 호출하고,
//   각 스레드가 자기 진입부에서 PinGameThread()/PinIoThread()로 자가-핀한다.
//   두 마스크가 0이면(격리 off) 전부 no-op → 프로세스 마스크 상속(현행 동작과 동일 = A/B baseline).
//
// [설계 메모]
//   구현은 CoreAffinity.cpp에 둔다 — windows.h를 이 헤더 밖으로 빼서, 이 헤더를 어느 TU/헤더에
//   넣어도 <WinSock2.h> include 순서(구 winsock.h 충돌)를 신경 쓸 필요가 없게 한다.
// ==========================================================================

namespace CoreAffinity
{
    // 게임/ I/O 코어 마스크를 심는다. main이 프로세스 affinity 직후 스레드 생성 전에 1회 호출.
    //   격리 off거나 도출 실패면 (0, 0)을 넘기면 된다(= 이후 Pin*은 모두 no-op).
    void SetIsolationMasks(uint64_t gameMask, uint64_t ioMask);

    // 스레드 진입부에서 자기 자신을 해당 코어군에 고정. mask==0이면 no-op.
    void PinGameThread();   // 게임루프 스레드 → 게임 전용 코어
    void PinIoThread();     // I/O 스레드 → 나머지 코어(게임코어 밖)
}
