#pragma once

// ==========================================================================
// 서버 코어 빌드 토글 — 전송·송신 경로 (ServerBase / ServerNetwork 전용)
//
// 여기 있는 토글은 "바이트를 어떻게 소켓에 밀어넣는가"만 정한다.
// 누구에게 무엇을 보낼지(섹터·멤버십·DB)는 게임 쪽 BuildConfig.h 가 정한다.
//
// 주의: #if 로 이 토글을 분기하는 모든 TU에서 "가장 먼저" include 해야 한다.
//       include 순서가 어긋나 정의가 보이지 않으면 매크로는 조용히 0으로
//       평가되어 A/B 빌드가 어긋날 수 있다.
// ==========================================================================
// SendQ 구현 경로 선택
//   1: LockFreeQueue 경로
//   0: 기존 RingBuffer 경로 [권장] 
#define USE_LOCKFREE_SENDQ 0

// Send flush 시점 선택 (USE_LOCKFREE_SENDQ와 직교 — PostSend 공통 경로에 적용)
//   0: 기존 baseline — RequestSendMsg마다 즉시 PostSend
//   1: 틱 끝에 세션당 1회 flush (송신 coalescing → WSASend 시스콜 절감, WSABUF↑) [권장]
#define USE_SEND_COALESCING 1

// 커널 송신버퍼 크기 0 실험 (accept 소켓마다 SO_SNDBUF=0)
//   목적: WSASend 호출비용 중 "커널 송신버퍼 복사(③)"가 실제로 미미한지 실측 검증.
//   0: 기존 OS 기본값 [권장]
//   1: SO_SNDBUF=0 (zero-copy, 송신은 ACK까지 pending) 
//   주의: 세션당 송신 제출 직렬화와 겹쳐 flush_send가 호출수 감소로 줄 수 있음 →
//         mmo_wsa_send_calls_total(틱당 호출수)을 반드시 함께 봐야 해석 가능.
#define USE_ZERO_SNDBUF 0

// 송신 스레드 분리 실험 — A1: 단일 send 스레드 (offload 효과 격리)
//   목적: 틱의 74%(flush_send=WSASend 루프)를 게임루프 임계경로에서 떼어내
//         "오프로딩만으로 틱 천장이 오르나"를 격리 검증.
//   1: 게임루프는 FlushPendingSends에서 dirty 배치(sessionId)만 send 스레드에 넘기고,
//      전용 send 스레드 1개가 FindSession→PostSend로 WSASend 수행 (게임루프와 파이프라인)
//   0: 기존 — 게임루프가 직접 PostSend 루프 (baseline)

//   주의: tick p99 하락만 보지 말고 send 스레드 CPU·완료율(wsa_send_completions)·
//         _flushQueue 백로그를 함께 봐야 천장 이동 지점이 보임.
#define USE_SEND_THREAD 1

//   의존: USE_SEND_COALESCING=1 필요 (_dirtySessions 배치를 재사용). =0이면 dirty 배치가
//         안 쌓여 무의미하므로 함께 켤 것.
#if USE_SEND_THREAD && !USE_SEND_COALESCING
	#error "USE_SEND_THREAD requires USE_SEND_COALESCING=1 (dirty 배치 핸드오프 의존)"
#endif

// RIO 전송 계층 실험 — WSARecv/WSASend/GQCS(IOCP)를 Registered I/O로 교체.
//   전송 계층만 교체하고 상위(세션 수명·링버퍼·coalescing·digest·게임루프)는 불변.
//   1: RIO 경로 — 버퍼 사전등록 슬랩·유저모드 완료큐·RioWorker N개가 WT/SendWorker 풀 대체 [실험]
//   0: 기존 IOCP 경로 (baseline) [기본]
//   주의: 1이면 WorkerThread/SendWorkerThread/IOCP 핸들/CancelIoEx 경로가 컴파일 제외되고
//         INI의 WorkerThreads/SendWorkers 대신 RioWorkers를 쓴다.
//         RIO 소켓은 RIO 전용 — 일반 WSASend/WSARecv가 거부된다(10038, Phase0 스모크 실측).
//         전송 교체 구현 커밋 전 골격 단계에서는 1로 켜도 동작이 바뀌지 않는다.
#define USE_RIO_TRANSPORT 0

//   배타: LockFree SendQ는 CSerialBuffer 포인터를 WSABUF로 직접 송신하는데, 그 메모리는
//         등록 슬랩 밖이라 RIO_BUF(BufferId+Offset)로 표현 불가 — 링버퍼 경로 전용.
#if USE_RIO_TRANSPORT && USE_LOCKFREE_SENDQ
	#error "USE_RIO_TRANSPORT requires USE_LOCKFREE_SENDQ=0 (링버퍼=등록슬랩 슬라이스 경로 전용)"
#endif

//   의존: 게임 송신이 dirty 배치 → 소유 워커 핸드오프로만 흐른다 (틱 끝 flush 필수).
#if USE_RIO_TRANSPORT && !USE_SEND_COALESCING
	#error "USE_RIO_TRANSPORT requires USE_SEND_COALESCING=1 (틱 끝 dirty 배치 핸드오프 의존)"
#endif
