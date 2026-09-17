// ==========================================================================
// CMonitorManager — 서버 모니터링 지표 저장소
//
// [책임]
//  - 서버 런타임 지표를 Interlocked 카운터/게이지/히스토그램으로 수집
//  - 워커 스레드, 게임 루프 스레드 등 다중 스레드에서 안전하게 기록
//  - HTTP 엔드포인트(2단계)에서 읽어 Prometheus 형식으로 노출
//
// [소유권]
//  - main()이 값으로 소유, GameServer/IOCPServer는 레퍼런스로 참조
// ==========================================================================
#pragma once

#include <cstdint>
#include <atomic>

#include "Platform/Platform.h"   // Platform::ThreadCpuHandle (스레드 CPU 측정 핸들)

// ── 원자 카운터 (통계/게이지) ──
//   순서 의존 없는 지표라 relaxed 고정 (Interlocked→std::atomic 포팅 + 캡슐화).
//   비복사(atomic 멤버). 읽기는 암시적 변환(operator int64_t)으로 표시 코드 불변.
//   operator= 미제공 → 평문 대입이 컴파일 에러로 잡혀 누락 방지(명시적 Store 강제).
struct Counter
{
    std::atomic<int64_t> v{ 0 };

    Counter() = default;
    Counter(const Counter&) = delete;
    Counter& operator=(const Counter&) = delete;

    void    Inc()               { v.fetch_add(1, std::memory_order_relaxed); }
    void    Add(int64_t n)      { v.fetch_add(n, std::memory_order_relaxed); }
    void    Store(int64_t n)    { v.store(n, std::memory_order_relaxed); }
    int64_t FetchAdd(int64_t n) { return v.fetch_add(n, std::memory_order_relaxed); }  // 반환값 필요 시(슬롯 index)
    int64_t Load() const        { return v.load(std::memory_order_relaxed); }
    operator int64_t() const    { return v.load(std::memory_order_relaxed); }
};

class CMonitorManager
{
public:
    CMonitorManager() = default;
    ~CMonitorManager() = default;

    CMonitorManager(const CMonitorManager&) = delete;
    CMonitorManager& operator=(const CMonitorManager&) = delete;

    // ══════════════════════════════════════════════════════════════
    // 카운터 (monotonically increasing)
    //
    // 워커 스레드 핫패스(per-packet)에서 동시 갱신되는 카운터를
    // alignas(64)로 캐시 라인 분리하여 false sharing 방지
    // ══════════════════════════════════════════════════════════════

    // ── Recv 핫 카운터 (ProcessRecv 경로, 모든 워커 스레드) ──
    alignas(64) Counter _recvPackets;   // 수신 패킷 누적
    Counter _recvBytes;                 // 수신 바이트 누적
    Counter _wsaRecvCalls;              // WSARecv 시스템 콜 횟수
    Counter _recvContention;            // [epoll 전용] 수신 게이트 경합 — 다른 워커가 그 세션을 읽는 중이라
                                        //   물러난 횟수. level-trigger는 아직 안 비운 소켓을 여러 워커에게
                                        //   거듭 알려서, 물러난 워커가 곧바로 같은 통지를 다시 받는다(헛돎).
                                        //   이 값이 수신 syscall 수에 견줘 크면 그 헛돎이 CPU를 먹고 있다는 뜻.
                                        //   (IOCP 팔은 완료가 워커 하나에만 가므로 항상 0)

    // ── Send 핫 카운터 (ProcessSend/PostSend/RequestSendMsg 경로, 모든 워커 스레드) ──
    alignas(64) Counter _sendPackets;    // 논리적 송신 패킷 누적 (RequestSendMsg에서 Enqueue 성공 시)
    Counter _sendBytes;                  // 송신 바이트 누적
    Counter _wsaSendCalls;               // WSASend 시스템 콜 횟수
    Counter _wsaSendCompletions;         // WSASend 완료 횟수 (ProcessSend IOCP 콜백)
    Counter _sendEnqueuedBytes;          // SendQ Enqueue 바이트 누적 (_sendBytes와의 차이 = 체류량)
    Counter _sendContention;             // PostSend 경합 — 다른 스레드가 제출 중이라 물러난 횟수
    Counter _sendFollowUp;               // [다중 pending] 완료를 기다려 "이어 보낸" 횟수 —
                                         //   한 번에 다 못 보내 완료 왕복이 끼었다는 뜻. 깊이를 올려
                                         //   이 값이 줄어드는지가 곧 깊이의 효과다.
                                         //   [계측 비용 0] 이미 미제출량을 확인하는 자리에서 센다 —
                                         //   포화 여부를 따로 확인하면 정상 경로에 링 락이 붙어
                                         //   깊이 1 팔만 불리해진다(A/B 편향).
    Counter _sendWrapSplits;             // [다중 pending] 링 랩 때문에 직선 구간만 보낸 횟수.
                                         //   RIO 팔의 "꼬리는 완료 후 재제출" 왕복 빈도 = 배치 제출(DEFER) 도입 판단 근거.

    // ── 세션/에러 카운터 (per-session 또는 rare, 낮은 빈도) ──
    alignas(64) Counter _sessionCreated;    // 세션 생성 누적
    Counter _sessionDestroyed;               // 세션 소멸 누적
    Counter _acceptFailed;                   // Accept 거부 (인덱스 부족)
    Counter _acceptRejectedByQueue;          // Accept 거부 (게임 이벤트 큐 과부하)
    Counter _sessionTimedOut;                // 타이밍 휠 타임아웃 킥
    Counter _packetErrors;                   // 패킷 에러 (크기 검증 실패, 알 수 없는 타입)
    Counter _sendQueueOverflow;              // SendQ 오버플로 (Enqueue 실패)
    Counter _partialSend;                    // 진짜 partial send (WSASend 성공이나 일부만 전송) → disconnect 횟수
    Counter _recvBufferOverflow;             // RecvQ 오버플로 (수신 버퍼 가득 참)
    Counter _sendDiscardedBytes;             // Disconnect 시 SendQ 잔여 바이트 (체류량 보정용)

    // ── DB 저장 파이프라인 (USE_DB_WORKER) — dirty flag 비동기 저장 ──
    alignas(64) Counter _dbSavedJobs;    // UPSERT 성공 누적
    Counter _dbFailedJobs;               // UPSERT 실패 누적
    Counter _dbDroppedJobs;              // 백프레셔 드롭 누적 (슬롯 큐 상한 초과)
    static constexpr int MAX_DB_WORKERS = 16;
    Counter _dbQueueDepth[MAX_DB_WORKERS];               // 워커별 게이지: 마지막 배치 인출량
    Counter _dbWorkerCount;                              // 노출 루프 상한 (CDBWorker::Start에서 설정)

    // ══════════════════════════════════════════════════════════════
    // 게이지 (up/down)
    // ══════════════════════════════════════════════════════════════
    alignas(64) Counter _sessionCount;    // 현재 동접 수
    // 이벤트 큐 크기는 ThreadSafeQueue::GetSize()로 직접 조회

    // 완료 수거 방식 실효값 (INI CompletionBatch) — 0=GQCS 1건씩, N>0=GQCSEx 최대 N건.
    //   수집 스크립트가 아암 라벨과 대조해 "INI 미적용/오라벨"을 잡는다(mmo_transport_rio와 같은 용도).
    //   IOCPServer::Start가 clamp 후 1회 기록, 이후 불변.
    volatile LONG _completionBatch = 0;

    // 송신 깊이 실효값 (INI SendDepth) — 1=기존 1-pending, 2/4/8=다중 pending.
    //   수집 스크립트가 아암 라벨과 대조해 "INI 미적용/오라벨"을 잡는다(_completionBatch와 같은 용도).
    //   IOCPServer::Start가 2의 거듭제곱으로 내린 뒤 1회 기록, 이후 불변.
    volatile LONG _sendDepth = 0;

    // ══════════════════════════════════════════════════════════════
    // 게임 루프 전용 카운터 (alignas(64)로 캐시 라인 분리)
    //
    // 워커 스레드 카운터와 false sharing 방지를 위해 별도 struct로 격리
    // ══════════════════════════════════════════════════════════════
    struct alignas(64) GameLoopCounters
    {
        Counter _zoneChangeCount;   // 존 이동 횟수
        Counter _cheatDetected;     // 이동 검증 실패 (치트 감지) — 미구현, 향후 서버 권위 좌표 검증 시 사용 예정

        // 구간별 시간 (마이크로초 누적, counter)
        // Prometheus에서 rate(phase_sum) / rate(tick_count) → 틱당 평균 소비 시간
        Counter _phaseNetworkUs;        // ProcessNetworkEvents 소비 시간
        Counter _phaseGameLogicUs;      // TickAll + 섹터 변경 + 클램핑 브로드캐스트
        Counter _phaseBroadcastSyncUs;  // 주기적 위치 동기화 브로드캐스트

        // 이벤트 큐 크기 (게이지, 게임 루프 스레드에서만 갱신)
        // ProcessNetworkEvents() 진입 전 스냅샷 → 소비 못 따라가면 누적되는 걸 감지
        Counter _eventQueueSize;

        // 브로드캐스트 비용 (counter)
        // 평균 대상 수 = rate(targets) / rate(calls)
        Counter _broadcastCalls;        // 브로드캐스트 호출 횟수
        Counter _broadcastTargets;      // 브로드캐스트 대상 수 누적

        // 멤버십 변경 복사량 — BroadcastAroundSector(gather/enqueue 계측 대상) 밖의 송신(복사) 횟수.
        //   ProcessSectorChange·BroadcastEnterZone·BroadcastLeaveZone 전용인
        //   SendCreateOtherPlayer/SendDeletePlayer 호출 수. _broadcastTargets(계측 대상 복사)와
        //   비교해 비계측 복사의 비중을 가늠 → 정밀 측정(choke-point) 필요 여부 판단용.
        Counter _membershipSends;

        // 멤버십 변경 송신(복사) 시간 — ProcessSectorChange 구간 전용(섹터이동분).
        //   game_logic 페이즈(_phaseGameLogicUs)에 섞여 있던 멤버십 복사를 분리 측정 → 묶음 최적화 ROI 판단용.
        //   접속/퇴장(BroadcastEnter/LeaveZone)은 네트워크 페이즈라 1차 제외.
        Counter _membershipCostUs;

        // 같은 틱 이동자 쌍 보정 발동 횟수 — 틱 끝 일괄 통보가 놓친 CREATE/DELETE를 보충한 건수.
        //   섹터 점유는 이동 즉시 갱신되는데 통보는 틱 끝에 "그 시점 주민"을 읽으므로,
        //   같은 틱에 상대도 움직였으면 서로의 조회에서 빠진다(유령/투명인간). 그 보충분 집계.
        //   0에 수렴하면 보정이 불필요한 부하, 지속 증가면 정상(이동자 밀집도에 비례).
        Counter _membershipPairFixes;

        // C2S_MOVE_START 좌표 수용을 이동량 예산 초과로 거부한 횟수.
        //   정상 클라는 예산이 늘 만충이라 0에 머무는 것이 정상 — 0이 아니면 치트 시도이거나
        //   예산 계수(MOVE_BUDGET_SLACK)가 빡빡하다는 신호.
        Counter _moveBudgetRejects;

        // 비용종류별 계측 (counter, 마이크로초 누적) — 1단계: BroadcastAroundSector hot path 전용
        //
        // 기존 _phase*Us는 "루프 단계별"이라 하나의 브로드캐스트 비용이 network/broadcast_sync에
        // 흩어져 섞인다. 아래 3개는 "비용 종류별"로, 호출이 어느 단계든 같은 통에 누적한다.
        // 이로써 "복사(enqueue) vs 송신(flush) 누가 큰가"를 단계 경계와 무관하게 비교 가능.
        //   주의(1단계 범위): BroadcastAroundSector(move/stop/chat/sync/clamp)만 계측.
        //         BroadcastEnter/LeaveZone·ProcessSectorChange(접속/해제/존이동·섹터변경)의
        //         GetAroundPlayers·복사는 미포함 → 필요 시 2단계(RequestSendMsg choke point)로 확장.
        Counter _broadcastGatherUs;     // GetAroundPlayers 주변 모으기 (수신자 수 비례)
        Counter _broadcastEnqueueUs;    // 수신자별 처리(precount+배치AddRef+RequestSendMsg 복사)
        Counter _flushSendUs;           // FlushPendingSends 실제 송신(WSASend) — 틱 끝 1회

        // Tick 히스토그램
        //
        // 버킷 경계: 1, 5, 10, 20, 40, 60, 80, 100, 200 ms
        // 마지막 버킷(인덱스 9)은 200ms 초과 (+Inf)
        // 비누적 방식 저장 → Prometheus 노출 시 누적으로 변환
        static constexpr int TICK_BUCKET_COUNT = 10;
        static constexpr double TICK_BUCKET_BOUNDS[TICK_BUCKET_COUNT - 1] = {
            1.0, 5.0, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0, 200.0
        };

        Counter _tickBuckets[TICK_BUCKET_COUNT];
        Counter _tickSumUs;   // 합계 (마이크로초)
        Counter _tickCount;   // 총 tick 수

        // Tick 시간 기록 (밀리초 단위)
        void RecordTickTime(double ms)
        {
            int64_t us = static_cast<int64_t>(ms * 1000.0);
            _tickSumUs.Add(us);
            _tickCount.Inc();

            // 해당 버킷에 1 증가
            for (int i = 0; i < TICK_BUCKET_COUNT - 1; ++i)
            {
                if (ms <= TICK_BUCKET_BOUNDS[i])
                {
                    _tickBuckets[i].Inc();
                    return;
                }
            }
            // 모든 경계 초과 → +Inf 버킷
            _tickBuckets[TICK_BUCKET_COUNT - 1].Inc();
        }

        // Handle-latency 히스토그램
        //
        // recv 이벤트 enqueue → 게임루프 처리완료(응답 송신 포함) 시간 (ms).
        // 클라 RTT(왕복)에서 이 값을 빼면 네트워크+클라(더미) 기여분이 분리된다.
        // 버킷 경계는 틱과 동일(1~200ms): 40ms 틱 게이트 대기가 지배적이라 범위가 겹침.
        static constexpr int HANDLE_BUCKET_COUNT = 10;
        static constexpr double HANDLE_BUCKET_BOUNDS[HANDLE_BUCKET_COUNT - 1] = {
            1.0, 5.0, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0, 200.0
        };

        Counter _handleBuckets[HANDLE_BUCKET_COUNT];
        Counter _handleSumUs;   // 합계 (마이크로초)
        Counter _handleCount;   // 총 처리 이벤트 수

        // [계측 오버헤드 절감] 게임 스레드 전용 틱-로컬 누적기 (비원자 — 게임 루프만 접근).
        //   패킷마다 누적 → 틱 끝 FlushHandleLatency()가 전역에 1회 반영 (원자연산 패킷당 3 → 틱당 최대 12).
        int64_t _tickHandleBuckets[HANDLE_BUCKET_COUNT] = {};
        int64_t _tickHandleSumUs = 0;
        int64_t _tickHandleCount = 0;

        // Handle-latency 기록 (밀리초) — 게임 스레드 전용 지역 누적. 전역 반영은 FlushHandleLatency().
        void RecordHandleLatencyLocal(double ms)
        {
            _tickHandleSumUs += static_cast<int64_t>(ms * 1000.0);
            _tickHandleCount += 1;

            for (int i = 0; i < HANDLE_BUCKET_COUNT - 1; ++i)
            {
                if (ms <= HANDLE_BUCKET_BOUNDS[i])
                {
                    _tickHandleBuckets[i] += 1;
                    return;
                }
            }
            _tickHandleBuckets[HANDLE_BUCKET_COUNT - 1] += 1;
        }

        // 틱 끝 1회 호출 — 지역 누적분을 전역(원자) 카운터에 반영 후 리셋.
        void FlushHandleLatency()
        {
            if (_tickHandleCount == 0)
                return;   // 이번 틱 수신 처리 없음 → 원자연산 생략

            _handleSumUs.Add(_tickHandleSumUs);
            _handleCount.Add(_tickHandleCount);
            for (int i = 0; i < HANDLE_BUCKET_COUNT; ++i)
            {
                if (_tickHandleBuckets[i] != 0)
                {
                    _handleBuckets[i].Add(_tickHandleBuckets[i]);
                    _tickHandleBuckets[i] = 0;
                }
            }
            _tickHandleSumUs = 0;
            _tickHandleCount = 0;
        }
    } _gameLoop;

    // ══════════════════════════════════════════════════════════════
    // 워커 스레드별 처리량
    //
    // alignas(64)로 캐시 라인 분리 — 각 워커가 자기 슬롯만 갱신
    // ══════════════════════════════════════════════════════════════
    static constexpr int MAX_WORKER_THREADS = 64;

    struct alignas(64) WorkerCounter
    {
        volatile LONG64 completionCount = 0;
        volatile LONG64 dequeueCalls = 0;         // 완료 수거 호출 횟수 (GQCS / GQCSEx / RIODequeueCompletion)
        volatile Platform::ThreadCpuHandle threadHandle = Platform::kInvalidThreadCpuHandle;   // CPU 측정 핸들 (워커 시작 시 등록)

        // [계측 편향 방지] 위 두 카운터는 소유 워커 하나만 쓴다(RegisterWorkerThread가 슬롯을 전용 배정)
        //   — 읽는 쪽은 HTTP 스레드뿐이라 원자연산이 필요 없다. 굳이 평문 증가로 통일하는 이유는
        //   완료 수거 A/B 때문이다: 원자 증가로 두면 계측 비용이 완료 1건당(GQCS) vs 배치당(GQCSEx)으로
        //   갈려, 하필 가설이 기대하는 방향으로 팔이 유리해진다. x64 정렬 64비트 저장은 찢어지지 않는다.
        void AddCompletion() { completionCount = completionCount + 1; }
        void AddDequeue()    { dequeueCalls    = dequeueCalls + 1; }
    };

    WorkerCounter _workerCounters[MAX_WORKER_THREADS] = {};
    Counter _workerThreadCount;

    // ══════════════════════════════════════════════════════════════
    // 스레드 CPU 점유율 측정용 핸들
    //
    // [목적] 진단정리 6의 "계측 사각지대" 보강 — 게임루프가 무제한 드워커
    //        루프에 갇혀도 외부 관측자(HTTP 스레드)가 GetThreadTimes로 CPU를 읽음
    // [원리] 게임루프 스레드가 시작 시 DuplicateHandle로 실핸들을 복제해 등록
    //        (GetCurrentThread() 의사핸들은 호출 스레드 기준이라 타 스레드에서 못 씀)
    // [수명] 프로세스 종료까지 유지 (단일 핸들, 명시적 Close 생략 — 진단용)
    // ══════════════════════════════════════════════════════════════
    volatile Platform::ThreadCpuHandle _gameLoopThreadHandle = Platform::kInvalidThreadCpuHandle;

    // [USE_SEND_THREAD] 전용 송신 워커별 카운터 — 워커 카운터(_workerCounters)와 동일 패턴.
    //   alignas(64)로 워커 간 false sharing 차단. 워커이 0개(토글 OFF)면 _sendWorkerCount=0 →
    //   CPU 샘플러·백로그·flushUs 노출이 자동 생략(메트릭이 토글과 함께 꺼짐).
    static constexpr int MAX_SEND_WORKERS = 16;

    struct alignas(64) SendCounter
    {
        volatile Platform::ThreadCpuHandle threadHandle = Platform::kInvalidThreadCpuHandle;   // CPU 측정 핸들 (워커 시작 시 등록)
        Counter backlog;         // 마지막 drain 시 인출한 세션 수 (1틱 dirty 초과 = 송신 못 따라감)
        Counter flushUs;         // 이 워커의 WSASend 누적(us) — 게임루프 flush_send 이전분
    };

    SendCounter   _sendCounters[MAX_SEND_WORKERS] = {};
    Counter _sendWorkerCount;           // 노출 루프 상한 (Start에서 워커 수로 설정)

    // 워커 스레드 시작 시 호출 → 슬롯 인덱스 반환 (-1: 슬롯 초과)
    int RegisterWorkerThread()
    {
        int64_t index = _workerThreadCount.FetchAdd(1);   // 반환=이전 값 = 슬롯 인덱스
        if (index >= MAX_WORKER_THREADS)
        {
            _workerThreadCount.Add(-1);
            return -1;
        }
        return static_cast<int>(index);
    }

};
