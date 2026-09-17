#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <cstdint>
#include <chrono>

#include "LockFreeConfig.h"
#include "CoreAffinity.h"

//=============================================================================
// CTimingWheel
//
// 세션 무활동 타임아웃을 O(1) tick으로 처리하는 타이밍 휠.
//
// [설계 원칙]
// - 휠 자료구조는 타이머 스레드만 접근 (락 불필요)
// - 외부 스레드(워커/Accept)는 lock-free 큐를 통해 요청만 전달
// - 타이머 스레드가 큐를 drain한 후 휠 조작 + tick 처리
//=============================================================================

class CTimingWheel
{
public:
    // 타임아웃 콜백 시그니처: sessionId를 넘겨받아 disconnect 처리
    using TimeoutCallback = void(*)(void* context, int64_t sessionId);

    //-------------------------------------------------------------------------
    // 외부에서 전달하는 요청 타입
    //-------------------------------------------------------------------------
    enum class RequestType : uint8_t
    {
        REGISTER,   // 세션 등록 (Accept 시)
        REFRESH,    // 수명 갱신 (Recv 성공 시)
        UNREGISTER  // 세션 제거 (Release 시)
    };

    struct Request
    {
        RequestType type;
        uint16_t index;
        int64_t sessionId;
    };

private:
    //-------------------------------------------------------------------------
    // 휠 내부 노드 — 각 슬롯의 이중 연결 리스트 원소
    //-------------------------------------------------------------------------
    struct WheelNode
    {
        uint16_t sessionIndex;
        int slotIndex;          // 현재 소속 슬롯 (-1이면 미등록)
        int64_t sessionId;      // 등록 시점의 sessionId (ABA 방지)
        WheelNode* prev;
        WheelNode* next;
    };

    //-------------------------------------------------------------------------
    // 슬롯 — 이중 연결 리스트의 더미 헤드
    //-------------------------------------------------------------------------
    struct Slot
    {
        WheelNode head; // sentinel (prev/next만 사용)

        Slot()
        {
            head.prev = &head;
            head.next = &head;
            head.sessionIndex = UINT16_MAX;
            head.slotIndex = -1;
        }

        bool IsEmpty() const { return head.next == &head; }
    };

public:
    CTimingWheel()
        : _maxSessions(0)
        , _timeoutSec(0)
        , _tickIntervalMs(0)
        , _slotCount(0)
        , _cursor(-1)
        , _running(false)
        , _callback(nullptr)
        , _callbackContext(nullptr)
    {
    }

    // 복사/이동 금지
    CTimingWheel(const CTimingWheel&) = delete;
    CTimingWheel& operator=(const CTimingWheel&) = delete;
    CTimingWheel(CTimingWheel&&) = delete;
    CTimingWheel& operator=(CTimingWheel&&) = delete;

    //-------------------------------------------------------------------------
    // Init
    // - maxSessions: 최대 동시 세션 수
    // - timeoutSec: 무활동 타임아웃 (초)
    // - tickIntervalMs: tick 주기 (밀리초). 운영값 = IOCPServer::TIMER_TICK_INTERVAL_MS = 1000 (1초)
    // 
    //   세션 타임아웃은 수십 초 단위이므로 고해상도 tick 불필요
    //   1초 해상도로 60초 타임아웃 -> 슬롯 61개(60000/1000+1). 슬롯 메모리 부담은 무시 수준.
    //   +1 보정 덕분에 만료는 항상 타임아웃 이후다(60 ~ 60+tick초) — 일찍 끊기는 경우는 없고, 늦어질 뿐이다
    //   반환값: 초기화 성공 여부
    //-------------------------------------------------------------------------
    bool Init(uint16_t maxSessions, int timeoutSec, int tickIntervalMs = 1000)
    {
        if (maxSessions == 0 || timeoutSec <= 0 || tickIntervalMs <= 0)
            return false;

        // 올림 나눗셈: 나머지가 있으면 몫+1, 나누어떨어지면 그대로
        int slotCount = (timeoutSec * 1000 + tickIntervalMs - 1) / tickIntervalMs;
        if (slotCount <= 0)
            return false;

        // DrainRequests와 Tick이 같은 루프에서 수행되므로, 삽입 직후의
        // tick은 다른 슬롯을 처리한다. cursor가 다시 돌아오려면
        // slotCount-1 tick만 필요하므로, 슬롯 1개를 추가하여 정확한 timeout을 보장한다.
        _maxSessions = maxSessions;
        _timeoutSec = timeoutSec;
        _tickIntervalMs = tickIntervalMs;
        _slotCount = slotCount + 1;
        _cursor = _slotCount - 1;

        // 슬롯 배열 생성 (재할당 불가 — 내부 sentinel 포인터 안정성 보장)
        _slots = std::make_unique<Slot[]>(_slotCount);

        // 세션별 노드 사전 할당 (인덱스 기반 O(1) 접근)
        _nodes = std::make_unique<WheelNode[]>(maxSessions);
        for (uint16_t i = 0; i < maxSessions; ++i)
        {
            _nodes[i].sessionIndex = i;
            _nodes[i].slotIndex = -1;
            _nodes[i].sessionId = 0;
            _nodes[i].prev = nullptr;
            _nodes[i].next = nullptr;
        }

        if (!_requestQueue.Init())
            return false;

        return true;
    }

    ~CTimingWheel()
    {
        Stop();
    }

    //-------------------------------------------------------------------------
    // 타이머 스레드 시작/종료
    //-------------------------------------------------------------------------
    void Start(TimeoutCallback callback, void* context)
    {
        if (_running.load(std::memory_order_acquire))
            return;

        _callback = callback;
        _callbackContext = context;
        _running.store(true, std::memory_order_release);
        _timerThread = std::thread(&CTimingWheel::TimerThreadProc, this);
    }

    void Stop()
    {
        if (_running.exchange(false, std::memory_order_acq_rel))
        {
            if (_timerThread.joinable())
                _timerThread.join();
        }
    }

    //-------------------------------------------------------------------------
    // 외부 스레드용 인터페이스 (lock-free 큐에 push)
    // 워커/Accept 스레드에서 호출한다.
    //-------------------------------------------------------------------------
    void RequestRegister(uint16_t index, int64_t sessionId)
    {
        _requestQueue.Enqueue({ RequestType::REGISTER, index, sessionId });
    }

    void RequestRefresh(uint16_t index, int64_t sessionId)
    {
        _requestQueue.Enqueue({ RequestType::REFRESH, index, sessionId });
    }

    void RequestUnregister(uint16_t index, int64_t sessionId)
    {
        _requestQueue.Enqueue({ RequestType::UNREGISTER, index, sessionId });
    }

private:
    //-------------------------------------------------------------------------
    // 타이머 스레드 메인 루프
    //-------------------------------------------------------------------------
    void TimerThreadProc()
    {
        CoreAffinity::PinIoThread();   // 타이머 스레드 → 게임코어 밖으로 (격리 off면 no-op)

        // 절대 시각 기준 tick 보정 — Sleep 후 처리 시간만큼 다음 Sleep을 줄여
        // tick 간격이 누적으로 밀리지 않도록 한다.
        auto nextTick = std::chrono::steady_clock::now();

        while (_running.load(std::memory_order_acquire))
        {
            nextTick += std::chrono::milliseconds(_tickIntervalMs);
            auto now = std::chrono::steady_clock::now();
            if (nextTick > now)
                std::this_thread::sleep_for(nextTick - now);

            // 1. lock-free 큐에서 요청 처리 (drain)
            DrainRequests();

            // 2. cursor 전진 → 해당 슬롯의 세션 전부 만료 처리
            Tick();
        }
    }

    //-------------------------------------------------------------------------
    // 큐에 쌓인 요청을 모두 꺼내서 휠 조작
    //-------------------------------------------------------------------------
    void DrainRequests()
    {
        Request req;
        while (_requestQueue.Dequeue(&req))
        {
            switch (req.type)
            {
            case RequestType::REGISTER:
                RegisterInternal(req.index, req.sessionId);
                break;
            case RequestType::REFRESH:
                RefreshInternal(req.index, req.sessionId);
                break;
            case RequestType::UNREGISTER:
                UnregisterInternal(req.index, req.sessionId);
                break;
            }
        }
    }

    //-------------------------------------------------------------------------
    // Tick: cursor를 전진시키고 해당 슬롯의 모든 세션을 만료 처리
    //-------------------------------------------------------------------------
    void Tick()
    {
        _cursor = (_cursor + 1) % _slotCount;

        Slot& slot = _slots[_cursor];
        WheelNode* node = slot.head.next;

        while (node != &slot.head)
        {
            WheelNode* next = node->next; // 제거 전에 next 보존

            // 노드를 리스트에서 분리
            node->prev->next = node->next;
            node->next->prev = node->prev;
            node->slotIndex = -1;
            node->prev = nullptr;
            node->next = nullptr;

            // 타임아웃 콜백 호출 (sessionId 전달로 ABA 방지)
            if (_callback)
            {
                _callback(_callbackContext, node->sessionId);
            }

            node = next;
        }
    }

    //-------------------------------------------------------------------------
    // 내부 휠 조작 (타이머 스레드에서만 호출)
    //-------------------------------------------------------------------------

    // 세션을 휠에 등록 — 현재 cursor 기준 한 바퀴 뒤 슬롯에 삽입
    void RegisterInternal(uint16_t index, int64_t sessionId)
    {
        if (index >= _maxSessions)
            return;

        WheelNode& node = _nodes[index];

        // 이미 등록된 상태면 먼저 제거
        if (node.slotIndex != -1)
            RemoveFromSlot(node);

        node.sessionId = sessionId;
        InsertToTimeoutSlot(node);
    }

    // 수명 갱신 — 기존 슬롯에서 빼고 한 바퀴 뒤 슬롯으로 이동
    void RefreshInternal(uint16_t index, int64_t sessionId)
    {
        if (index >= _maxSessions)
            return;

        WheelNode& node = _nodes[index];

        // 미등록이거나 다른 세션이면 무시 (ABA 방지)
        if (node.slotIndex == -1 || node.sessionId != sessionId)
            return;

        RemoveFromSlot(node);
        InsertToTimeoutSlot(node);
    }

    // 세션을 휠에서 제거 (정상 종료 시)
    void UnregisterInternal(uint16_t index, int64_t sessionId)
    {
        if (index >= _maxSessions)
            return;

        WheelNode& node = _nodes[index];

        // 같은 세션만 제거 (ABA 방지)
        if (node.slotIndex != -1 && node.sessionId == sessionId)
            RemoveFromSlot(node);
    }

    //-------------------------------------------------------------------------
    // 리스트 조작 유틸
    //-------------------------------------------------------------------------

    // 노드를 현재 소속 슬롯에서 제거
    void RemoveFromSlot(WheelNode& node)
    {
        node.prev->next = node.next;
        node.next->prev = node.prev;
        node.slotIndex = -1;
        node.prev = nullptr;
        node.next = nullptr;
    }

    // 노드를 타임아웃 슬롯(현재 cursor = 한 바퀴 후 만료)에 삽입
    void InsertToTimeoutSlot(WheelNode& node)
    {
        // Tick()은 cursor를 전진 후 처리하므로, 현재 cursor 슬롯은
        // 방금 처리된 위치 — 다시 도달까지 정확히 N tick (= timeoutSec)
        int targetSlot = _cursor;

        Slot& slot = _slots[targetSlot];

        // 리스트 tail에 삽입 (head.prev = tail)
        node.prev = slot.head.prev;
        node.next = &slot.head;
        slot.head.prev->next = &node;
        slot.head.prev = &node;

        node.slotIndex = targetSlot;
    }

private:
    uint16_t _maxSessions;
    int _timeoutSec;
    int _tickIntervalMs;
    int _slotCount;         // 전체 슬롯 수
    int _cursor;            // 현재 tick 위치

    std::atomic<bool> _running;
    std::thread _timerThread;

    std::unique_ptr<Slot[]> _slots;           // 슬롯 배열 (재할당 불가, sentinel 포인터 안정성 보장)
    std::unique_ptr<WheelNode[]> _nodes;      // 세션 인덱스 기반 노드 배열 (사전 할당)

    // 외부 스레드 → 타이머 스레드 요청 큐 (lock-free)
    LockFree::CLockFreeQueue<Request> _requestQueue;

    // 타임아웃 콜백
    TimeoutCallback _callback;
    void* _callbackContext;
};
