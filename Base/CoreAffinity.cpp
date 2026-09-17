#include "CoreAffinity.h"
#include "Platform/Platform.h"   // OS별 affinity·타입 경계

// windows.h를 이 TU에만 가둔다(헤더는 clean). WinSock2 include 순서 문제와 무관.

namespace CoreAffinity
{
    namespace
    {
        // main 스레드가 스레드 생성 "전에" 1회 기록한다. std::thread 생성이 happens-before를
        // 만들어, 이후 생성되는 워커/게임/모니터 스레드는 이 값을 확실히 관측한다(atomic 불필요).
        uint64_t g_gameMask = 0;
        uint64_t g_ioMask   = 0;

        void PinCurrent(uint64_t mask)
        {
            if (mask == 0)
                return;   // 격리 off → no-op (프로세스 마스크 그대로 상속)

            // GetCurrentThread()는 "호출 스레드 자신"을 가리키는 의사핸들 —
            // 같은 스레드에서 SetThreadAffinityMask에 쓰기엔 유효하다(다른 스레드에서 읽을 때만 실핸들 필요).
            // mask는 항상 프로세스 affinity의 부분집합이라(ServerCores에서 도출) 실패하지 않는다.
            Platform::SetCurrentThreadAffinity(mask);
        }
    }

    void SetIsolationMasks(uint64_t gameMask, uint64_t ioMask)
    {
        g_gameMask = gameMask;
        g_ioMask   = ioMask;
    }

    void PinGameThread() { PinCurrent(g_gameMask); }
    void PinIoThread()   { PinCurrent(g_ioMask); }
}
