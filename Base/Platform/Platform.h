#pragma once
// ==========================================================================
// Platform — OS별로 갈리는 저수준 동작을 한곳에 격리하는 경계(seam)
//
//   게임·네트워크 로직은 이 헤더의 Platform::* 함수만 호출하고,
//   실제 구현은 OS가 #ifdef로 고른다. (Windows / Linux)
//   목표: windows.h 의존을 이 경계 뒤로 몰아, 나머지 코드가 플랫폼 중립이 되게.
//
//   [현재 입주자]
//     SetHighResolutionTimer — 시스템 타이머 해상도 상향 (Sleep/틱 정밀도).
//                              Windows=timeBeginPeriod / Linux=불필요(no-op).
//     GetExecutableDir       — 실행 파일 디렉토리 (설정 파일 경로 등).
//                              Windows=GetModuleFileNameA / Linux=/proc/self/exe.
//     SetProcessAffinity     — 프로세스 CPU 코어 고정. Win=SetProcessAffinityMask / Linux=sched_setaffinity.
//     GetAvailableCoreCount  — affinity 가용 코어 수(워커 자동 산정). Win=GetProcessAffinityMask / Linux=sched_getaffinity.
//     InstallShutdownHandler — Ctrl+C/SIGTERM → 콜백. Win=SetConsoleCtrlHandler / Linux=sigaction+폴링.
//                              창 닫기·로그오프는 정리 완료까지 핸들러가 버틴다(Windows 한정).
//     ThreadCpuHandle / CaptureCurrentThreadCpu / GetThreadCpuTimeNs
//                            — 스레드 CPU 시간(모니터링). Win=GetThreadTimes / Linux=clock_gettime(per-thread).
//
//   NOTE: 지금은 헤더 전용(inline). 입주자가 늘고 windows.h 격리가 중요해지면
//         선언/구현을 Platform.cpp로 분리해 windows.h를 단일 TU에 가둔다.
// ==========================================================================

#include <string>
#include <cstdint>
#include <thread>
#include <atomic>                        // 종료 플래그 (양쪽 공용)

#ifdef _WIN32
    // WinSock2는 Windows.h보다 먼저 와야 한다(구버전 winsock.h가 딸려와 sockaddr이 재정의된다).
    //   이 순서를 여기 한 곳에 가두어, 이 헤더를 쓰는 쪽은 순서를 신경 쓰지 않게 한다.
    //   WIN32_LEAN_AND_MEAN까지 함께 세우는 이유: 다른 TU가 이 헤더보다 먼저 <Windows.h>를
    //   들이면 그쪽에서 winsock.h가 딸려와 순서 보장이 무너진다. 아예 안 끌려오게 막는다.
    //   [주의] 여기서 WinSock2를 include하지 않는다. 이 헤더는 여러 곳에서 쓰이는데,
    //   그중 하나라도 <Windows.h>를 먼저 들인 TU가 있으면 구버전 winsock.h가 딸려와
    //   sockaddr이 재정의된다. Windows의 소켓 헤더 순서는 IOCPServer.h가 책임진다.
    #include <Windows.h>
    #include <timeapi.h>                 // timeBeginPeriod/timeEndPeriod
                                         //   WIN32_LEAN_AND_MEAN이 mmsystem.h를 빼므로 직접 들인다
    #pragma comment(lib, "winmm.lib")
#else
    #include <sys/socket.h>              // socket / bind / listen / accept / setsockopt
    #include <netinet/in.h>              // sockaddr_in / htons
    #include <netinet/tcp.h>             // TCP_NODELAY
    #include <arpa/inet.h>               // inet_ntop
    #include <cerrno>                    // errno (소켓 오류 코드)
    #include <cstring>                   // memset / memcpy (ZeroMemory·memcpy_s 대체)
    #include <cwchar>                    // wcslen

    // ── Windows 스칼라 타입·매크로 ──
    //   서버 코드가 이 이름들로 쓰여 있어(volatile LONG _ioCount 등) 호출부를 고치는 대신 별칭을 준다.
    //   폭은 Windows 정의를 그대로 따른다 — LONG은 64비트 리눅스에서도 32비트다(LP64의 long과 다름).
    using LONG      = std::int32_t;
    using LONGLONG  = long long;   // LockFreeCompat의 LONG64와 같은 타입이어야 한다(LP64에서 int64_t는 long)
    using LONG64    = long long;   // 〃 (모니터 카운터가 이 이름을 쓴다)
    using BOOL      = int;
    using ULONG_PTR = std::uintptr_t;
    using SOCKADDR    = struct sockaddr;
    using SOCKADDR_IN = struct sockaddr_in;
    using LINGER      = struct linger;

    // ── 32비트 원자연산 ──
    //   LockFreeCompat.h는 64비트·16비트만 덮는다(락프리 자료구조가 그 폭만 쓴다).
    //   서버 골격은 `volatile LONG`(32비트) 상태 플래그를 Interlocked로 다루므로 여기서 채운다.
    //   전부 "연산 후 값"을 돌려주는 Windows 계열 시맨틱이고, Exchange만 "이전 값"이다.
    inline LONG InterlockedIncrement(volatile LONG* p)
    {
        return __atomic_add_fetch(p, 1, __ATOMIC_SEQ_CST);
    }
    inline LONG InterlockedDecrement(volatile LONG* p)
    {
        return __atomic_sub_fetch(p, 1, __ATOMIC_SEQ_CST);
    }
    inline LONG InterlockedExchange(volatile LONG* p, LONG value)
    {
        return __atomic_exchange_n(p, value, __ATOMIC_SEQ_CST);   // 반환은 교환 "전" 값
    }
    inline LONG InterlockedCompareExchange(volatile LONG* p, LONG exchange, LONG comparand)
    {
        LONG expected = comparand;
        __atomic_compare_exchange_n(p, &expected, exchange, false,
                                    __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
        return expected;   // 성공이면 comparand, 실패면 관측된 실제 값 (Windows와 동일)
    }

    // 밀리초 대기 — Windows Sleep 대체
    #ifndef Sleep
        #define Sleep(ms) std::this_thread::sleep_for(std::chrono::milliseconds(ms))
    #endif

    // listen 백로그 힌트 — Windows 전용 매크로. 리눅스는 값을 그대로 쓴다.
    #ifndef SOMAXCONN_HINT
        #define SOMAXCONN_HINT(n) (n)
    #endif
    // ULONG/DWORD는 LockFreeCompat.h도 같은 폭(uint32_t)으로 정의한다.
    //   동일 타입 재선언은 합법이므로 어느 쪽이 먼저 include돼도 문제없다.
    using ULONG = std::uint32_t;
    using DWORD = std::uint32_t;

    // 소켓 핸들의 Windows 이름 — 서버 코드가 `SOCKET`으로 쓰여 있어 호출부를 그대로 둔다.
    //   실체는 Platform::NetSocket과 같은 fd다.
    using SOCKET = int;
    #ifndef INVALID_SOCKET
        #define INVALID_SOCKET (-1)
    #endif
    #ifndef SOCKET_ERROR
        #define SOCKET_ERROR (-1)
    #endif

    #ifndef TRUE
        #define TRUE  1
    #endif
    #ifndef FALSE
        #define FALSE 0
    #endif

    // 메모리 0 채우기 — Windows 매크로 대체
    #ifndef ZeroMemory
        #define ZeroMemory(dst, len) std::memset((dst), 0, (len))
    #endif

    // memcpy_s — MS 확장(대상 크기를 받아 넘침을 런타임에 잡는다). 리눅스엔 대응물이 없어
    //   크기 인자를 버리고 memcpy로 간다. 즉 "리눅스 빌드에서는 그 검사가 없다".
    //   호출부가 이미 범위를 계산해 넘기는 자리들이라 동작은 같지만, 방어 한 겹이 빠진다는
    //   사실은 남겨 둔다(넘침이 의심되면 ASan 빌드로 잡을 것).
    #ifndef memcpy_s
        #define memcpy_s(dst, dstSize, src, count) (std::memcpy((dst), (src), (count)), 0)
    #endif
    #include <unistd.h>                  // readlink (/proc/self/exe)
    #include <sched.h>                   // sched_setaffinity / CPU_SET
    #include <csignal>                   // sigaction (종료 시그널) · signal (SIGPIPE 무시)
    #include <chrono>                    // 폴링 슬립
    #include <pthread.h>                 // pthread_getcpuclockid (스레드 CPU clock)
    #include <time.h>                    // clock_gettime / clockid_t
#endif

namespace Platform
{
    // ── 소켓 핸들 ──
    //   Windows의 SOCKET은 UINT_PTR(부호 없는 포인터 크기), 리눅스는 파일 디스크립터(int)다.
    //   크기도 부호도 달라서 경계 시그니처에 그대로 두면 한쪽에서 반드시 깨진다.
    //
    //   Windows 쪽을 `SOCKET`이 아니라 `UINT_PTR`로 쓰는 이유: `SOCKET`은 <WinSock2.h>에 있는데
    //   그 헤더는 <Windows.h>보다 먼저 와야 한다는 순서 제약이 있다. 이 파일이 그 제약을
    //   퍼뜨리지 않도록, 같은 타입인 UINT_PTR(<Windows.h> 제공)로 정의한다.
    //   (winsock2.h 원문: typedef UINT_PTR SOCKET)
#ifdef _WIN32
    using NetSocket = UINT_PTR;
    inline constexpr NetSocket kInvalidSocket = static_cast<NetSocket>(~0);   // = INVALID_SOCKET
#else
    using NetSocket = int;
    inline constexpr NetSocket kInvalidSocket = -1;
#endif

    // ── 소켓 API ──
    //   Windows는 WSAStartup/WSACleanup 쌍이 필요하고, 리눅스는 SIGPIPE를 꺼야 한다.
    inline bool SocketStartup()
    {
#ifdef _WIN32
        WSADATA wsaData;
        return WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;
#else
        // 끊긴 소켓에 writev하면 SIGPIPE로 프로세스가 죽는다. 무시로 바꾸면 EPIPE가 반환되고
        // 그 처리는 이미 있다(Transport_Epoll.cpp의 writev 실패 경로).
        //   그동안은 httplib(모니터) 생성자가 대신 해줬다 — MonitorEnabled=0이면 무방비였다.
        ::signal(SIGPIPE, SIG_IGN);
        return true;
#endif
    }

    inline void SocketCleanup()
    {
#ifdef _WIN32
        WSACleanup();
#endif
    }

    // 소켓 닫기 — Windows closesocket / 리눅스 close.
    inline void CloseSocket(NetSocket s)
    {
#ifdef _WIN32
        closesocket(s);
#else
        ::close(s);
#endif
    }

    // 마지막 소켓 오류 코드. 값 자체는 OS별로 다르므로 "로그에 남기는 용도"로만 쓴다.
    //   두 OS의 상수를 맞추려 들면 의미가 어긋난다(WSAEWOULDBLOCK vs EAGAIN 등) — 분기가 필요한
    //   자리는 아래 WouldBlock() 같은 판정 함수를 따로 두는 쪽이 안전하다.
    inline int LastSocketError()
    {
#ifdef _WIN32
        return WSAGetLastError();
#else
        return errno;
#endif
    }

    // "지금은 보낼/받을 수 없다"는 뜻인가 — 논블로킹 경로의 정상 상태다.
    inline bool WouldBlock(int err)
    {
#ifdef _WIN32
        return err == WSAEWOULDBLOCK;
#else
        return err == EAGAIN || err == EWOULDBLOCK;
#endif
    }

    // 시스템 타이머 해상도를 1ms로 올리거나(enable=true) 되돌린다(false).
    //   Windows: timeBeginPeriod/timeEndPeriod(1) — Sleep·대기 정밀도 ~15ms → 1ms.
    //   Linux  : nanosleep 계열이 이미 고해상도라 불필요 → no-op.
    //   enable/disable는 반드시 쌍으로 호출 (중첩 카운트는 OS가 관리).
    inline void SetHighResolutionTimer(bool enable)
    {
#ifdef _WIN32
        if (enable)
            timeBeginPeriod(1);
        else
            timeEndPeriod(1);
#else
        (void)enable;
#endif
    }

    // 실행 파일이 위치한 디렉토리 (끝에 경로 구분자 포함). 실패 시 "".
    //   Windows: GetModuleFileNameA (CP_ACP narrow — ifstream이 여는 인코딩과 일치)
    //   Linux  : readlink("/proc/self/exe")
    inline std::string GetExecutableDir()
    {
#ifdef _WIN32
        char buf[MAX_PATH];
        DWORD n = GetModuleFileNameA(nullptr, buf, MAX_PATH);
        if (n == 0 || n >= MAX_PATH)
            return std::string();
        std::string path(buf, n);
#else
        char buf[4096];
        ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
        if (n <= 0)
            return std::string();
        std::string path(buf, static_cast<size_t>(n));
#endif
        size_t pos = path.find_last_of("/\\");
        if (pos == std::string::npos)
            return std::string();
        return path.substr(0, pos + 1);   // 구분자 포함
    }

    // 프로세스를 지정한 논리코어 비트마스크에 고정. 성공 시 true.
    //   Windows: SetProcessAffinityMask / Linux: sched_setaffinity(CPU_SET)
    inline bool SetProcessAffinity(uint64_t mask)
    {
#ifdef _WIN32
        return SetProcessAffinityMask(GetCurrentProcess(),
                                      static_cast<DWORD_PTR>(mask)) != 0;
#else
        cpu_set_t set;
        CPU_ZERO(&set);
        for (int i = 0; i < 64; ++i)
            if (mask & (1ull << i))
                CPU_SET(i, &set);
        return sched_setaffinity(0, sizeof(set), &set) == 0;
#endif
    }

    // 호출 스레드를 지정한 논리코어 비트마스크에 고정.
    //   Windows: SetThreadAffinityMask(의사핸들이면 자기 자신) / Linux: pthread_setaffinity_np
    inline void SetCurrentThreadAffinity(uint64_t mask)
    {
#ifdef _WIN32
        SetThreadAffinityMask(GetCurrentThread(), static_cast<DWORD_PTR>(mask));
#else
        cpu_set_t set;
        CPU_ZERO(&set);
        for (int i = 0; i < 64; ++i)
            if (mask & (1ull << i))
                CPU_SET(i, &set);
        pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#endif
    }

    // affinity로 제한된 가용 논리코어 수. 미제한/불명이면 하드웨어 논리코어 수로 폴백.
    //   (worker 스레드 자동 산정용 — affinity 설정 이후 호출해야 정확)
    inline int GetAvailableCoreCount()
    {
#ifdef _WIN32
        DWORD_PTR procMask = 0, sysMask = 0;
        if (GetProcessAffinityMask(GetCurrentProcess(), &procMask, &sysMask) && procMask != 0)
        {
            int count = 0;
            for (DWORD_PTR m = procMask; m != 0; m &= (m - 1))
                ++count;
            if (count > 0)
                return count;
        }
#else
        cpu_set_t set;
        CPU_ZERO(&set);
        if (sched_getaffinity(0, sizeof(set), &set) == 0)
        {
            int count = CPU_COUNT(&set);
            if (count > 0)
                return count;
        }
#endif
        return static_cast<int>(std::thread::hardware_concurrency());
    }

    // 종료 시그널(Ctrl+C / SIGINT·SIGTERM) 수신 시 cb를 "정상 스레드 컨텍스트"에서 1회 호출.
    //   Windows: SetConsoleCtrlHandler (핸들러가 OS 주입 스레드에서 실행 → cb의 mutex/cv 안전)
    //   Linux  : sigaction으로 플래그만 세팅(async-signal-safe) + 폴링 스레드가 cb 호출.
    //            시그널 컨텍스트에서 cb 직접 호출 금지 — cb 내부 mutex/cv는 비동기시그널 안전이 아님.
    //   NOTE: Linux 경로는 리눅스 빌드 환경 전이라 미검증.
    namespace detail
    {
        inline void (*g_shutdownCb)() = nullptr;
        inline const std::atomic<bool>* g_shutdownComplete = nullptr;   // 정리 완료 신호 (nullptr=대기 안 함)
        inline uint32_t g_shutdownWaitMaxMs = 0;
#ifdef _WIN32
        inline BOOL WINAPI ConsoleCtrlProxy(DWORD ctrlType)
        {
            switch (ctrlType)
            {
            case CTRL_C_EVENT:
            case CTRL_BREAK_EVENT:
                // 이 둘은 핸들러가 반환해도 프로세스가 계속 살아 있다 → main이 Stop()을 완주할 수 있다.
                if (g_shutdownCb) g_shutdownCb();
                return TRUE;

            case CTRL_CLOSE_EVENT:
            case CTRL_SHUTDOWN_EVENT:
            case CTRL_LOGOFF_EVENT:
                // 이 셋은 핸들러가 반환하는 순간 OS가 프로세스를 종료한다.
                //   그냥 반환하면 main의 server.Stop()(SaveAllPlayers + DB 드레인)이 시작도 못 하고 잘려
                //   마지막 저장 주기 이후의 플레이어 위치가 통째로 유실된다.
                //   → 정리가 끝날 때까지 여기서 버틴다. 유예를 넘기면 어차피 OS가 죽이므로 포기하고 반환.
                if (g_shutdownCb) g_shutdownCb();
                if (g_shutdownComplete)
                {
                    const ULONGLONG deadline = GetTickCount64() + g_shutdownWaitMaxMs;
                    while (!g_shutdownComplete->load(std::memory_order_acquire))
                    {
                        if (GetTickCount64() >= deadline)
                            break;
                        Sleep(20);
                    }
                }
                return TRUE;

            default:
                return FALSE;
            }
        }
#else
        inline std::atomic<bool> g_shutdownFlag{ false };
        inline void SignalSetFlag(int) { g_shutdownFlag.store(true, std::memory_order_relaxed); }
#endif
    }

    // completeFlag/waitMaxMs — 창 닫기·로그오프처럼 "핸들러가 반환하면 OS가 즉시 죽이는" 경로에서
    //   정리 완료를 기다릴 대상과 상한(ms). Windows 전용이며 nullptr이면 대기하지 않는다.
    //   Linux는 SIGINT/SIGTERM이 와도 프로세스가 바로 죽지 않아(main이 스스로 빠져나온다) 대기가 불필요.
    inline void InstallShutdownHandler(void (*cb)(),
                                       const std::atomic<bool>* completeFlag = nullptr,
                                       uint32_t waitMaxMs = 0)
    {
        detail::g_shutdownCb = cb;
        detail::g_shutdownComplete = completeFlag;
        detail::g_shutdownWaitMaxMs = waitMaxMs;
#ifdef _WIN32
        SetConsoleCtrlHandler(detail::ConsoleCtrlProxy, TRUE);
#else
        struct sigaction sa {};
        sa.sa_handler = detail::SignalSetFlag;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGINT,  &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
        // 폴링 스레드: 플래그가 서면 정상 컨텍스트에서 cb 호출 (cv.notify 안전)
        std::thread([]
        {
            while (!detail::g_shutdownFlag.load(std::memory_order_relaxed))
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (detail::g_shutdownCb) detail::g_shutdownCb();
        }).detach();
#endif
    }

    // ── 스레드 CPU 시간 측정 (모니터링) ──
    //   각 스레드가 시작 시 CaptureCurrentThreadCpu로 자기 핸들을 등록하고,
    //   외부 관측 스레드가 GetThreadCpuTimeNs로 그 스레드의 누적 CPU 시간(ns)을 읽는다.
    //   Windows: 스레드 실핸들(DuplicateHandle) + GetThreadTimes
    //   Linux  : per-thread CPU clock(pthread_getcpuclockid) + clock_gettime
#ifdef _WIN32
    using ThreadCpuHandle = HANDLE;
    inline constexpr ThreadCpuHandle kInvalidThreadCpuHandle = nullptr;
#else
    using ThreadCpuHandle = clockid_t;
    inline constexpr ThreadCpuHandle kInvalidThreadCpuHandle = static_cast<clockid_t>(-1);
#endif

    // 호출 스레드의 CPU 시간 측정 핸들을 캡처. 실패 시 kInvalidThreadCpuHandle.
    //   수명은 프로세스 종료까지 — Windows 복제 핸들은 명시적 Close 생략(진단용, 기존 동작 보존).
    inline ThreadCpuHandle CaptureCurrentThreadCpu()
    {
#ifdef _WIN32
        HANDLE dup = nullptr;
        if (DuplicateHandle(GetCurrentProcess(), GetCurrentThread(),
                            GetCurrentProcess(), &dup, 0, FALSE, DUPLICATE_SAME_ACCESS))
            return dup;
        return nullptr;
#else
        clockid_t cid;
        if (pthread_getcpuclockid(pthread_self(), &cid) == 0)
            return cid;
        return static_cast<clockid_t>(-1);
#endif
    }

    // 핸들이 가리키는 스레드의 누적 CPU 시간(ns, 커널+유저)을 out에 넣고 true. 실패/미등록이면 false.
    inline bool GetThreadCpuTimeNs(ThreadCpuHandle h, uint64_t& outNs)
    {
#ifdef _WIN32
        if (h == nullptr)
            return false;
        FILETIME ftCreate, ftExit, ftKernel, ftUser;
        if (!GetThreadTimes(h, &ftCreate, &ftExit, &ftKernel, &ftUser))
            return false;
        auto u64 = [](const FILETIME& ft) {
            return (static_cast<uint64_t>(ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
        };
        outNs = (u64(ftKernel) + u64(ftUser)) * 100;   // FILETIME 100ns 단위 → ns
        return true;
#else
        if (h == static_cast<clockid_t>(-1))
            return false;
        struct timespec ts;
        if (clock_gettime(h, &ts) != 0)
            return false;
        outNs = static_cast<uint64_t>(ts.tv_sec) * 1000000000ull
              + static_cast<uint64_t>(ts.tv_nsec);
        return true;
#endif
    }

    // 위와 같되 그중 커널모드 CPU(syscall 실행분)를 따로 돌려주는 변형 — 모니터링의 kernel_ratio용.
    //   Linux: per-thread CPU clock이 user/kernel을 나눠주지 않아 커널분은 0으로 둔다.
    //          (분리가 필요해지면 /proc/self/task/<tid>/stat의 utime/stime을 읽어야 한다)
    inline bool GetThreadCpuTimeNs(ThreadCpuHandle h, uint64_t& outNs, uint64_t& outKernelNs)
    {
#ifdef _WIN32
        if (h == nullptr)
            return false;
        FILETIME ftCreate, ftExit, ftKernel, ftUser;
        if (!GetThreadTimes(h, &ftCreate, &ftExit, &ftKernel, &ftUser))
            return false;
        auto u64 = [](const FILETIME& ft) {
            return (static_cast<uint64_t>(ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
        };
        outKernelNs = u64(ftKernel) * 100;                    // FILETIME 100ns 단위 → ns
        outNs       = outKernelNs + u64(ftUser) * 100;
        return true;
#else
        outKernelNs = 0;
        return GetThreadCpuTimeNs(h, outNs);
#endif
    }
}
