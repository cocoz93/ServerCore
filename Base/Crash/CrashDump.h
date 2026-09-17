#pragma once

// 크래시 덤프 — Windows: SEH + MiniDump(원본 보존) / Linux: sigaction + backtrace(아래 #else)
// 공개 표면 동일: 전역 CrashDump 인스턴스 + CRASH(reason) 매크로 + _CrashReason
#ifdef _WIN32
#pragma comment(lib, "DbgHelp")	//MiniDumpWriteDump

#include <stdio.h>
#include <signal.h>		//signal, SIGABRT
#include <new.h>		//_set_new_handler
#include <windows.h>
#include <DbgHelp.h>	//_MINIDUMP_EXCEPTION_INFORMATION
#include <crtdbg.h>		//_CrtSetReportMode


class CCrashDump
{
public:
	// 크래시 사유 저장 — 덤프에서 조회 가능
	inline static const wchar_t* _CrashReason = nullptr;

	CCrashDump()
	{
		//밑 구문들은 CRT오류 메시지표시를 중단하기 위함.
		//우리는 덤프를 빼고 프로세스를 종료시켜야 한다.

		//-------------------------------------------------------------------------------

		// INVALIDE_PARAMETER 에러핸들러를 우리가 캐치
		// ex) CRT함수에 잘못된 인자전달 (매개변수 가변인자 오류, NULL이 들어갈수 없는곳에 NULL)
		_set_invalid_parameter_handler(myInvalidParameterHandler);

		// pure virtual function called 에러핸들러를 우리가 캐치
		_set_purecall_handler(MyPurecallHandler);

		// C++ 미처리 예외의 최종 종착점 (throw 후 catch 없을 때)
		set_terminate(MyTerminateHandler);

		// operator new 실패 시 핸들러 (bad_alloc 대신 우리가 처리)
		_set_new_handler(MyNewHandler);
		_set_new_mode(1);

		// abort() 호출 시 메시지 박스 / WER 보고를 차단 → 바로 우리 핸들러로 진입
		_set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);

		// SIGABRT 시그널 캐치 (abort, assert 등이 발생시킴)
		signal(SIGABRT, MySigAbrtHandler);

		//CRT 디버그 보고 모드를 모두 끔 (경험한 적은 없으나 만약을 위함)
		_CrtSetReportMode(_CRT_WARN, 0);
		_CrtSetReportMode(_CRT_ASSERT, 0);
		_CrtSetReportMode(_CRT_ERROR, 0);
		_CrtSetReportHook(_custom_Report_hook);


		// 핸들링되지 않은 모든 예외를 우리쪽으로 캐치
		// ex) Throw 던졌는데 Catch존재하지않음, 메모리참조 오류, 모든예외를 받는 경우 등
		// 원래는 catch를 못받는 경우 메인까지 튀어나와 SEH로 예외발생
		SetUnhandledExceptionFilter(MyExceptionFilter);

		// CRT가 내부적으로 SetUnhandledExceptionFilter(NULL)을 호출해 우리 핸들러를 덮어쓰는 것을 차단.
		// kernel32.dll의 SetUnhandledExceptionFilter 진입점 첫 바이트를 ret으로 패치한다.
		DisableSetUnhandledExceptionFilter();

		// 덤프 저장 디렉토리 사전 생성 (실행 파일 옆 CrashDump 폴더)
		CreateDumpDirectory();

		//-------------------------------------------------------------------------------
	}

	CCrashDump(const CCrashDump&) = delete;
	CCrashDump& operator=(const CCrashDump&) = delete;


	// 의도적 크래시 — CRASH() 매크로 사용을 권장 (사유 기록)
	static void Crash(void)
	{
		__debugbreak();
	}



	/*
	우리가 정의한 MyExceptionFilter는 생성자 내부에서,
	API함수인 SetUnhandledExceptionFilter()의 매개변수로 전달될 함수.

	이 함수의 매개변수 EXCEPTION_POINTERS은 규칙을 따른것.
	ExceptionFilter라는 구조체의 포인터를 여기서 받으면 된다.
	그럼 예외발생시 이 인자로 자동으로 들어올 것이다.
	우리는 여기서 덤프를 여기서 뺀다.
	*/
	/*------------------------------------------------------------------
	  2차 크래시 안전성 원칙
	  ─────────────────────
	  1) CRT 함수 사용 금지 (힙 손상 시 2차 크래시/데드락 유발)
	     → wsprintfW/A(kernel32), WriteFile, Win32 API만 사용

	  2) 작업 순서: 덤프 생성 → 콘솔 출력
	     → 출력 중 2차 예외가 나도 덤프는 이미 확보

	  3) __try/__except로 필터 전체를 감싼다
	     → 필터 내부 2차 예외 시 OS가 프로세스를 즉시 죽이는 것을 방지
	     → 어떤 상황에서든 TerminateProcess까지 도달 보장
	------------------------------------------------------------------*/
	static LONG WINAPI MyExceptionFilter(PEXCEPTION_POINTERS pExceptionPointer)
	{
		long DumpCount = InterlockedIncrement(&_DumpCount);

		// 동시 다발 크래시 시 첫 번째 스레드만 덤프를 생성한다.
		// 나머지 스레드는 프로세스 종료를 대기.
		if (DumpCount > 1)
		{
			Sleep(INFINITE);
			return EXCEPTION_EXECUTE_HANDLER;
		}

		// 필터 내부 2차 예외 방어: 어떤 예외가 나도 TerminateProcess까지 도달
		__try
		{
			//----------------------------------------------------------
			// 덤프 파일명 생성 (wsprintfW = kernel32, CRT 미사용으로 안전)
			//----------------------------------------------------------
			SYSTEMTIME st;
			GetLocalTime(&st);

			WCHAR filename[MAX_PATH];
			wsprintfW(filename, L"%sDump_%d%02d%02d_%02d.%02d.%02d.dmp",
				_dumpDir,
				st.wYear, st.wMonth, st.wDay,
				st.wHour, st.wMinute, st.wSecond);


			//----------------------------------------------------------
			// [최우선] 덤프 파일 생성 — 이후 어떤 실패가 와도 덤프는 확보
			//----------------------------------------------------------
			HANDLE hDumpFile = CreateFileW(
				filename,
				GENERIC_WRITE,
				0,
				NULL,
				CREATE_ALWAYS,
				FILE_ATTRIBUTE_NORMAL, NULL);

			if (hDumpFile != INVALID_HANDLE_VALUE)
			{
				_MINIDUMP_EXCEPTION_INFORMATION mei;
				mei.ThreadId = GetCurrentThreadId();
				mei.ExceptionPointers = pExceptionPointer;
				mei.ClientPointers = FALSE;

				MINIDUMP_TYPE dumpType = (MINIDUMP_TYPE)(
					MiniDumpWithFullMemory          |	// 전체 프로세스 메모리
					MiniDumpWithFullMemoryInfo       |	// 메모리 영역별 상세 정보 (주소, 크기, 보호 속성)
					MiniDumpWithHandleData           |	// 열린 핸들 목록 (파일, 소켓, 이벤트 등)
					MiniDumpWithThreadInfo           |	// 스레드 시작 주소, 선호도, 우선순위 등
					MiniDumpWithUnloadedModules      |	// 언로드된 DLL 목록 (DLL 언로드 후 해당 코드 크래시 추적용)
					MiniDumpWithProcessThreadData);		// TLS, PEB, TEB 등 프로세스/스레드 내부 데이터

				MiniDumpWriteDump(
					GetCurrentProcess(),
					GetCurrentProcessId(),
					hDumpFile,
					dumpType,
					&mei,
					NULL,
					NULL);

				CloseHandle(hDumpFile);
			}


			//----------------------------------------------------------
			// 콘솔 출력 (WriteFile + wsprintfA = CRT 완전 우회)
			// 덤프는 이미 저장됐으므로, 여기서 실패해도 문제없음
			//----------------------------------------------------------
			HANDLE hStdErr = GetStdHandle(STD_ERROR_HANDLE);
			if (hStdErr != INVALID_HANDLE_VALUE)
			{
				char msg[512];
				DWORD written;
				int len;

				len = wsprintfA(msg,
					"\r\n\r\n!!! Crash Error !!! %d.%02d.%02d / %02d:%02d:%02d\r\n",
					st.wYear, st.wMonth, st.wDay,
					st.wHour, st.wMinute, st.wSecond);
				WriteFile(hStdErr, msg, len, &written, NULL);

				if (_CrashReason != nullptr)
				{
					char reasonBuf[256];
					int converted = WideCharToMultiByte(CP_UTF8, 0,
						_CrashReason, -1, reasonBuf, sizeof(reasonBuf) - 1, NULL, NULL);
					if (converted > 0)
					{
						len = wsprintfA(msg, "Crash Reason: %s\r\n", reasonBuf);
						WriteFile(hStdErr, msg, len, &written, NULL);
					}
				}
			}
		}
		__except (EXCEPTION_EXECUTE_HANDLER)
		{
			// 2차 예외 발생 — 덤프가 저장됐든 안 됐든 여기로 온다.
			// 아무것도 하지 않고 아래 TerminateProcess로 낙하.
		}

		// 어떤 경로든 반드시 프로세스 종료
		TerminateProcess(GetCurrentProcess(), 1);

		return EXCEPTION_EXECUTE_HANDLER;
	}



	static void myInvalidParameterHandler(const wchar_t* expression, const wchar_t* function, const wchar_t* file, unsigned int line, uintptr_t pReserved)
	{
		Crash();
	}

	static int _custom_Report_hook(int ireposttype, char* message, int* returnval)
	{
		Crash();
		return 1;
	}

	static void MyPurecallHandler(void)
	{
		Crash();
	}

	static void __cdecl MyTerminateHandler(void)
	{
		Crash();
	}

	static int __cdecl MyNewHandler(size_t size)
	{
		Crash();
		return 0;
	}

	static void MySigAbrtHandler(int sig)
	{
		// SIGABRT 재진입 방지: 핸들러 안에서 다시 abort가 호출될 수 있으므로 재등록
		signal(SIGABRT, MySigAbrtHandler);
		Crash();
	}

private:
	// CRT 내부가 SetUnhandledExceptionFilter(NULL)을 호출해 우리 핸들러를 제거하는 것을 차단.
	// 함수 진입점의 첫 바이트들을 "xor eax, eax; ret" 또는 "ret 4"(x86)로 패치하여 무력화.
	static void DisableSetUnhandledExceptionFilter(void)
	{
		void* pFunc = (void*)GetProcAddress(GetModuleHandleW(L"kernel32.dll"), "SetUnhandledExceptionFilter");
		if (pFunc == nullptr)
			return;

		DWORD dwOldProtect;
#if defined(_WIN64)
		// x64: xor eax, eax (31 C0) + ret (C3) = 3바이트
		BYTE patch[] = { 0x31, 0xC0, 0xC3 };
#else
		// x86 __stdcall: mov eax, 0 (B8 00 00 00 00) + ret 4 (C2 04 00) = 8바이트
		BYTE patch[] = { 0xB8, 0x00, 0x00, 0x00, 0x00, 0xC2, 0x04, 0x00 };
#endif

		if (VirtualProtect(pFunc, sizeof(patch), PAGE_EXECUTE_READWRITE, &dwOldProtect))
		{
			memcpy(pFunc, patch, sizeof(patch));
			VirtualProtect(pFunc, sizeof(patch), dwOldProtect, &dwOldProtect);
		}
	}

	// 실행 파일 옆에 CrashDump 디렉토리를 생성하고 경로를 저장
	static void CreateDumpDirectory()
	{
		GetModuleFileNameW(NULL, _dumpDir, MAX_PATH);
		WCHAR* lastSlash = wcsrchr(_dumpDir, L'\\');
		if (lastSlash)
			*(lastSlash + 1) = L'\0';
		wcscat_s(_dumpDir, L"CrashDump\\");
		CreateDirectoryW(_dumpDir, NULL);
	}


	inline static long _DumpCount = 0;
	inline static WCHAR _dumpDir[MAX_PATH] = {};
};

// 전역 인스턴스. inline으로 다중 TU에서 include해도 단일 정의 보장 (C++17)
inline CCrashDump CrashDump;

// 의도적 크래시 매크로: 사유만 기록하고 __debugbreak()
// 콘솔 출력은 MyExceptionFilter가 안전하게 처리 (CRT 미사용)
#define CRASH(reason)                                           \
    do {                                                        \
        CCrashDump::_CrashReason = L##reason;                   \
        __debugbreak();                                         \
    } while(0)

#else  // ===================== Linux =====================

#include <cstdlib>      // abort
#include <csignal>      // sigaction, raise, signal
#include <execinfo.h>   // backtrace, backtrace_symbols_fd
#include <unistd.h>     // write, close, STDERR_FILENO
#include <fcntl.h>      // open

// 치명 시그널(SIGSEGV/ABRT/FPE/ILL/BUS)을 잡아 백트레이스를 stderr + crashdump.txt로 남기고
// 기본 동작으로 재발생시켜 종료(코어덤프). backtrace_symbols_fd는 malloc을 안 해 핸들러에서 비교적 안전.
//   NOTE: 리눅스 빌드 환경 전이라 미검증(best-effort). 심볼 이름은 링크 시 -rdynamic 필요.
class CCrashDump
{
public:
    inline static const wchar_t* _CrashReason = nullptr;

    CCrashDump()
    {
        void* warm[4];
        backtrace(warm, 4);   // libgcc 예열 → 핸들러 내 최초 malloc 회피

        struct sigaction sa {};
        sa.sa_sigaction = &CCrashDump::SignalHandler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_SIGINFO;

        const int sigs[] = { SIGSEGV, SIGABRT, SIGFPE, SIGILL, SIGBUS };
        for (int s : sigs)
            sigaction(s, &sa, nullptr);
    }

    CCrashDump(const CCrashDump&) = delete;
    CCrashDump& operator=(const CCrashDump&) = delete;

    static void Crash(void) { ::abort(); }   // 의도적 크래시 → SIGABRT 핸들러 경유

private:
    static void SignalHandler(int sig, siginfo_t*, void*)
    {
        void* frames[64];
        int n = backtrace(frames, 64);

        WriteReport(STDERR_FILENO, sig, frames, n);

        int fd = ::open("crashdump.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0)
        {
            WriteReport(fd, sig, frames, n);
            ::close(fd);
        }

        ::signal(sig, SIG_DFL);   // 기본 동작으로 재발생 → 코어덤프/종료
        ::raise(sig);
    }

    // 시그널 핸들러 안이라 write 실패를 복구할 길이 없다 — 반환값을 의도적으로 버린다.
    //   그냥 두면 glibc의 warn_unused_result 때문에 호출마다 경고가 쌓여, 정작 봐야 할
    //   새 경고가 묻힌다. 버린다는 뜻을 코드로 남긴다.
    static void WriteRaw(int fd, const void* buf, size_t len)
    {
        const ssize_t written = ::write(fd, buf, len);
        (void)written;
    }

    // async-signal-safe 범위(write / backtrace_symbols_fd / 수동 정수변환)만 사용
    static void WriteReport(int fd, int sig, void* const* frames, int n)
    {
        WriteRaw(fd, "\n!!! Crash: signal ", 19);
        char num[16];
        WriteRaw(fd, num, IntToStr(sig, num));
        WriteRaw(fd, " !!!\n", 5);

        if (_CrashReason)
        {
            char rbuf[256];
            int i = 0;
            for (; _CrashReason[i] && i < 255; ++i)
                rbuf[i] = static_cast<char>(_CrashReason[i]);   // ASCII 사유 가정
            WriteRaw(fd, "Reason: ", 8);
            WriteRaw(fd, rbuf, i);
            WriteRaw(fd, "\n", 1);
        }

        backtrace_symbols_fd(frames, n, fd);
    }

    static int IntToStr(int v, char* out)
    {
        char tmp[16];
        int t = 0;
        unsigned u = (v < 0) ? static_cast<unsigned>(-v) : static_cast<unsigned>(v);
        do { tmp[t++] = static_cast<char>('0' + u % 10); u /= 10; } while (u);
        int o = 0;
        if (v < 0) out[o++] = '-';
        while (t > 0) out[o++] = tmp[--t];
        return o;
    }
};

// 전역 인스턴스 (생성자에서 시그널 핸들러 설치)
inline CCrashDump CrashDump;

// 의도적 크래시: 사유 기록 후 abort() → SIGABRT 핸들러가 덤프
#define CRASH(reason)                                           \
    do {                                                        \
        CCrashDump::_CrashReason = L##reason;                   \
        ::abort();                                              \
    } while(0)

#endif  // _WIN32
