#pragma once

// ==========================================================================
// spdlog 비동기 로거 래퍼
// MMOServer 전용 — USE_SPDLOG_LOGGER 정의 시에만 실제 동작
// ==========================================================================

#ifdef USE_SPDLOG_LOGGER

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <string>
#include <memory>
#include <vector>
#include <chrono>
#include <ctime>

namespace shared
{

class Logger
{
public:
    // 로거 초기화 — main() 시작 직후 호출
    // logDir: 로그 디렉토리 (기본 "logs")
    // 파일명 자동 생성: {YYMMDD}_{프로세스명}.log (예: 260522_MMOServer.log)
    static void Init(const std::string& logDir = "logs");

    // 로거 종료 — 프로세스 종료 전 호출 (flush + drop)
    static void Shutdown();

    // 기본 로거 접근
    static std::shared_ptr<spdlog::logger>& Get();

private:
    static std::shared_ptr<spdlog::logger> _logger;
};

// RAII guard — main()에서 사용
class LoggerGuard
{
public:
    LoggerGuard(const std::string& logDir = "logs")
    {
        Logger::Init(logDir);
    }
    ~LoggerGuard()
    {
        Logger::Shutdown();
    }

    LoggerGuard(const LoggerGuard&) = delete;
    LoggerGuard& operator=(const LoggerGuard&) = delete;
};

} // namespace shared

#endif // USE_SPDLOG_LOGGER
