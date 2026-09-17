#pragma once

#include <iostream>
#include <sstream>
#include <string>

namespace shared
{
inline bool ShouldIgnoreWsaError(int errorCode)
{
    switch (errorCode)
    {
    // Per-IO방식 사용 시 나올수있는 에러
    case 10004: // WSAEINTR : 사용자쪽에서 진행중인 I/O를 제거 (closesocket / CancelIO)
    case 10038: // WSAENOTSOCK: 유효하지 않은 소켓 핸들 (이미 close되었거나 잘못된 값)

    case 10053: // WSAECONNABORTED: 소프트웨어적으로 연결 중단 (타임아웃, 프로토콜 오류 등)
    case 10054: // WSAECONNRESET: 상대방이 연결을 강제로 끊음 (RST 수신)
    case 10064: // WSAEHOSTDOWN: 상대 호스트가 다운됨
        return true;
    default:
        return false;
    }
}

inline void LogError(const std::string& message)
{
    std::cerr << message << std::endl;
}

inline void LogError(const std::wstring& message)
{
    std::wcerr << message << std::endl;
}
}

// ==========================================================================
// USE_SPDLOG_LOGGER 분기 — MMOServer에만 정의됨
// ==========================================================================
#ifdef USE_SPDLOG_LOGGER

#include "Logger.h"

// ---- SLOG_* 매크로 (직접 spdlog 호출) ----
#define SLOG_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define SLOG_INFO(...)  SPDLOG_INFO(__VA_ARGS__)
#define SLOG_WARN(...)  SPDLOG_WARN(__VA_ARGS__)
#define SLOG_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)

// ---- 기존 LOG_ERROR_STREAM → spdlog 브릿지 ----
// ostringstream으로 조립 후 SLOG_ERROR로 출력 (source location 전달)
#define LOG_ERROR_STREAM(expr) \
    do \
    { \
        std::ostringstream _logErrorOss; \
        _logErrorOss << expr; \
        SLOG_ERROR("{}", _logErrorOss.str()); \
    } while (0)

#define WLOG_ERROR_STREAM(expr) \
    do \
    { \
        std::wostringstream _logErrorOss; \
        _logErrorOss << expr; \
        ::shared::LogError(_logErrorOss.str()); \
    } while (0)

// ---- LOG_WSA_ERROR_STREAM → ShouldIgnoreWsaError 유지, 레벨 세분화 ----
// 초기화 단계(WSAStartup, bind, listen 등) 실패 = ERROR
// 런타임 Per-IO 에러 = WARN
#define LOG_WSA_ERROR_STREAM(expr, errCodeExpr) \
    do \
    { \
        const int _wsaErr = (errCodeExpr); \
        if (!::shared::ShouldIgnoreWsaError(_wsaErr)) \
        { \
            std::ostringstream _logErrorOss; \
            _logErrorOss << expr << _wsaErr; \
            SLOG_WARN("{}", _logErrorOss.str()); \
        } \
    } while (0)

#define WLOG_WSA_ERROR_STREAM(expr, errCodeExpr) \
    do \
    { \
        const int _wsaErr = (errCodeExpr); \
        if (!::shared::ShouldIgnoreWsaError(_wsaErr)) \
        { \
            std::wostringstream _logErrorOss; \
            _logErrorOss << expr << _wsaErr; \
            ::shared::LogError(_logErrorOss.str()); \
        } \
    } while (0)

// ==========================================================================
// USE_SPDLOG_LOGGER 미정의 — 기존 동작 유지 (GameClient 등)
// ==========================================================================
#else

// SLOG_* 매크로 — spdlog 없이는 no-op
#define SLOG_DEBUG(...) ((void)0)
#define SLOG_INFO(...)  ((void)0)
#define SLOG_WARN(...)  ((void)0)
#define SLOG_ERROR(...) ((void)0)

#define LOG_ERROR_STREAM(expr) \
    do \
    { \
        std::ostringstream _logErrorOss; \
        _logErrorOss << expr; \
        ::shared::LogError(_logErrorOss.str()); \
    } while (0)

#define WLOG_ERROR_STREAM(expr) \
    do \
    { \
        std::wostringstream _logErrorOss; \
        _logErrorOss << expr; \
        ::shared::LogError(_logErrorOss.str()); \
    } while (0)

#define LOG_WSA_ERROR_STREAM(expr, errCodeExpr) \
    do \
    { \
        const int _wsaErr = (errCodeExpr); \
        if (!::shared::ShouldIgnoreWsaError(_wsaErr)) \
        { \
            std::ostringstream _logErrorOss; \
            _logErrorOss << expr << _wsaErr; \
            ::shared::LogError(_logErrorOss.str()); \
        } \
    } while (0)

#define WLOG_WSA_ERROR_STREAM(expr, errCodeExpr) \
    do \
    { \
        const int _wsaErr = (errCodeExpr); \
        if (!::shared::ShouldIgnoreWsaError(_wsaErr)) \
        { \
            std::wostringstream _logErrorOss; \
            _logErrorOss << expr << _wsaErr; \
            ::shared::LogError(_logErrorOss.str()); \
        } \
    } while (0)

#endif // USE_SPDLOG_LOGGER
