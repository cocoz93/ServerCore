#pragma once

// ==========================================================================
// RioApi — Windows Registered I/O(RIO) 함수 테이블 로더 + 등록 슬랩 RAII
// (USE_RIO_TRANSPORT 경로 전용 — IOCP 경로에서는 include되지 않는다)
//
//  - CRioApi : WSAIoctl로 RIO 확장 함수 테이블을 프로세스 1회 획득.
//              이후 CRioApi::Rio().RIOSend(...) 형태로 호출한다.
//  - CRioSlab: VirtualAlloc 슬랩 + RIORegisterBuffer 1회 등록/해제 묶음.
//              세션 링버퍼는 이 슬랩의 슬라이스(RingBuffer::InitExternal)로 배치하고,
//              RIO_BUF.Offset은 OffsetOf(포인터)로 계산한다.
//              등록 = 물리 메모리 고정(스왑 불가) — 크기는 MaxClients × 링 2개.
//
//  주의(Phase 0 스모크 실측, 2026-07-20):
//   · WSA_FLAG_REGISTERED_IO 소켓은 RIO 전용 — 일반 WSASend/WSARecv가
//     WSAENOTSOCK(10038)으로 거부된다. RIO 소켓에는 반드시 RIO 함수만 쓸 것.
//   · accept()로 받은 소켓은 리슨 소켓의 REGISTERED_IO 플래그를 상속한다.
//   · closesocket 시 pending RIO 요청은 에러 완료(예: WSAECONNABORTED 10053)로
//     CQ에 도착한다 — CancelIoEx가 없는 RIO 종료 경로의 근거.
// ==========================================================================

#include <WinSock2.h>
#include <MSWSock.h>

// RIO 확장 함수 테이블 (프로세스 전역 1회 로드)
class CRioApi
{
public:
    // Start()에서 1회 호출 — probe는 아무 소켓이나 가능 (리슨 소켓 재사용)
    static bool Load(SOCKET probeSocket)
    {
        GUID funcTableId = WSAID_MULTIPLE_RIO;
        DWORD bytes = 0;
        Rio().cbSize = sizeof(RIO_EXTENSION_FUNCTION_TABLE);
        return WSAIoctl(probeSocket, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
                        &funcTableId, sizeof(funcTableId),
                        &Rio(), sizeof(RIO_EXTENSION_FUNCTION_TABLE),
                        &bytes, NULL, NULL) == 0;
    }

    static RIO_EXTENSION_FUNCTION_TABLE& Rio()
    {
        static RIO_EXTENSION_FUNCTION_TABLE table = {};
        return table;
    }
};

// VirtualAlloc 슬랩과 RIO 버퍼 등록을 수명으로 묶는다.
// 해제 순서 주의: WSACleanup 전에 Release()를 명시 호출할 것 (소멸자는 안전망).
class CRioSlab
{
public:
    CRioSlab() = default;
    ~CRioSlab() { Release(); }

    bool Init(size_t totalSize)
    {
        if (_base != nullptr || totalSize == 0 || totalSize > MAXDWORD)
            return false;

        _base = static_cast<char*>(VirtualAlloc(nullptr, totalSize,
                                                MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE));
        if (_base == nullptr)
            return false;

        _bufferId = CRioApi::Rio().RIORegisterBuffer(_base, static_cast<DWORD>(totalSize));
        if (_bufferId == RIO_INVALID_BUFFERID)
        {
            VirtualFree(_base, 0, MEM_RELEASE);
            _base = nullptr;
            return false;
        }

        _size = totalSize;
        return true;
    }

    void Release()
    {
        if (_bufferId != RIO_INVALID_BUFFERID)
        {
            CRioApi::Rio().RIODeregisterBuffer(_bufferId);
            _bufferId = RIO_INVALID_BUFFERID;
        }
        if (_base != nullptr)
        {
            VirtualFree(_base, 0, MEM_RELEASE);
            _base = nullptr;
        }
        _size = 0;
    }

    char* Base() const { return _base; }
    size_t Size() const { return _size; }
    RIO_BUFFERID BufferId() const { return _bufferId; }

    // 슬랩 내부 포인터 → RIO_BUF.Offset (호출자가 p ∈ [Base, Base+Size) 보장)
    ULONG OffsetOf(const char* p) const { return static_cast<ULONG>(p - _base); }

private:
    char* _base = nullptr;
    size_t _size = 0;
    RIO_BUFFERID _bufferId = RIO_INVALID_BUFFERID;

    CRioSlab(const CRioSlab&) = delete;
    CRioSlab& operator=(const CRioSlab&) = delete;
};
