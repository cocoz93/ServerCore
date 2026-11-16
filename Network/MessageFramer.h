#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "NetHeader.h"
#include "SerialBuffer.h"   // MSG_DEFAULT_SIZE · HEADER_SIZE — 길이 머리 자르기의 최대가 기본 버퍼에서 나온다

//==================================================
// 메시지 자르기 부품 — 받은 바이트 흐름에서 메시지 하나의 경계를 찾는다
//
//   전송 계층(ParsePackets)은 링에 쌓인 바이트를 이 부품에 보이고, 부품이 알려 준 대로
//   앞 deliver 바이트를 버퍼에 담아 넘긴 뒤 링에서 consume 바이트를 지운다.
//   쓰는 쪽이 Start 전에 고른다(CIOCPServer::SetFramer). 안 고르면 전체 길이 머리.
//
//   한 번 볼 때 결과는 셋
//     NeedMore — 메시지 하나가 아직 다 안 왔다. 더 받는다
//     Message  — 메시지 하나. 링에서 consume 바이트를 지우고 그 앞 deliver 바이트를 넘긴다(deliver ≤ consume)
//     Invalid  — 규약 위반. 전송 계층이 오류를 세고 접속을 끊는다
//
//   부품은 상태를 갖지 않는다 — 같은 바이트면 같은 결과다. 그래서 서버 하나에 하나를 두고
//   모든 세션 · 워커 스레드가 같이 부른다. 줄 단위가 「아직 LF 없음」 뒤에 처음부터 다시 훑는 값은
//   한 줄 최대(4,096)라 작다.
//
//   왜 가상 함수 하나짜리 부품인가
//     - enum + switch 를 이 안에 두면 새 규약마다 이 라이브러리를 고쳐야 한다(게임을 모르는 토대에 규약 목록이 쌓인다)
//     - 템플릿 인자로 받으면 CIOCPServer 전체가 템플릿이 되어 쓰는 쪽 타입이 규약마다 갈린다
//     - 비용은 받기 완료 한 번에 가상 호출 몇 번 — 커널 호출 한 번보다 훨씬 작다고 본다(실측 전)
//==================================================

// 링 버퍼에 쌓인 읽을 데이터 — 복사 없이 두 구간으로 보인다
//   first  : 읽기 자리 ~ 링 끝
//   second : 링 처음 ~ (링 끝에서 감겼을 때만 · 아니면 크기 0)
struct RecvSpan
{
    const char* first      = nullptr;
    size_t      firstSize  = 0;
    const char* second     = nullptr;
    size_t      secondSize = 0;

    size_t Size() const { return firstSize + secondSize; }

    // offset 자리의 바이트 하나 (offset < Size())
    char At(size_t offset) const
    {
        return (offset < firstSize) ? first[offset] : second[offset - firstSize];
    }

    // 앞에서 size 바이트를 dest 로 복사 (size ≤ Size())
    void CopyFront(void* dest, size_t size) const
    {
        const size_t head = (size < firstSize) ? size : firstSize;
        if (head != 0)
            std::memcpy(dest, first, head);
        if (size > head)
            std::memcpy(static_cast<char*>(dest) + head, second, size - head);
    }
};

enum class FrameStatus : uint8_t
{
    NeedMore,
    Message,
    Invalid,
};

struct FrameResult
{
    FrameStatus status  = FrameStatus::NeedMore;
    size_t      consume = 0;   // Message — 링에서 지울 바이트 수
    size_t      deliver = 0;   // Message — 넘길 바이트 수(앞에서부터 · consume 이하)
    size_t      badSize = 0;   // Invalid — 로그에 남길 크기(길이 머리가 말한 크기 · 줄이면 그때까지의 길이)

    static FrameResult More() { return FrameResult{}; }
    static FrameResult Msg(size_t consume, size_t deliver) { return FrameResult{ FrameStatus::Message, consume, deliver, 0 }; }
    static FrameResult Bad(size_t size) { return FrameResult{ FrameStatus::Invalid, 0, 0, size }; }
};

class CMessageFramer
{
public:
    virtual ~CMessageFramer() = default;

    // 앞에서부터 메시지 하나를 찾는다
    virtual FrameResult Next(const RecvSpan& data) const = 0;

    // 넘길 수 있는 메시지의 최대(deliver 상한) — 전송 계층은 이 크기를 담을 버퍼를 고른다
    virtual size_t MaxMessageSize() const = 0;

    // 기동 로그에 싣는 이름
    virtual const char* Name() const = 0;
};

// 길이 머리 자르기 둘이 같이 쓰는 최대 — 기본 직렬화 버퍼 하나에 머리째 담기는 크기(1,458)
constexpr size_t LENGTH_FRAME_MAX_SIZE = static_cast<size_t>(MSG_DEFAULT_SIZE - HEADER_SIZE);

namespace FramerDetail
{
    // 앞 2바이트 길이 머리 — 와이어 그대로(이 라이브러리가 도는 x86-64 · 리틀 엔디언)
    inline size_t PeekLength16(const RecvSpan& data)
    {
        uint16_t length = 0;
        data.CopyFront(&length, sizeof(length));
        return static_cast<size_t>(length);
    }
}

// ① 전체 길이 머리 — MsgHeader(4바이트) · 길이 = 머리 포함 전체
class CTotalLengthFramer final : public CMessageFramer
{
public:
    static constexpr size_t HEADER = sizeof(MsgHeader);

    FrameResult Next(const RecvSpan& data) const override
    {
        const size_t have = data.Size();
        if (have < HEADER)
            return FrameResult::More();

        const size_t total = FramerDetail::PeekLength16(data);
        if (total < HEADER || total > LENGTH_FRAME_MAX_SIZE)
            return FrameResult::Bad(total);

        if (have < total)
            return FrameResult::More();
        return FrameResult::Msg(total, total);   // 머리째 넘긴다(받는 쪽이 type 을 읽는다)
    }

    size_t MaxMessageSize() const override { return LENGTH_FRAME_MAX_SIZE; }
    const char* Name() const override { return "TotalLength"; }
};

// ② 몸 길이 머리 — EchoMsgHeader(2바이트) · 길이 = 몸만
class CBodyLengthFramer final : public CMessageFramer
{
public:
    static constexpr size_t HEADER = sizeof(EchoMsgHeader);

    FrameResult Next(const RecvSpan& data) const override
    {
        const size_t have = data.Size();
        if (have < HEADER)
            return FrameResult::More();

        // 몸 길이 + 머리 = 전체. 머리보다 작아질 수는 없다(길이 0 = 빈 몸)
        const size_t total = FramerDetail::PeekLength16(data) + HEADER;
        if (total > LENGTH_FRAME_MAX_SIZE)
            return FrameResult::Bad(total);

        if (have < total)
            return FrameResult::More();
        return FrameResult::Msg(total, total);   // 머리째 넘긴다(에코가 받은 그대로 되돌려 보낸다)
    }

    size_t MaxMessageSize() const override { return LENGTH_FRAME_MAX_SIZE; }
    const char* Name() const override { return "BodyLength"; }
};

// ③ 줄 단위 — LF 로 자르고 끝의 CR 하나를 뗀다
//   - LF 만 와도 받는다 · 넘기는 줄에 CR LF 는 싣지 않는다 · 빈 줄도 넘긴다(길이 0)
//   - 줄 가운데 CR 은 그대로 둔다(끝의 하나만 뗀다)
//   - 한 줄 최대 4,096 — LF 없이 쌓인 것이 4,096 을 넘으면 틀림(줄 단위 규약을 쓰는 쪽의 줄 한도와 같은 값)
//   - LF 를 찾은 줄이라도 CR 을 뗀 길이가 4,096 을 넘으면 틀림 — 넘길 버퍼 크기가 정해져 있어서다
class CLineFramer final : public CMessageFramer
{
public:
    static constexpr size_t MAX_LINE = 4096;

    FrameResult Next(const RecvSpan& data) const override
    {
        // LF 찾기 — 두 구간을 차례로 훑는다(복사 없음)
        size_t lf = SIZE_MAX;
        if (data.firstSize != 0)
        {
            const void* hit = std::memchr(data.first, '\n', data.firstSize);
            if (hit != nullptr)
                lf = static_cast<size_t>(static_cast<const char*>(hit) - data.first);
        }
        if (lf == SIZE_MAX && data.secondSize != 0)
        {
            const void* hit = std::memchr(data.second, '\n', data.secondSize);
            if (hit != nullptr)
                lf = data.firstSize + static_cast<size_t>(static_cast<const char*>(hit) - data.second);
        }

        if (lf == SIZE_MAX)
        {
            if (data.Size() > MAX_LINE)
                return FrameResult::Bad(data.Size());
            return FrameResult::More();
        }

        size_t length = lf;
        if (length != 0 && data.At(length - 1) == '\r')
            --length;
        if (length > MAX_LINE)
            return FrameResult::Bad(length);
        return FrameResult::Msg(lf + 1, length);
    }

    size_t MaxMessageSize() const override { return MAX_LINE; }
    const char* Name() const override { return "Line"; }
};
