#pragma once

//=============================================================================
// 락프리 자료구조 진입점 — 형제 저장소(MyGit\LockFree)를 직접 참조한다.
//
// 사본을 두지 않는 이유: 예전엔 MMOServer\LockFree\ 아래에 복사해 뒀는데, 저장소 쪽
// 결함 수정이 서버에 반영되지 않은 채 양쪽이 갈라졌다. 그래서 사본을 없애고 원본을 본다.
//
// [빌드 전제] 저장소 위치는 CMake 의 LOCKFREE_DIR 이 정한다. 기본값은 나란히 둔 형제 폴더다.
//     <부모>/MMO/         <- 이 저장소
//     <부모>/LockFree/    <- https://github.com/cocoz93/LockFree
//   다른 자리에 두려면 -DLOCKFREE_DIR=<LockFree_Test 경로>. 없으면 CMake 가 구성 단계에서
//   경로를 찍으며 멈춘다(예전엔 여기 include 에서 C1083 이 나고 원인이 안 보였다).
//
// 정책을 이 파일 한 곳에 모아둔 이유:
//   - 어떤 락프리 헤더를 쓰는지가 소스에 그대로 보인다. 경로만 빌드계가 준다.
//   - 아래 LF_ON_ALLOC_FAIL 은 라이브러리 헤더보다 반드시 먼저 정의돼야 한다.
//     같은 파일 안에 정의와 include 를 붙여두면 그 순서가 깨질 수 없다.
//     (매크로가 번역 단위마다 다르게 펼쳐지면 템플릿 인라인 함수가 서로 달라진다)
//
// 락프리 자료구조를 쓰는 곳은 LockFree/*.h 를 직접 include 하지 말고 이 헤더를 include 할 것.
//=============================================================================

//-----------------------------------------------------------------------------
// 할당 실패 정책: 라이브러리는 nullptr 을 반환할 뿐 정책을 정하지 않는다.
// 그런데 이 서버는 실패를 검사하지 않는 호출부가 있다 —
//   SerialBuffer::Alloc() 은 결과를 널 검사 없이 바로 역참조하고,
//   TimingWheel 은 Enqueue 반환값을 무시한다.
// 그대로 두면 메모리 부족이 조용한 유실이나 엉뚱한 곳의 접근 위반으로 나타나므로,
// 원인 지점에서 즉시 덤프를 남기고 죽인다.
//
// 실제 크래시는 LockFreeConfig.cpp 가 맡는다. 이 헤더가 CrashDump.h(=windows.h)를
// 끌어오면 그걸 include 하는 모든 곳의 WinSock2 포함 순서가 깨지기 때문이다.
//-----------------------------------------------------------------------------
void LockFree_OnAllocFail(const char* reason);
#define LF_ON_ALLOC_FAIL(msg) LockFree_OnAllocFail(msg)

// 위 정의보다 먼저 include 되면 안 된다. 순서를 바꾸지 말 것.
#include <LockFree/InternalFreeList.h>
#include <LockFree/LockFreeStack.h>
#include <LockFree/LockFreeQueue.h>
#include <LockFree/ExternalTlsFreeList.h>