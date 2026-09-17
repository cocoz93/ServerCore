// CrashDump.h는 set_terminate 등 표준 헤더를 스스로 들이지 않아, 먼저 include되면
// 식별자를 못 찾는다. 이 파일은 CrashDump.h를 선두에서 쓰므로 여기서 채워준다.
#include <exception>
#include <new>
#include "Crash/CrashDump.h"
#include "LockFreeConfig.h"

// 락프리 풀이 노드를 확보하지 못했을 때(HeapAlloc 실패) 호출된다.
// CRASH()는 문자열 리터럴만 받으므로(L##reason) 사유는 고정 문구로 남기고,
// 호출자가 넘긴 상세는 덤프의 콜스택으로 확인한다.
void LockFree_OnAllocFail(const char* reason)
{
    (void)reason;
    CRASH("LockFree pool allocation failed - out of memory");
}