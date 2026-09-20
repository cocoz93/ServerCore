# ServerCore

C++17 서버 토대 — **게임을 모르는** 네트워크·기반 계층입니다.
[MMO_Zone](https://github.com/cocoz93/MMO_Zone) 에서 동접 ~5,000까지 병목을 추적하며 만든 코드를 떼어낸 것으로,
다른 게임이 그대로 가져다 쓸 수 있게 두 계층으로 분리합니다.

```
---------------------------------------------------------------------------------
ServerBaseLib      링버퍼 · 직렬화 · 락프리 · 코어 친화도 · 로거
   ▲   ▲
   │   └── MO_Belt — Linux epoll 벨트스크롤, 헤더만 가져간다
   │
ServerNetworkLib   수용 · 세션 수명 · 전송 (IOCP / RIO / epoll)
   ▲
   ├── MMO_Zone — Windows IOCP MMO
   └── 다음 게임 — 붙이면 그만
----------------------------------------------------------------------------------
```

## 어디까지가 ServerCore 인가

| | 여기 | 가져다 쓰는 쪽 |
|---|---|---|
| 패킷 | 「앞 2바이트가 길이」 `Network/NetHeader.h` | `MsgType` 정의 `Protocol.h` |
| 빌드 토글 | 전송·송신 경로 `Base/CoreConfig.h` | 게임 실험 토글 |

쓰는 곳 둘 — [MMO_Zone](https://github.com/cocoz93/MMO_Zone)(Windows IOCP) ·
[TotalTest](https://github.com/cocoz93/TotalTest)(링버퍼 단위 테스트)

## 쓰는 법

```cmake
add_subdirectory(ServerCore)
target_link_libraries(내게임 PRIVATE ServerNetworkLib)   # Base 는 따라온다
```

게임 쪽 프로토콜 헤더에서 이 헤더를 include 하면 양쪽이 같은 규약을 쓰게 됩니다.

```cpp
#include "ServerCore/Network/NetHeader.h"   // MsgHeader · EchoMsgHeader
enum class MsgType : uint16_t { /* 게임이 정한다 */ };
```

## 고칠 때

**이 저장소가 정본입니다.** 가져다 쓰는 쪽에서 고치면 그 PC 에만 남습니다
— 그쪽 `git add -A` 에 담기지 않고 `git status` 에 `m ServerCore` 한 줄로만 보입니다.

```
cd ServerCore && git checkout main   # ← 이것부터 (안 하면 커밋이 어느 가지에도 안 붙습니다)
# 고치고 커밋 · push
cd .. && git add ServerCore          # 가져다 쓰는 쪽이 가리키는 곳 옮기기
```

**ServerCore 를 먼저 push** 해야 합니다 — 순서를 바꾸면 가져다 쓰는 쪽이 아직 올라가지 않은 커밋을 가리키게 됩니다.

## 필요한 것

### LockFree 저장소

락프리 큐·스택·메모리풀은 여기에 사본을 두지 않고 [LockFree](https://github.com/cocoz93/LockFree) 저장소를 그대로 가져다 씁니다.
기본값은 **나란히 둔 옆 폴더**이고, 다른 자리에 뒀다면 구성 명령에 경로를 적어 주면 됩니다.

```
<부모>/
├─ ServerCore/    ← 이 저장소
└─ LockFree/      ← https://github.com/cocoz93/LockFree
```

```
cmake -S . -B build -DLOCKFREE_DIR=<LockFree_Test 경로>
```

없으면 **구성 단계에서** 찾던 경로를 알려 주며 멈춥니다(전에는 컴파일까지 간 뒤에야 `C1083` 이 났는데, 그 자리를 앞으로 당긴 것입니다).

### 그 밖

- **x64 전용** — 128비트 CAS(`cmpxchg16b`)를 씁니다
- **spdlog · cpp-httplib 는 이 저장소에 담겨 있습니다**(`Base/ThirdParty/`, 둘 다 MIT). 따로 받을 것이 없습니다
- 리눅스는 `g++-13` 에서 빌드·동작을 확인했습니다

## 단독 빌드

가져다 쓰는 게임 없이도 빌드됩니다. 정말 독립된 라이브러리인지를 이걸로 가립니다.

```
cmake -S . -B build -G "Visual Studio 17 2022" -A x64    # Windows
cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=g++-13 # Linux
cmake --build build --config Release
```

## 빌드 토글

전송·송신 경로 선택은 `Base/CoreConfig.h` 에 모여 있습니다.

| 토글 | 기본 | 무엇 |
|---|---|---|
| `USE_LOCKFREE_SENDQ` | 0 | 보낼 것을 쌓아두는 자리 — 링버퍼 / 락프리큐 |
| `USE_SEND_COALESCING` | 1 | 틱 끝에 세션당 한 번만 내보내기 |
| `USE_ZERO_SNDBUF` | 0 | `SO_SNDBUF=0` 실험 |
| `USE_SEND_THREAD` | 1 | 보내는 일을 따로 스레드로 |
| `USE_RIO_TRANSPORT` | 0 | IOCP ↔ Registered I/O |

같이 켤 수 없거나 서로를 필요로 하는 조합이 있어, 헤더 안에서 `#error` 로 막아 둡니다.

## 이력

이 코드의 과거 커밋은 [MMO_Zone](https://github.com/cocoz93/MMO_Zone) 에 있습니다.
떼어낼 때 파일만 가져왔으므로 `git blame` 은 여기서 떼어낸 날부터 시작합니다.

## 라이선스

MIT — [LICENSE](LICENSE)
`Base/ThirdParty/` 의 spdlog · cpp-httplib 는 각자의 MIT 라이선스를 따릅니다.
