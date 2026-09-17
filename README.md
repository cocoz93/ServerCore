# ServerCore

C++17 서버 토대 — **게임을 모르는** 네트워크·기반 계층입니다.
[MMO_Zone](https://github.com/cocoz93/MMO) 에서 동접 ~5,000까지 병목을 추적하며 만든 코드를 떼어낸 것으로,
다른 게임이 그대로 가져다 쓸 수 있게 두 계층으로 갈라 두었습니다.

```
ServerBaseLib      링버퍼 · 직렬화 버퍼 · 락프리 설정 · 코어 친화도 · 로거 · 플랫폼 경계
      ↑
ServerNetworkLib   수용 · 세션 수명 · 전송 팔 (Windows IOCP / RIO · Linux epoll)
      ↑
   (소비자 게임)    게임 로직 — 이 저장소는 여기를 모릅니다
```

## 무엇을 모르는가

경계를 코드가 아니라 **컴파일로 강제**합니다. `ServerNetworkLib` 의 include 경로에는
소비자 디렉터리가 들어 있지 않아서, 게임 헤더를 참조하면 빌드가 깨집니다.

전송 계층이 아는 것은 **와이어 헤더(`Network/NetHeader.h`)** 까지입니다 — 「패킷 앞 2바이트가 길이다」는
규약이고, 게임 패킷 정의(`Protocol.h`)는 소비자 것입니다.

## 쓰는 법

```cmake
add_subdirectory(ServerCore)
target_link_libraries(내게임 PRIVATE ServerNetworkLib)   # Base 는 따라온다
```

소비자의 게임 프로토콜 헤더에서 와이어 헤더를 include 하면 같은 규약을 말하게 됩니다.

```cpp
#include "ServerCore/Network/NetHeader.h"   // MsgHeader · EchoMsgHeader
enum class MsgType : uint16_t { /* 게임이 정한다 */ };
```

## 필요한 것

### LockFree 저장소

락프리 큐·스택·메모리풀은 사본을 두지 않고 [LockFree](https://github.com/cocoz93/LockFree) 를 직접 참조합니다.
기본값은 **나란히 둔 형제 폴더**이고, 다른 자리에 뒀다면 구성 명령에 알려주면 됩니다.

```
<부모>/
├─ ServerCore/    ← 이 저장소
└─ LockFree/      ← https://github.com/cocoz93/LockFree
```

```
cmake -S . -B build -DLOCKFREE_DIR=<LockFree_Test 경로>
```

없으면 **구성 단계에서** 경로를 찍으며 멈춥니다(컴파일까지 가서 `C1083` 이 나던 것을 앞으로 당긴 것입니다).

### 그 밖

- **x64 전용** — 128비트 CAS(`cmpxchg16b`)를 씁니다
- **spdlog · cpp-httplib 는 이 저장소에 담겨 있습니다**(`Base/ThirdParty/`, 둘 다 MIT). 따로 받을 것이 없습니다
- 리눅스는 `g++-13` 기준으로 검증했습니다

## 단독 빌드

소비자 없이도 빌드됩니다. 라이브러리인지 아닌지를 이걸로 가릅니다.

```
cmake -S . -B build -G "Visual Studio 17 2022" -A x64    # Windows
cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=g++-13 # Linux
cmake --build build --config Release
```

## 빌드 토글

전송·송신 경로 선택은 `Base/CoreConfig.h` 에 모여 있습니다.

| 토글 | 기본 | 무엇 |
|---|---|---|
| `USE_LOCKFREE_SENDQ` | 0 | SendQ 구현 — 링버퍼 / 락프리큐 |
| `USE_SEND_COALESCING` | 1 | 틱 끝에 세션당 1회 flush |
| `USE_ZERO_SNDBUF` | 0 | `SO_SNDBUF=0` 실험 |
| `USE_SEND_THREAD` | 1 | 송신 워커 분리 |
| `USE_RIO_TRANSPORT` | 0 | IOCP ↔ Registered I/O |

값끼리 배타·의존 관계가 있어 헤더 안에서 `#error` 로 막아 둡니다.

## 이력

이 코드의 과거 커밋은 [MMO_Zone](https://github.com/cocoz93/MMO) 에 있습니다.
떼어낼 때 파일만 가져왔으므로 `git blame` 은 여기서 오늘부터 시작합니다.

## 라이선스

MIT — [LICENSE](LICENSE)
`Base/ThirdParty/` 의 spdlog · cpp-httplib 는 각자의 MIT 라이선스를 따릅니다.