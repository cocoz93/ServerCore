cmake_minimum_required(VERSION 3.20)
project(ServerCore LANGUAGES CXX)

# ══════════════════════════════════════════════════════════════════════════
#  ServerCore — 게임을 모르는 서버 토대
#
#  두 계층으로 나뉜다.
#    ServerBaseLib    링버퍼·직렬화 버퍼·락프리 설정·코어 친화도·로거·플랫폼 경계
#    ServerNetworkLib 수용·세션·전송 팔(IOCP / RIO / epoll). Base 만 딛는다.
#
#  소비자는 add_subdirectory(ServerCore) 후 ServerNetworkLib 을 링크하면 된다.
#  단독으로도 빌드된다 — 그게 "라이브러리다"의 판정 조건이라 그렇게 유지한다.
# ══════════════════════════════════════════════════════════════════════════

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 단일구성 제너레이터(Ninja 등) 기본 Release — 실측 기준선이 Release 로 쌓여 있다
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

set(CORE_BASE ${CMAKE_CURRENT_SOURCE_DIR}/Base)
set(CORE_NET  ${CMAKE_CURRENT_SOURCE_DIR}/Network)

# ─────────────────────────────────────────────────────────────
#  LockFree 저장소 위치 — 형제 저장소(github.com/cocoz93/LockFree)
#    소스에 상대경로를 박지 않는다. 박으면 폴더 배치가 코드에 들어가서
#    옮기는 순간 C1083으로 죽고, 원인이 에러 메시지에 안 보인다.
#    소비자가 자기 배치에 맞게 -DLOCKFREE_DIR=<경로> 로 넘길 수 있다.
#    기본값은 이 저장소와 나란히 둔 형제 폴더.
# ─────────────────────────────────────────────────────────────
set(LOCKFREE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../LockFree/LockFree_Test"
    CACHE PATH "LockFree 저장소의 LockFree_Test 디렉터리")

if(NOT EXISTS "${LOCKFREE_DIR}/LockFree/LockFreeQueue.h")
    message(FATAL_ERROR
        "LockFree 저장소를 찾을 수 없습니다: ${LOCKFREE_DIR}\n"
        "  git clone https://github.com/cocoz93/LockFree  (이 저장소와 나란히)\n"
        "  또는 -DLOCKFREE_DIR=<LockFree_Test 경로>")
endif()

# ─────────────────────────────────────────────────────────────
#  플랫폼 전역 옵션 — 타겟 정의보다 먼저
#    add_compile_options는 "이후 정의되는" 타겟에만 적용된다.
# ─────────────────────────────────────────────────────────────
if(UNIX)
    # 16바이트 CAS(cmpxchg16b) — LockFree의 128비트 CAS는 이 플래그가 없으면 인라인되지 않고
    #   libatomic 호출로 내려간다. LockFreeCompat.h가 #error로 막아 두긴 했지만, 여기서
    #   전역으로 걸어야 앞으로 추가될 타겟이 빠뜨릴 일이 없다.
    add_compile_options(-mcx16)

    # backtrace_symbols_fd가 함수명을 찍으려면 실행 파일의 심볼이 동적 심볼 테이블에 있어야 한다.
    #   기본 링크는 실행 파일 심볼을 export하지 않아, 크래시 로그가 주소만 남고 이름이 사라진다.
    add_link_options(-rdynamic)
endif()

# ═════════════════════════════════════════════════════════════
#  ServerBaseLib — 토대 계층 (직렬화 버퍼 · 락프리 설정 · 코어 친화도 · 로거)
#    게임도 네트워크도 모른다. 위 두 계층이 공통으로 딛는 바닥이다.
#    컴파일 설정을 PUBLIC으로 거는 이유 — 소비자 타겟에 그대로 전파되어야
#    매크로가 갈리지 않는다(갈리면 구조체 레이아웃이 달라져 조용히 깨진다).
# ═════════════════════════════════════════════════════════════
add_library(ServerBaseLib STATIC
    ${CORE_BASE}/SerialBuffer.cpp
    ${CORE_BASE}/LockFreeConfig.cpp
    ${CORE_BASE}/CoreAffinity.cpp
    ${CORE_BASE}/Common/Logger.cpp
)

target_include_directories(ServerBaseLib PUBLIC
    ${CORE_BASE}               # <RingBuffer.h> · <Common/…> · "Platform/Platform.h"
    ${CORE_BASE}/ThirdParty    # spdlog·fmt·httplib (헤더온리 번들 — 리포에 담겨 있다)
    ${LOCKFREE_DIR}            # 형제 저장소 — <LockFree/…> 로 열린다
)

target_compile_features(ServerBaseLib PUBLIC cxx_std_17)

target_compile_definitions(ServerBaseLib PUBLIC
    USE_SPDLOG_LOGGER
)

if(WIN32)
    target_compile_definitions(ServerBaseLib PUBLIC
        _CONSOLE
        UNICODE
        _UNICODE
    )
endif()

if(MSVC)
    # 공통: UTF-8 소스, 표준 준수(/permissive-), SDL, 다중 컴파일
    target_compile_options(ServerBaseLib PUBLIC /utf-8 /permissive- /sdl /MP)
    # Release: 함수단위링크(/Gy)·인트린식(/Oi)·전체프로그램최적화(/GL)
    #   ※ PUBLIC이어야 라이브러리 경계를 넘어 인라인이 유지된다. PRIVATE으로 두면
    #     빌드는 되는데 경계에서 인라인이 끊겨 수치만 조용히 나빠진다.
    target_compile_options(ServerBaseLib PUBLIC $<$<CONFIG:Release>:/Gy /Oi /GL>)
endif()

if(UNIX)
    target_link_libraries(ServerBaseLib PUBLIC pthread)
endif()

# ═════════════════════════════════════════════════════════════
#  ServerNetworkLib — 전송 계층 (수용·세션·전송 팔)
#    게임 로직을 모른다. 와이어 헤더(NetHeader.h)까지만 안다.
#    전송 팔만 플랫폼별로 갈린다 — Windows는 IOCP/RIO, Linux는 epoll.
# ═════════════════════════════════════════════════════════════
add_library(ServerNetworkLib STATIC
    ${CORE_NET}/IOCPServer.cpp
)

if(WIN32)
    target_sources(ServerNetworkLib PRIVATE
        ${CORE_NET}/Transport_Iocp.cpp   # 완료포트 팔 (RIO 빌드에서는 빈 번역단위)
        ${CORE_NET}/Transport_Rio.cpp    # RIO 팔 (IOCP 빌드에서는 빈 번역단위)
    )
else()
    target_sources(ServerNetworkLib PRIVATE
        ${CORE_NET}/Transport_Epoll.cpp
    )
endif()

target_include_directories(ServerNetworkLib PUBLIC
    ${CORE_NET}    # "IOCPServer.h" · "RioApi.h" · "NetIoModel.h" · "NetHeader.h"
)

# 와이어 헤더(NetHeader.h)는 이 저장소가 갖는다 — "패킷 앞 2바이트가 길이다"는
#   전송 계층이 정하는 규약이라 게임이 바뀌어도 안 바뀐다.
#   게임 패킷 정의(Protocol.h)는 소비자 것이고, 그쪽이 이 헤더를 include 해서
#   같은 와이어를 말한다. 그래서 이 저장소는 소비자 없이도 단독으로 빌드된다.

target_link_libraries(ServerNetworkLib PUBLIC ServerBaseLib)

# ⚠ 빌드 토글 — Base/CoreConfig.h 가 전송·송신 경로 기본값을 정한다.
#   소비자가 다르게 쓰려면 그 헤더를 고치는 대신 컴파일 정의로 덮어쓸 수 있게
#   #ifndef 가드를 씌우는 것이 다음 단계다(지금은 USE_DB_WORKER 만 그 형태).