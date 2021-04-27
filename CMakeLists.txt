cmake_minimum_required(VERSION 3.18)
project(Etesian
        VERSION 0.0.1
        DESCRIPTION "Quintessential."
        LANGUAGES CXX C ASM)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS -L/usr/local/opt/llvm/lib)

add_executable(etesian
        src/etesian/liballoc/alloc-impl.cc src/etesian/liballoc/alloc.h src/etesian/liballoc/alloc-impl.h src/etesian/liballoc/thread_support.h src/etesian/libcore/rt-lambda.h src/etesian/liballoc/thread_support.cc src/etesian/libcore/rt-var.h)
set_property(TARGET etesian PROPERTY CXX_STANDARD_20)
message(STATUS "Include dir: ${CMAKE_SOURCE_DIR}/etesian")
target_include_directories(etesian PUBLIC ${CMAKE_SOURCE_DIR})

if (ETS_USE_BREW_CONFIG)
    if (ETS_USE_BREW_CONFIG STREQUAL "LLVM")
        set(ETS_EXTRA_CFLAGS "-isystem/usr/local/include"
                "-isystem/Library/Developer/CommandLineTools/usr/lib/clang/12.0.0/include"
                "-isystem/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include"
                "-isystem/Library/Developer/CommandLineTools/usr/include"
                "-F/Library/Developer/CommandLineTools/MacOSX.sdk/System/Library/Frameworks"
                "-F/Library/Developer/CommandLineTools/MacOSX.sdk/Library/Frameworks"
                "-F/Library/Developer/CommandLineTools/MacOSX.sdk/usr/local/include")
        set(ETS_EXTRA_LDFLAGS ${ETS_EXTRA_CFLAGS}
                "-L/usr/local/opt/llvm/lib"
                "-Wl,-rpath,/usr/local/opt/llvm/lib")
    else ()
        message(ERROR "ETS_USER_BREW_CONFIG=GCC is not supported yet")
    endif ()
endif ()

target_compile_options(etesian PRIVATE -Wno-unused-command-line-argument)
target_include_directories(etesian PUBLIC ${CMAKE_SOURCE_DIR}/src)

if (${CMAKE_CXX_COMPILER_ID} MATCHES "^.*Clang$")
    message(STATUS "Detected Clang, adding appropriate flags")
    target_compile_options(etesian PRIVATE
            $<$<CONFIG:DEBUG>:-fcolor-diagnostics>
            --config ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/cxx.txt
            ${ETS_EXTRA_CFLAGS})
    add_custom_target(etesian-flags-clang DEPENDS
            ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/cxx.txt
            ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/ld.txt)
    target_link_options(etesian PRIVATE
            --config ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/ld.txt
            ${ETS_EXTRA_LDFLAGS})
    add_dependencies(etesian etesian-flags-clang)
endif ()
find_program(_etesian_ccache ccache)
if (_etesian_ccache)
    set_target_properties(etesian
            PROPERTIES RULE_LAUNCH_COMPILE ${_etesian_ccache}
            RULE_LAUNCH_LINK ${_etesian_ccache})
endif ()
