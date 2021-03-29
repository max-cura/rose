cmake_minimum_required(VERSION 3.18)
project(Rose
        VERSION 0.0.1
        DESCRIPTION "Quintessential."
        LANGUAGES CXX C ASM)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS -L/usr/local/opt/llvm/lib)

add_executable(rose
        rose/archive/archive.cc
        rose/rose.cc)
set_property(TARGET rose PROPERTY CXX_STANDARD_20)
message(STATUS "Include dir: ${CMAKE_SOURCE_DIR}/rose")
target_include_directories(rose PUBLIC ${CMAKE_SOURCE_DIR})
#target_link_libraries(rose PUBLIC stdc++)

if (${CMAKE_CXX_COMPILER_ID} MATCHES "^.*Clang$")
    target_compile_options(rose PRIVATE
            $<$<CONFIG:DEBUG>:-fcolor-diagnostics>
            --config ${CMAKE_SOURCE_DIR}/build-flags/${CMAKE_CXX_COMPILER_ID}/${CMAKE_BUILD_TYPE}/cxx.txt)
    add_custom_target(rose-flags-clang DEPENDS
            ${CMAKE_SOURCE_DIR}/build-flags/${CMAKE_CXX_COMPILER_ID}/${CMAKE_BUILD_TYPE}/cxx.txt
            ${CMAKE_SOURCE_DIR}/build-flags/${CMAKE_CXX_COMPILER_ID}/${CMAKE_BUILD_TYPE}/ld.txt)
    target_link_options(rose PRIVATE --config ${CMAKE_SOURCE_DIR}/build-flags/${CMAKE_CXX_COMPILER_ID}/${CMAKE_BUILD_TYPE}/ld.txt)
    add_dependencies(rose rose-flags-clang)
endif ()
find_program(_rose_ccache ccache)
if (_rose_ccache)
    set_target_properties(rose
            PROPERTIES RULE_LAUNCH_COMPILE ${_rose_ccache}
            RULE_LAUNCH_LINK ${_rose_ccache})
endif ()
