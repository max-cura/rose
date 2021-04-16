cmake_minimum_required(VERSION 3.18)
project(Etesian
        VERSION 0.0.1
        DESCRIPTION "Quintessential."
        LANGUAGES CXX C ASM)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS -L/usr/local/opt/llvm/lib)

add_executable(etesian
        src/alloc.c)
set_property(TARGET etesian PROPERTY CXX_STANDARD_20)
message(STATUS "Include dir: ${CMAKE_SOURCE_DIR}/etesian")
target_include_directories(etesian PUBLIC ${CMAKE_SOURCE_DIR})
#target_link_libraries(etesian PUBLIC stdc++)

if (${CMAKE_CXX_COMPILER_ID} MATCHES "^.*Clang$")
    target_compile_options(etesian PRIVATE
            $<$<CONFIG:DEBUG>:-fcolor-diagnostics>
            --config ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/cxx.txt)
    add_custom_target(etesian-flags-clang DEPENDS
        ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/cxx.txt
        ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/ld.txt)
    target_link_options(etesian PRIVATE --config ${CMAKE_SOURCE_DIR}/support/${CMAKE_CXX_COMPILER_ID}/$<$<CONFIG:DEBUG>:profile_debug>/ld.txt)
    add_dependencies(etesian etesian-flags-clang)
endif ()
find_program(_etesian_ccache ccache)
if (_etesian_ccache)
    set_target_properties(etesian
            PROPERTIES RULE_LAUNCH_COMPILE ${_etesian_ccache}
            RULE_LAUNCH_LINK ${_etesian_ccache})
endif ()
