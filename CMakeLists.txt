cmake_minimum_required(VERSION 3.16)

project(fxmatch VERSION 0.2.0 LANGUAGES CXX)

add_library(fxmatch INTERFACE)
target_compile_features(fxmatch INTERFACE cxx_std_17)
target_include_directories(
    fxmatch
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

if(MSVC)
    target_compile_options(fxmatch INTERFACE /W4 /permissive-)
else()
    target_compile_options(fxmatch INTERFACE -Wall -Wextra -Wpedantic)
endif()

find_package(Threads REQUIRED)

include(CTest)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

add_subdirectory(benchmarks)
add_subdirectory(examples)
