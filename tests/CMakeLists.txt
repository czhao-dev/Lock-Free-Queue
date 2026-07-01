add_executable(correctness_test correctness_test.cpp)
target_link_libraries(correctness_test PRIVATE fxmatch Threads::Threads)
add_test(NAME correctness_test COMMAND correctness_test)

add_executable(engine_test engine_test.cpp)
target_link_libraries(engine_test PRIVATE fxmatch Threads::Threads)
add_test(NAME engine_test COMMAND engine_test)
