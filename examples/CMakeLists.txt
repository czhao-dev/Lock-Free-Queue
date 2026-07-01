add_executable(fxmatch_sim simulation.cpp)
target_link_libraries(fxmatch_sim PRIVATE fxmatch Threads::Threads)
