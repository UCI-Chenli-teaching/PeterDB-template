add_library(rm rm.cc
        rm_utils.cpp)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)