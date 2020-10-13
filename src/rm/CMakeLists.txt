add_library(rm rm.cc)
add_dependencies(rm googlelog)
target_link_libraries(rm glog)