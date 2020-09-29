add_library(rbfm rbfm.cc)
add_dependencies(rbfm googlelog)
target_link_libraries(rbfm glog)