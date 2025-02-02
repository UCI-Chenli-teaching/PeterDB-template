include_directories(${PROJECT_SOURCE_DIR})

add_subdirectory(pfm)
add_subdirectory(rbfm)
add_subdirectory(ix)
add_subdirectory(rm)
add_subdirectory(qe)
add_subdirectory(cli)

add_executable(main start.cc
        include/rbfm_utils.h
        include/rm_utils.h)
target_link_libraries(main cli qe ix rm)