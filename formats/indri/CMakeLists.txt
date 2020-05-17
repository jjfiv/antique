cmake_minimum_required(VERSION 3.13)

add_library(lemur 
    src/keyfilecode.c 
    src/keyprint.c 
    src/keyfilecompress.c 
    src/fileio.c
    src/Keyfile.cpp
    src/RVLCompress.cpp
    src/BulkTree.cpp
    src/File.cpp
    src/lemur-compat.cpp)

add_executable(make_test_bulktree src/make_test_bulktree.cpp)
target_link_libraries(make_test_bulktree lemur)