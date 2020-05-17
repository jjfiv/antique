cmake_minimum_required(VERSION 3.13)

add_library(lemur 
    src/keyfilecode.c 
    src/keyprint.c 
    src/keyfilecompress.c 
    src/fileio.c
    src/string-set.c
    src/Keyfile.cpp
    src/RVLCompress.cpp
    src/BulkTree.cpp
    src/File.cpp
    src/lemur-compat.cpp)

add_executable(write_bulktree src/write_bulktree.cpp)
target_link_libraries(write_bulktree lemur)

add_executable(read_bulktree src/read_bulktree.cpp)
target_link_libraries(read_bulktree lemur)
