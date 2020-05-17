cmake_minimum_required(VERSION 3.13)

add_library(lemur 
    src/keyfilecode.c 
    src/keyprint.c 
    src/keyfilecompress.c 
    src/string-set.c 
    src/fn_f.c 
    src/fileio.c
    src/SHA1.cpp
    src/Keyfile.cpp
    src/RVLCompress.cpp
    src/BulkTree.cpp
    src/File.cpp
    src/lemur-compat.cpp)