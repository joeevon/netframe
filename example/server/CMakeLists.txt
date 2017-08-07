cmake_minimum_required(VERSION 2.8)

#编译方法
#[root@six chengdu]#chmod +x build.sh;./build.sh

#netframe develop
include_directories("/home/joee/netframe/src/include")
link_directories("/home/joee/netframe/publish/debug/lib")

#liblog4cplus
include_directories("/usr/local/ucloud/include")
link_directories("/usr/local/ucloud/lib64")

#libxml2
include_directories("/usr/include/libxml2")

set(CMAKE_VERBOSE_MAKEFILE on)
enable_testing()
enable_language(C)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "debug")
endif()

if(${CMAKE_BUILD_TYPE} STREQUAL "debug")
    set(CMAKE_C_FLAGS_DEBUG "-g -Wall -std=gnu99 -O0")
    set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -std=gnu++0x -O0")
else()
    set(CMAKE_C_FLAGS_RELEASE "-O2 -DNDEBUG -std=gnu99")
    set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG -std=gnu++0x")
endif()

if(CMAKE_BUILD_BITS EQUAL 32)
   list(APPEND CMAKE_C_FLAGS "-m32")
   list(APPEND CMAKE_CXX_FLAGS "-m32")
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

add_subdirectory(src)
