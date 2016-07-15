include_directories(../src/include)
link_directories(${LIBRARY_OUTPUT_PATH})

add_subdirectory(agent)
add_subdirectory(business_server)