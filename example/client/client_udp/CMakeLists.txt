project(client_udp)

list(APPEND LIBS
    pthread
    )
	
add_executable(${PROJECT_NAME} ${PROJECT_NAME}.c)
target_link_libraries(${PROJECT_NAME} ${LIBS})