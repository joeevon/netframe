project(client_tcp.ums)

list(APPEND SRC
	agent_dll.c
    client_tcp.c
    )

list(APPEND LIBS
    pthread
    )
	
add_executable(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})