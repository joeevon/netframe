project(server.ums)

list(APPEND SRC
	server.c
	)

list(APPEND LIBS
	netframe_frame
	netframe_container
	netframe_osadapter
	netframe_net
	netframe_base
	cnv_liblog4cplus
	log4cplus
	pthread
	xml2
    )

add_executable(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})