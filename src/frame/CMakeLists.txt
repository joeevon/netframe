project(netframe_frame)

list(APPEND SRC 
	cnv_xml_parse.c
    netframe_common.c
    netframe_handle.c
    netframe_io.c
    netframe_accept.c
    netframe_main.c
    )

list(APPEND LIBS
    pthread
	netframe_container
	netframe_base
)

add_library(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})
