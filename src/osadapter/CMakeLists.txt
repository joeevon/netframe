project(netframe_osadapter)

list(APPEND SRC
    cnv_thread_sys.c
    cnv_thread.c
    )

add_library(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})
