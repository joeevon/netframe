project(netframe_container)

list(APPEND SRC
    cnv_hashmap.c
    cnv_queue.c
	cnv_blocking_queue.c
	cnv_unblock_queue.c
	cnv_lock_free_queue.c
	cnv_fifo.c
    )

add_library(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})
