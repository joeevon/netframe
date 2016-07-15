project(netframe_base)

list(APPEND SRC
	cjson.c
	alg_md5.c
	cnv_crc32.c
	cnv_comm.c
	cnv_adler32.c
    )

add_library(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})
