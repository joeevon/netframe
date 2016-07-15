project(netframe_net)

list(APPEND SRC
    netframe_net.c
    )
	
list(APPEND LIBS
	netframe_base
)

add_library(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} ${LIBS})
