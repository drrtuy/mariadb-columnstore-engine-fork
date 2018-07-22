
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(messageqcpp_LIB_SRCS
    messagequeue.cpp
    messagequeuepool.cpp
    bytestream.cpp
    socketparms.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    compressed_iss.cpp)

add_library(messageqcpp SHARED ${messageqcpp_LIB_SRCS})

set_target_properties(messageqcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS messageqcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

add_executable(srv srv.cpp)
add_executable(cli cli.cpp)
target_link_libraries(srv ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS}) 
target_link_libraries(cli ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})
install(TARGETS srv DESTINATION  ${ENGINE_BINDIR} COMPONENT platform)
install(TARGETS cli DESTINATION  ${ENGINE_BINDIR} COMPONENT platform)
