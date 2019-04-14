
include_directories( ${ENGINE_COMMON_INCLUDES} ../blockcache ../primproc)


########### next target ###############

set(processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

#libprocessor_a_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

add_library(processor STATIC ${processor_STAT_SRCS})

target_link_libraries(processor ${NETSNMP_LIBRARIES})

INSTALL (TARGETS processor DESTINATION ${ENGINE_LIBDIR})

add_executable(pp_processor_ops_tests tdriver.cpp)
target_link_libraries(pp_processor_ops_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} processor dbbc)

install(TARGETS pp_processor_ops_tests DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
