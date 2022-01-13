
include_directories( ${ENGINE_COMMON_INCLUDES} ../blockcache
    ../primproc 
    /data/tensorflow/bazel-bin/tensorflow/include/
    /data/tensorflow/bazel-bin/tensorflow/include/src
    /data/tensorflow/third_party/eigen3/
    /usr/local/include/tensorflow/
    /data/tensorflow/ )

########### next target ###############

set(processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

add_library(processor STATIC ${processor_STAT_SRCS})

add_dependencies(processor loggingcpp)

target_link_libraries(processor ${NETSNMP_LIBRARIES} tensorflow_cc tensorflow_framework)

INSTALL (TARGETS processor DESTINATION ${ENGINE_LIBDIR})
