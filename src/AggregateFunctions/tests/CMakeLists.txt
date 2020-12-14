add_executable (quantile-t-digest quantile-t-digest.cpp)


include_directories (${SKETCH_COMMON_INCLUDE_DIR})
include_directories (${SKETCH_TUPLE_INCLUDE_DIR})

add_executable (sketch-test sketch-test.cpp)

target_link_libraries (quantile-t-digest PRIVATE dbms clickhouse_aggregate_functions)
