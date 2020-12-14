option(ENABLE_TUPLE_SKETCH "Enable tuple sketch unique counting")

if (ENABLE_TUPLE_SKETCH)
    set (SKETCH_COMMON_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/datasketch/common/include) 
    set (SKETCH_TUPLE_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/datasketch/tuple/include) 
    set (USE_TUPLE_SKETCH 1)
endif()
message (STATUS, "Using data-sketch=${USE_TUPLE_SKETCH} : ${SKETCH_COMMON_INCLUDE_DIR};${SKETCH_TUPLE_INCLUDE_DIR} ")
