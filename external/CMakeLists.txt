include_directories(${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/include)
add_library(streamvbyte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/src/streamvbyte.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/src/streamvbytedelta.c
)
target_compile_options(streamvbyte PRIVATE -Wno-cast-align
                                           -Wno-incompatible-pointer-types
                                           -Wno-implicit-function-declaration
)
target_compile_options(streamvbyte INTERFACE -Wno-unused-function)
