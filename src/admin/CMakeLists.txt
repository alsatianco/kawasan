add_library(kawasan_admin OBJECT
    placeholder.cpp
)

target_include_directories(kawasan_admin PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_admin PUBLIC
    kawasan_common
    kawasan_protocol
)

