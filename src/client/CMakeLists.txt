add_library(kawasan_client OBJECT
    producer.cpp
)

target_include_directories(kawasan_client PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_client PUBLIC
    kawasan_common
    kawasan_protocol
    kawasan_storage
)
