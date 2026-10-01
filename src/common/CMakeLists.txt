add_library(kawasan_common OBJECT
    error.cpp
    config.cpp
    logger.cpp
    file_util.cpp
)

target_include_directories(kawasan_common PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_common PUBLIC
    spdlog::spdlog
    nlohmann_json::nlohmann_json
)

