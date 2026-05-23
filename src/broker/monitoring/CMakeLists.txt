add_library(kawasan_monitoring
    metrics_collector.cpp
    http_server.cpp
    monitoring_manager.cpp
)

target_include_directories(kawasan_monitoring
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_monitoring
    PUBLIC
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        kawasan_common
)
