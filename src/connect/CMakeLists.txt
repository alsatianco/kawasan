add_library(kawasan_connect OBJECT
    worker.cpp
    offset_storage.cpp
    source_task_runner.cpp
    file_source_connector.cpp
    sink_task_runner.cpp
    file_sink_connector.cpp
    jdbc_sink_connector.cpp
)

target_include_directories(kawasan_connect PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_connect PUBLIC
    kawasan_common
    kawasan_client
    nlohmann_json::nlohmann_json
    spdlog::spdlog
)

