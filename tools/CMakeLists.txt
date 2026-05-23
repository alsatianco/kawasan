# Broker executable
add_executable(kawasan-broker
    kawasan-broker.cpp
)

target_link_libraries(kawasan-broker PRIVATE
    kawasan
    Boost::program_options
)

# Install executable
install(TARGETS kawasan-broker
    RUNTIME DESTINATION bin
)

add_executable(kawasan-topics
    kawasan-topics.cpp
)

target_link_libraries(kawasan-topics PRIVATE
    kawasan
    Boost::program_options
)

install(TARGETS kawasan-topics
    RUNTIME DESTINATION bin
)

# Consumer groups management tool
add_executable(kawasan-groups
    kawasan-groups.cpp
)

target_link_libraries(kawasan-groups PRIVATE
    kawasan
    Boost::program_options
)

install(TARGETS kawasan-groups
    RUNTIME DESTINATION bin
)

# Metadata consistency check tool
add_executable(kawasan-metadata-check
    kawasan-metadata-check.cpp
)

target_link_libraries(kawasan-metadata-check PRIVATE
    kawasan
    Boost::program_options
)

install(TARGETS kawasan-metadata-check
    RUNTIME DESTINATION bin
)
