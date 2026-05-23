# Simple producer example
add_executable(simple_producer simple_producer.cpp)
target_link_libraries(simple_producer PRIVATE kawasan)

# Simple consumer example
add_executable(simple_consumer simple_consumer.cpp)
target_link_libraries(simple_consumer PRIVATE kawasan)

# Streams examples (Week 22)
add_executable(streams_wordcount streams_wordcount.cpp)
target_link_libraries(streams_wordcount PRIVATE kawasan)

add_executable(streams_pageview streams_pageview.cpp)
target_link_libraries(streams_pageview PRIVATE kawasan)

add_executable(streams_anomaly streams_anomaly.cpp)
target_link_libraries(streams_anomaly PRIVATE kawasan)
