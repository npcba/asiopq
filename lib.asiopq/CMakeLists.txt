file (GLOB HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/asiopq/*.hpp
)

add_library (
    ${PROJECT_NAME}
    INTERFACE
)

target_sources (
    ${PROJECT_NAME}
    INTERFACE
    ${HEADERS}
)

target_link_libraries(
    ${PROJECT_NAME}
    INTERFACE
    ${Boost_LIBRARIES}
    ${PostgreSQL_LIBRARIES}
)

target_include_directories (
    ${PROJECT_NAME}
    INTERFACE
    ${PostgreSQL_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
)
