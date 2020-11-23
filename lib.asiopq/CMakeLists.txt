add_library (
    ${PROJECT_NAME}
    asiopq.cpp
)

target_link_libraries(
    ${PROJECT_NAME}
    ${Boost_LIBRARIES}
    ${PostgreSQL_LIBRARIES}
)

include_directories(${PostgreSQL_INCLUDE_DIRS})

# Make sure the compiler can find include files for our library
# when other libraries or executables link to it
target_include_directories (${PROJECT_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
