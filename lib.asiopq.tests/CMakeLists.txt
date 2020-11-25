enable_testing()
project(lib.asiopq.tests)

add_executable(${PROJECT_NAME} asiopq_tests.cpp)
add_test(NAME ${PROJECT_NAME} COMMAND ${PROJECT_NAME})

target_link_libraries(
    ${PROJECT_NAME}
    lib.asiopq
    ${Boost_LIBRARIES}
)
