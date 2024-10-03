project (lib.asiopq CXX)
enable_testing()

if (NOT DEFINED Boost_FOUND)
    set(Boost_USE_STATIC_LIBS ON)
    find_package(Boost 1.67.0 COMPONENTS context coroutine REQUIRED)
endif()

find_package(PostgreSQL 9.6 REQUIRED)

add_subdirectory(lib.asiopq)
add_subdirectory(lib.asiopq.tests)