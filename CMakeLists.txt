cmake_minimum_required(VERSION 3.15 FATAL_ERROR)

project(multicorn)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
set(CMAKE_FIND_LIBRARY_PREFIXES "")
set(CMAKE_FIND_LIBRARY_SUFFIXES ".lib")

add_library(multicorn SHARED src/errors.c src/python.c src/query.c src/multicorn.c src/utils.c)

set(Python_FIND_REGISTRY LAST)
find_package(Python REQUIRED COMPONENTS Interpreter Development)
target_link_libraries(multicorn ${Python_LIBRARIES})
target_link_directories(multicorn PUBLIC ${Python_LIBRARY_DIRS})
target_include_directories(multicorn PUBLIC ${Python_INCLUDE_DIRS})

message("python libs = ${Python_LIBRARIES}")

find_package(PostgreSQL REQUIRED)
target_link_libraries(multicorn ${PostgreSQL_LIBRARIES})
if(WIN32)
  target_link_libraries(multicorn postgres)
endif(WIN32)
target_link_directories(multicorn PUBLIC ${PostgreSQL_LIBRARY_DIRS})
target_include_directories(multicorn PUBLIC ${PostgreSQL_INCLUDE_DIRS})
if(WIN32)
  target_include_directories(multicorn PUBLIC ${PostgreSQL_INCLUDE_DIR}/server/port/win32)
endif()
if(MSVC)
  target_include_directories(multicorn PUBLIC ${PostgreSQL_INCLUDE_DIR}/server/port/win32_msvc)
endif()
