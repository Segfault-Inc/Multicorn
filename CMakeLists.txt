cmake_minimum_required(VERSION 3.4)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

set(CMAKE_FIND_LIBRARY_PREFIXES "")
set(CMAKE_FIND_LIBRARY_SUFFIXES ".lib")

project(multicorn)

#find_package(PG REQUIRED)

include_directories(${PG_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})
link_directories(${PG_LIBRARY_DIRS} ${PYTHON_LIBRARY_DIRS})
add_library(multicorn SHARED src/errors.c src/python.c src/query.c src/multicorn.c src/utils.c)
target_link_libraries(multicorn postgres)
target_link_libraries(multicorn python33)

# "c:\Program Files\CMake\bin\cmake.exe" .. -DPG_LIBRARY_DIRS="C:\Program Files\PostgreSQL\9.6\bin";"C:\Program Files\PostgreSQL\9.6\lib" -DPG_INCLUDE_DIRS="C:\Program Files\PostgreSQL\9.6\include\server";"C:\Program Files\PostgreSQL\9.6\include";"C:\Program Files\PostgreSQL\9.6\include\server\port\win32";"C:\Program Files\PostgreSQL\9.6\include\server\port\win32_msvc" -DPYTHON_INCLUDE_DIRS=C:\edb\languagepack-9.6\x64\Python-3.3\Include -DPYTHON_LIBRARY_DIRS=C:\edb\languagepack-9.6\x64\Python-3.3\libs -G "Visual Studio 14 2015 Win64"
# "c:\Program Files\CMake\bin\cmake.exe" --build . --config Release
