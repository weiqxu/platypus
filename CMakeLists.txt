cmake_minimum_required(VERSION 3.16)

project(platypus LANGUAGES C CXX)

option(PLATYPUS_BUILD_DUCKDB "Build DuckDB from contrib/duckdb" ON)
option(PLATYPUS_BUILD_XERCES "Build Xerces-C from contrib/xerces-c" ON)
option(PLATYPUS_BUILD_GPORCA "Build GPORCA from third_party/gporca" ON)

if(PLATYPUS_BUILD_DUCKDB)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/contrib/duckdb/CMakeLists.txt")
    add_subdirectory(contrib/duckdb)
  else(PLATYPUS_BUILD_DUCKDB)
    message(FATAL_ERROR "DuckDB sources were not found at contrib/duckdb")
  endif()
endif()

if(PLATYPUS_BUILD_XERCES)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/contrib/xerces-c/CMakeLists.txt")
    add_subdirectory(contrib/xerces-c)

    # Point GPORCA's FindXerces module at the in-tree Xerces build.
    #set(XERCES_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/contrib/xerces-c/src"
    #    CACHE PATH "Xerces include root" FORCE)
    set(XERCES_INCLUDE_DIR
        "${XERCES_INCLUDE_DIR};${CMAKE_CURRENT_BINARY_DIR}/contrib/xerces-c/src;${CMAKE_CURRENT_BINARY_DIR}/contrib/xerces-c/src"
        CACHE STRING "Xerces include roots" FORCE)
    set(XERCES_LIBRARY "xerces-c"
        CACHE STRING "Xerces library target/name" FORCE)
    set(XERCES_LIBRARIES "${XERCES_LIBRARY}"
        CACHE STRING "Xerces libraries" FORCE)
  else()
    message(FATAL_ERROR "Xerces-C sources were not found at contrib/xerces-c")
  endif()
endif()

if(PLATYPUS_BUILD_GPORCA)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/gporca/CMakeLists.txt")
    add_subdirectory(third_party/gporca)
  else()
    message(FATAL_ERROR "GPORCA sources were not found at third_party/gporca")
  endif()
endif()

if(NOT PLATYPUS_BUILD_DUCKDB AND NOT PLATYPUS_BUILD_GPORCA AND NOT PLATYPUS_BUILD_XERCES)
  message(FATAL_ERROR "No build target selected. Enable one or more PLATYPUS_BUILD_* options.")
endif()

add_subdirectory(src)
