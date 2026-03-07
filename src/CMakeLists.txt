cmake_minimum_required(VERSION 3.16)

project(server LANGUAGES CXX)

add_executable(server main.cpp)
target_compile_features(server PRIVATE cxx_std_17)
target_compile_definitions(server PRIVATE USE_CMAKE)
target_include_directories(server PRIVATE
	${CMAKE_SOURCE_DIR}/contrib/duckdb/src/include
	${CMAKE_SOURCE_DIR}/third_party/gporca/libgpos/include
	${CMAKE_SOURCE_DIR}/third_party/gporca/libnaucrates/include
	${CMAKE_SOURCE_DIR}/third_party/gporca/libgpdbcost/include
	${CMAKE_SOURCE_DIR}/third_party/gporca/libgpopt/include
	${CMAKE_BINARY_DIR}/third_party/gporca/libgpos/include
	${CMAKE_SOURCE_DIR}/contrib/xerces-c/src
	${CMAKE_BINARY_DIR}/contrib/xerces-c/src
)
target_link_libraries(server PRIVATE duckdb gpopt naucrates gpos gpdbcost)
