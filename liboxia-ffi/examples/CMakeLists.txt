cmake_minimum_required(VERSION 3.10)
project(oxia_c_example C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

include_directories(../)

add_executable(example example.c)

set(RUST_LIB_DIR "${CMAKE_SOURCE_DIR}/../../target/debug")

find_library(
  OXIA_FFI_LIBRARY
  NAMES liboxia_ffi
  PATHS ${RUST_LIB_DIR}
)

if(NOT OXIA_FFI_LIBRARY)
  message(FATAL_ERROR "Could not find liboxia_ffi. Please run 'cargo build' first.")
endif()

target_link_libraries(example PRIVATE ${OXIA_FFI_LIBRARY})
