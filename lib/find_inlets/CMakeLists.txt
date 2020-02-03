cmake_minimum_required(VERSION 2.8)
project(find_inlets CXX)

if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      set (CMAKE_CXX_FLAGS "-std=c++0x ${CMAKE_CXX_FLAGS}")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 11)
endif ()

# GDAL is required
find_package(GDAL REQUIRED)
include_directories(${GDAL_INCLUDE_DIR})

add_executable (find_inlets_mr src/find_inlets.cpp)
add_executable (find_inlets_hr src/find_inlets_hr.cpp)

target_link_libraries(find_inlets_mr ${GDAL_LIBRARY})
target_link_libraries(find_inlets_hr ${GDAL_LIBRARY})

install(TARGETS find_inlets_mr DESTINATION bin)
install(TARGETS find_inlets_hr DESTINATION bin)

