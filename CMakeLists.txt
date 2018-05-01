cmake_minimum_required(VERSION 3.9)
project(ParallelHashtable C)

set(CMAKE_C_STANDARD 11)
set (CMAKE_C_FLAGS "-pthread -Wall -Wextra")

add_executable(ParallelHashtable main.c)