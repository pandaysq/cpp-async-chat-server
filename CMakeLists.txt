cmake_minimum_required(VERSION 3.16)
project(cpp_async_chat_server VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(chat-server src/server/main.cpp)
add_executable(chat-client src/client/main.cpp)

target_compile_options(chat-server PRIVATE -Wall -Wextra -Wpedantic)
target_compile_options(chat-client PRIVATE -Wall -Wextra -Wpedantic)