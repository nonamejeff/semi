cmake_minimum_required(VERSION 3.19)
project(SanctSound LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Release by default unless generator supplies one
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# macOS baseline so users with older SDKs can run the app
set(CMAKE_OSX_DEPLOYMENT_TARGET "11.0")

# Clang warnings: helpful, but don’t fail on deprecations
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
  add_compile_options(
    -Wall -Wextra -Wpedantic
    -Wno-deprecated-declarations
    -Wno-unknown-pragmas
  )
endif()

add_subdirectory(juce_port)
