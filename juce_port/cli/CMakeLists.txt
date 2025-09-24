cmake_minimum_required(VERSION 3.19)

# If the top-level already added JUCE via add_subdirectory(external/JUCE), we don't repeat it here.

if (NOT COMMAND juce_add_console_app)
    include(${JUCE_SOURCE_DIR}/extras/Build/CMake/JUCEUtils.cmake)
endif()

juce_add_console_app(sanctsound_cli
    PRODUCT_NAME "SanctSound CLI"
)

target_sources(sanctsound_cli PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/main_cli.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../Source/SanctSoundClient.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../Source/Utilities.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../Source/PreviewModels.cpp
)

target_include_directories(sanctsound_cli PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../Source
)

# Link only non-GUI JUCE modules to avoid X11/WebKit
target_link_libraries(sanctsound_cli PRIVATE
    juce::juce_core
    juce::juce_data_structures
    juce::juce_events
)

target_compile_definitions(sanctsound_cli PRIVATE
    JUCE_WEB_BROWSER=0
    JUCE_USE_XRANDR=0
    JUCE_USE_CURL=0
    JUCE_DISPLAY_SPLASH_SCREEN=0
    JUCE_MODAL_LOOPS_PERMITTED=0
)
