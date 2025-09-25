cmake_minimum_required(VERSION 3.19)
project(SanctSoundJuce VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(SANCTSOUND_BUILD_APP "Build JUCE GUI app" ON)
option(SANCTSOUND_BUILD_CLI "Build headless CLI" ON)

# JUCE vendored
set(JUCE_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(JUCE_BUILD_EXTRAS   OFF CACHE BOOL "" FORCE)
if (SANCTSOUND_BUILD_CLI AND NOT SANCTSOUND_BUILD_APP)
    set(JUCE_MODULES_ONLY ON CACHE BOOL "" FORCE)
else()
    set(JUCE_MODULES_ONLY OFF CACHE BOOL "" FORCE)
endif()
add_subdirectory(external/JUCE)

# Core library sources that do NOT require GUI modules
set(SANCTSOUND_CORE_SOURCES
    Source/SanctSoundClient.cpp
    Source/Utilities.cpp
    Source/PreviewModels.cpp
)

add_library(sanctsound_core STATIC ${SANCTSOUND_CORE_SOURCES})
target_include_directories(sanctsound_core PUBLIC Source)
target_compile_definitions(sanctsound_core PUBLIC
    JUCE_GLOBAL_MODULE_SETTINGS_INCLUDED=1
    JUCE_DISABLE_JUCE_VERSION_PRINTING=1
    JUCE_USE_CURL=0
    JUCE_WEB_BROWSER=0
)
target_link_libraries(sanctsound_core PUBLIC
    juce::juce_core
    juce::juce_data_structures
    juce::juce_audio_basics
    juce::juce_audio_formats
)

if (SANCTSOUND_BUILD_CLI)
    add_executable(sanctsound_cli
        cli/sanctsound_cli.cpp
    )
    target_link_libraries(sanctsound_cli PRIVATE sanctsound_core)
endif()

if (SANCTSOUND_BUILD_APP)
    # ---- Full GUI app for macOS/local ----
    juce_add_gui_app(SanctSoundJuceApp
        PRODUCT_NAME "SanctSound JUCE"
        VERSION "0.1.0"
        COMPANY_NAME "Dolphin Audio"
        BUNDLE_ID com.dolphin.sanctsound
    )

    target_sources(SanctSoundJuceApp PRIVATE
        Source/Main.cpp
        Source/MainComponent.cpp
        Source/MainComponent.h
        Source/MetadataView.cpp
        Source/MetadataView.h
        Source/PreviewModels.h
        Source/SanctSoundClient.h
        Source/Utilities.h
    )

    target_include_directories(SanctSoundJuceApp PRIVATE Source)

    juce_generate_juce_header(SanctSoundJuceApp)

    target_compile_definitions(SanctSoundJuceApp PRIVATE
        JUCE_MODAL_LOOPS_PERMITTED=1
        JUCE_WEB_BROWSER=0
        JUCE_USE_CURL=0
    )

    target_link_libraries(SanctSoundJuceApp PRIVATE
        sanctsound_core
        juce::juce_core
        juce::juce_data_structures
        juce::juce_events
        juce::juce_graphics
        juce::juce_gui_basics
        juce::juce_gui_extra
        juce::juce_audio_basics
        juce::juce_audio_devices
        juce::juce_audio_utils
        juce::juce_audio_formats
        juce::juce_dsp
    )

    set(JUCE_ENABLE_MODULE_SOURCE_GROUPS ON CACHE BOOL "" FORCE)
endif()
