cmake_minimum_required(VERSION 3.19)
project(SanctSoundJuce VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(SANCTSOUND_HEADLESS "Build without JUCE GUI (for CI/Codex Linux)" OFF)

# JUCE vendored
set(JUCE_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(JUCE_BUILD_EXTRAS   OFF CACHE BOOL "" FORCE)
add_subdirectory(external/JUCE)

# Common sources (non-GUI)
set(SANCT_CORE_SOURCES
    Source/SanctSoundClient.cpp
    Source/Utilities.cpp
    Source/PreviewModels.cpp
)

if (SANCTSOUND_HEADLESS)
    # ---- Headless CLI target for CI/Codex (no X11 needed) ----
    add_executable(SanctSoundCli
        ${SANCT_CORE_SOURCES}
        Source/cli_main.cpp
    )
    target_include_directories(SanctSoundCli PRIVATE Source)
    target_link_libraries(SanctSoundCli PRIVATE
        juce::juce_core
        juce::juce_data_structures
        juce::juce_events
        juce::juce_audio_basics
        juce::juce_audio_devices
        juce::juce_dsp
        # ^ keep only non-GUI modules
    )
else()
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
        ${SANCT_CORE_SOURCES}
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
        juce::juce_core
        juce::juce_data_structures
        juce::juce_events
        juce::juce_graphics
        juce::juce_gui_basics
        juce::juce_gui_extra
        juce::juce_audio_basics
        juce::juce_audio_devices
        juce::juce_audio_utils
        juce::juce_dsp
    )

    set(JUCE_ENABLE_MODULE_SOURCE_GROUPS ON CACHE BOOL "" FORCE)
endif()
