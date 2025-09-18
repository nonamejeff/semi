cmake_minimum_required(VERSION 3.19)
project(SanctSoundJuce VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# JUCE is included in your repo at external/JUCE/
set(JUCE_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(JUCE_BUILD_EXTRAS   OFF CACHE BOOL "" FORCE)
add_subdirectory(external/JUCE)

juce_add_gui_app(SanctSoundJuceApp
    PRODUCT_NAME "SanctSound JUCE"
    VERSION "0.1.0"
    COMPANY_NAME "Dolphin Audio"
    BUNDLE_ID com.dolphin.sanctsound
)

target_sources(SanctSoundJuceApp PRIVATE
    Source/Main.cpp
    Source/MainComponent.cpp
    Source/MetadataView.cpp
    Source/PreviewModels.cpp
    Source/SanctSoundClient.cpp
    Source/Utilities.cpp

    # headers optional, fine to list:
    Source/MainComponent.h
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
    JUCE_USE_CURL=0   # set to 1 on Win/Linux if you link libcurl
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
