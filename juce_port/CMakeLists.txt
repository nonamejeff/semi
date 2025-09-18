cmake_minimum_required(VERSION 3.16)
project(SanctSoundJuce VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(JUCE CONFIG REQUIRED)

juce_add_gui_app(SanctSoundJuceApp
    PRODUCT_NAME "SanctSound JUCE"
)

target_sources(SanctSoundJuceApp PRIVATE
    Source/Main.cpp
    Source/MainComponent.cpp
    Source/MainComponent.h
    Source/MetadataView.cpp
    Source/MetadataView.h
    Source/PreviewModels.cpp
    Source/PreviewModels.h
    Source/SanctSoundClient.cpp
    Source/SanctSoundClient.h
    Source/Utilities.cpp
    Source/Utilities.h
)

target_compile_definitions(SanctSoundJuceApp PRIVATE
    JUCE_MODAL_LOOPS_PERMITTED=1
)

target_link_libraries(SanctSoundJuceApp PRIVATE
    juce::juce_gui_extra
    juce::juce_data_structures
    juce::juce_core
)

juce_generate_juce_header(SanctSoundJuceApp)
