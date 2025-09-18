cmake_minimum_required(VERSION 3.19)
project(SanctSoundJuce VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- JUCE is included in the repo at external/JUCE (no internet needed) ---
# If you don't have it yet: put the JUCE source tree at external/JUCE/
# (via git subtree or unzip), and commit it.
set(JUCE_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(JUCE_BUILD_EXTRAS   OFF CACHE BOOL "" FORCE)
add_subdirectory(external/JUCE)

# --- App target ---
juce_add_gui_app(SanctSoundJuceApp
    PRODUCT_NAME "SanctSound JUCE"
    VERSION "0.1.0"
    COMPANY_NAME "Dolphin Audio"
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

# (Optional) include your headers cleanly
target_include_directories(SanctSoundJuceApp PRIVATE Source)

# Generate the JUCE module header for this target
juce_generate_juce_header(SanctSoundJuceApp)

# Useful defs; tweak as needed (platform-safe)
target_compile_definitions(SanctSoundJuceApp PRIVATE
    JUCE_MODAL_LOOPS_PERMITTED=1
    JUCE_WEB_BROWSER=0          # disable embedded webviews if unused
    JUCE_USE_CURL=0             # macOS uses SecureTransport; set 1 on Win/Linux if you link libcurl
)

# Link the JUCE modules you actually use
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

# (Optional) nicer source grouping in IDEs
set(JUCE_ENABLE_MODULE_SOURCE_GROUPS ON CACHE BOOL "" FORCE)
