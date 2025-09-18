# SanctSound JUCE Port

This directory contains a JUCE/C++ port of the original `sanctsound_gui` Tk
application.  The port mirrors the major workflows of the Python GUI using
native JUCE components and asynchronous tasks.  It is intended to provide a
jumping-off point for integrating SanctSound tooling into a C++ audio workflow
while retaining support for Google Cloud Storage, metadata preview, file
selection, and clip generation.

## Building

The project is organised as a CMake build that depends on JUCE being available
in the CMake package registry.  Configure JUCE (either via
`cmake --install` or `FetchContent`) and then build:

```bash
cmake -B build -S juce_port -DJUCE_DIR=/path/to/juce
cmake --build build
```

The resulting executable is `SanctSoundJuceApp`.  Run it from the root of the
repository so that configuration files match the original Python defaults.

## Features

* Site selection with friendly labels and Google Cloud Storage scanning.
* Listing detection product sets with metadata counts.
* Embedded metadata viewer that pulls JSON summaries from GCS.
* Preview step that downloads detection CSVs, computes run windows, and lists
  the minimal set of audio files required for review.
* Download and clip actions implemented with the same `gsutil` and `ffmpeg`
  calls as the Python tooling.
* Logging window, progress feedback, and persisted configuration.

## Configuration

Runtime defaults (destination directory, storage prefixes, clip format) are
mirrored from `sanctsound_gui/config.py` in `Source/SanctSoundClient.cpp`.  Edit
those constants as needed for your environment.

The C++ port uses the same command-line utilities (`gsutil`, `ffmpeg`,
`ffprobe`) as the original app.  Ensure they are on your `PATH` before running
the program.
