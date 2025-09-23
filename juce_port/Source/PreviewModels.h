#pragma once

#include <juce_core/juce_core.h>
#include <map>

namespace sanctsound
{
struct ProductGroup
{
    juce::String name;
    juce::String mode;
    juce::StringArray paths;
    std::map<juce::String, int, std::less<>> extCounts;
};

struct MetadataSummary
{
    juce::String site;
    juce::String deployment;
    juce::String platform;
    juce::String recorder;
    juce::String coordinates;
    juce::String start;
    juce::String end;
    juce::String sampleRate;
    juce::String note;
    juce::String rawText;
};

struct ListedFile
{
    juce::String url;
    juce::String name;
    juce::Time start;
    juce::Time end;
    juce::String folder;
};

struct PreviewWindow
{
    juce::Time start;
    juce::Time end;
};

struct PreviewResult
{
    juce::String mode;
    juce::String summary;
    juce::String runsText;
    juce::Array<ListedFile> files;
    juce::Array<PreviewWindow> windows;
    juce::StringArray urls;
    juce::StringArray names;
};

struct ClipRow
{
    juce::String clipName;
    juce::String writtenPath;
    juce::String sourceNames;
    juce::String startIso;
    juce::String endIso;
    double durationSeconds = 0.0;
    juce::String mode;
    juce::String status;
};

struct ClipSummary
{
    int totalWindows = 0;
    int written = 0;
    int skipped = 0;
    juce::String mode;
    juce::File directory;
    juce::Array<ClipRow> manifestRows;
};

} // namespace sanctsound
