#pragma once

#include <juce_core/juce_core.h>
#include <functional>
#include <map>
#include <utility>
#include <vector>

#include "PreviewModels.h"

namespace sanctsound
{
struct PreviewCache
{
    juce::String mode;
    juce::Array<PreviewWindow> windows;
};

class SanctSoundClient
{
public:
    using LogFn = std::function<void(const juce::String&)>;

    struct AudioSpan
    {
        juce::String object;
        juce::Time start;
        juce::Time end;
    };

    struct AudioListingResult
    {
        juce::String prefix;
        int totalListed = 0;
        std::vector<juce::String> uniqueObjects;
        juce::StringArray sampleAll;
        juce::StringArray sampleKept;
        juce::StringArray sampleDropped;
        std::vector<AudioSpan> spans;
    };

    SanctSoundClient();

    bool setDestinationDirectory(const juce::File& directory);
    const juce::File& getDestinationDirectory() const;

    juce::StringArray siteLabels() const;
    juce::String codeForLabel(const juce::String& label) const;

    std::vector<ProductGroup> listProductGroups(const juce::String& site,
                                                const juce::String& tag,
                                                LogFn log) const;

    MetadataSummary fetchMetadataSummary(const juce::String& site,
                                         const juce::String& group,
                                         juce::String& rawText,
                                         LogFn log) const;

    PreviewResult previewGroup(const juce::String& site,
                               const ProductGroup& group,
                               bool onlyLongRuns,
                               LogFn log) const;

    void setOfflineDataRoot(const juce::File& directory);
    const juce::File& getOfflineDataRoot() const;

    AudioListingResult listAudioObjectsForGroup(const juce::String& site,
                                                const juce::String& groupName,
                                                LogFn log) const;

    void downloadFiles(const juce::StringArray& urls, LogFn log) const;

    ClipSummary clipGroups(const juce::Array<juce::String>& groups,
                           const std::map<juce::String, PreviewCache>& cache,
                           const juce::StringArray& selectedBasenames,
                           LogFn log) const;

private:
    juce::File destinationDir;

    juce::String gcsBucket;
    juce::String audioPrefix;
    juce::String productsPrefix;

    int clipSampleRate = 48000;
    bool clipMono = true;
    juce::String clipSampleFormat = "s16";

    juce::File offlineDataRoot;
    bool offlineEnabled = false;
};

} // namespace sanctsound
