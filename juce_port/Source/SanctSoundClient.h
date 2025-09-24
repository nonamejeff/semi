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
    juce::File getDestinationDirectory() const;

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

    void downloadFiles(const juce::StringArray& urls,
                       const std::function<void(const juce::String&)>& log) const;

    ClipSummary clipGroups(const juce::Array<juce::String>& groups,
                           const std::map<juce::String, PreviewCache>& cache,
                           const juce::StringArray& selectedBasenames,
                           LogFn log) const;

private:
    struct HourRow
    {
        juce::String url;
        juce::String fname;
        juce::String folder;
        juce::Time   startUTC;
        juce::Time   endUTC;
    };

    static bool parseTimeUTC(const juce::String& text, juce::Time& out);
    static std::vector<std::pair<juce::Time, juce::Time>> parseEventsFromCsv(const juce::File& localCsv);
    static std::vector<juce::Time> parsePresenceHoursFromCsv(const juce::File& localCsv);
    static std::vector<juce::Time> parsePresenceDaysFromCsv(const juce::File& localCsv);
    static juce::String folderFromSet(const juce::String& setName);
    static bool parseAudioStartFromName(const juce::String& name, juce::Time& outUTC);
    std::vector<juce::String> listSiteDeployments(const juce::String& site,
                                                  const std::function<void(const juce::String&)>& log) const;
    std::vector<HourRow> listAudioFilesInFolder(const juce::String& site,
                                                const juce::String& folder,
                                                juce::Optional<juce::Time> tmin,
                                                juce::Optional<juce::Time> tmax,
                                                const std::function<void(const juce::String&)>& log,
                                                juce::String* commandOut = nullptr,
                                                juce::StringArray* linesOut = nullptr) const;
    static void buildHoursFromRows(std::vector<HourRow>& rows);
    std::vector<HourRow> listAudioFilesAcross(const juce::String& site,
                                              const juce::String& preferredFolder,
                                              juce::Optional<juce::Time> tmin,
                                              juce::Optional<juce::Time> tmax,
                                              const std::function<void(const juce::String&)>& log) const;
    static void minimalUnionForWindows(const std::vector<HourRow>& files,
                                       const std::vector<std::pair<juce::Time, juce::Time>>& windows,
                                       juce::StringArray& outUrls,
                                       juce::StringArray& outNames);
    static int runAndStream(const juce::StringArray& args,
                            const std::function<void(const juce::String&)>& log);

    juce::File destinationDir;

    juce::String gcsBucket;
    juce::String audioPrefix;
    juce::String productsPrefix;

    int clipSampleRate = 48000;
    bool clipMono = true;
    juce::String clipSampleFormat = "s16";
    int clipMinBytes = 10000;

    juce::File offlineDataRoot;
    bool offlineEnabled = false;
};

} // namespace sanctsound
