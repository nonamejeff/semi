#pragma once

#include <juce_core/juce_core.h>
#include <functional>
#include <map>
#include <optional>
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

    struct EventWindow
    {
        juce::Time startUTC;
        juce::Time endUTC;
    };

    struct AudioHour
    {
        juce::String url, fname, folder;
        juce::Time   startUtc, endUtc; // [start, end) in UTC
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

    // === Parsing helpers (public, used by GUI & CLI) ===
    // Parse UTC start time from audio filename.
    // Supports "..._YYYYMMDDThhmmssZ.flac" and "..._YYMMDDhhmmss.flac" at the end.
    // Returns true on success and writes UTC juce::Time to outUtc.
    static bool parseAudioStartFromName (const juce::String& fileName, juce::Time& outUtc);
    // Small helper to format a juce::Time in ISO8601 UTC for logs.
    static juce::String toIsoUTC (const juce::Time& t);
    /** Returns "sanctsound_ci01_02" from "sanctsound_ci01_02_*" (lowercased),
        or empty string if no match. */
    static juce::String folderFromSetName(const juce::String& setName);

    static bool parseEventsFromCsv(const juce::File& csvFile, juce::Array<EventWindow>& out);

private:
    static bool parseTimeUTC(const juce::String& text, juce::Time& out);
    static std::vector<juce::Time> parsePresenceHoursFromCsv(const juce::File& localCsv);
    static std::vector<juce::Time> parsePresenceDaysFromCsv(const juce::File& localCsv);

    struct AudioHourSorter
    {
        int compareElements(const AudioHour& a, const AudioHour& b) const;
    };

    juce::StringArray listDeploymentsForSite(const juce::String& site,
                                             const std::function<void(const juce::String&)>& log) const;
    juce::Array<AudioHour> listAudioInFolder(const juce::String& site,
                                             const juce::String& folder,
                                             std::optional<juce::Time> tmin,
                                             std::optional<juce::Time> tmax,
                                             const std::function<void(const juce::String&)>& log,
                                             const juce::File& debugDir = {}) const;
    juce::Array<AudioHour> listAudioAcross(const juce::String& site,
                                          const juce::String& preferFolder,
                                          std::optional<juce::Time> tmin,
                                          std::optional<juce::Time> tmax,
                                          const std::function<void(const juce::String&)>& log,
                                          const juce::File& debugDir = {}) const;
    struct NeededFileRow
    {
        juce::Time start;
        juce::Time end;
        juce::String names;
        juce::String urls;
    };

    static void minimalFilesForWindows(const juce::Array<AudioHour>& files,
                                       const std::vector<std::pair<juce::Time, juce::Time>>& windows,
                                       juce::StringArray& outUrls,
                                       juce::StringArray& outNames,
                                       juce::Array<NeededFileRow>& rows,
                                       int& unmatchedCount,
                                       const juce::File& debugDir = {});

    juce::File makePreviewDebugDir (const juce::File& destDir, const juce::String& setName) const;
    juce::StringArray rowsToUrls (const juce::Array<AudioHour>& rows) const;
    static void dumpLines (const juce::File& outFile, const juce::StringArray& lines);

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
