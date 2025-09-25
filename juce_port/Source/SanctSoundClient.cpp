#include "SanctSoundClient.h"

#include "Utilities.h"

#include <juce_core/juce_core.h>
#include <juce_data_structures/juce_data_structures.h>
#include <juce_audio_basics/juce_audio_basics.h>
#include <juce_audio_formats/juce_audio_formats.h>

#include <algorithm>
#include <cmath>
#include <ctime>
#include <cstdio>
#include <map>
#include <memory>
#include <regex>
#include <string>
#include <stdexcept>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <limits>
#include <vector>
#include <optional>

// Portable timegm fallback (macOS has timegm; keep a fallback just in case).
static std::time_t ss_timegm (std::tm* tm)
{
#if defined(_WIN32)
    return _mkgmtime (tm);
#else
    return timegm (tm);
#endif
}

juce::String sanctsound::SanctSoundClient::toIsoUTC (const juce::Time& t)
{
    const auto millis = t.toMilliseconds();
    const std::time_t seconds = static_cast<std::time_t> (millis / 1000);

    std::tm tm{};
#if JUCE_WINDOWS
    gmtime_s (&tm, &seconds);
#else
    gmtime_r (&seconds, &tm);
#endif

    char buffer[64];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02dZ",
                  tm.tm_year + 1900,
                  tm.tm_mon + 1,
                  tm.tm_mday,
                  tm.tm_hour,
                  tm.tm_min,
                  tm.tm_sec);
    return juce::String(buffer);
}

bool sanctsound::SanctSoundClient::parseAudioStartFromName (const juce::String& fileName, juce::Time& outUtc)
{
    // Work with the basename
    const juce::String base = juce::File (fileName).getFileName();

    // 1) Suffix: _YYYYMMDDThhmmssZ.(flac|wav)
    {
        static const std::regex re1 (R"(_(\d{8}T\d{6}Z)\.(flac|wav)$)", std::regex::icase);
        std::cmatch m;
        const std::string s = base.toStdString();
        if (std::regex_search (s.c_str(), m, re1))
        {
            const std::string iso = m[1];
            // Parse yyyy-mm-ddThh:mm:ssZ manually to UTC
            int yr, mo, dy, hh, mm, ss;
            if (std::sscanf (iso.c_str(), "%4d%2d%2dT%2d%2d%2dZ", &yr, &mo, &dy, &hh, &mm, &ss) == 6)
            {
                std::tm tm{}; tm.tm_year = yr - 1900; tm.tm_mon = mo - 1; tm.tm_mday = dy;
                tm.tm_hour = hh; tm.tm_min = mm; tm.tm_sec = ss;
                const std::time_t tt = ss_timegm (&tm);
                if (tt > 0)
                {
                    outUtc = juce::Time ((juce::int64) tt * 1000);
                    return true;
                }
            }
        }
    }

    // 2) Suffix: _YYMMDDhhmmss.(flac|wav)
    {
        static const std::regex re2 (R"(_(\d{12})\.(flac|wav)$)", std::regex::icase);
        std::cmatch m;
        const std::string s = base.toStdString();
        if (std::regex_search (s.c_str(), m, re2))
        {
            const std::string d = m[1];
            int YY, MO, DD, hh, mm, ss;
            if (std::sscanf (d.c_str(), "%2d%2d%2d%2d%2d%2d", &YY, &MO, &DD, &hh, &mm, &ss) == 6)
            {
                const int year = (YY <= 69 ? 2000 + YY : 1900 + YY); // same rule as the Python GUI
                std::tm tm{}; tm.tm_year = year - 1900; tm.tm_mon = MO - 1; tm.tm_mday = DD;
                tm.tm_hour = hh; tm.tm_min = mm; tm.tm_sec = ss;
                const std::time_t tt = ss_timegm (&tm);
                if (tt > 0)
                {
                    outUtc = juce::Time ((juce::int64) tt * 1000);
                    return true;
                }
            }
        }
    }

    return false; // no pattern matched
}

namespace
{

// Make a juce::Time from UTC components safely
static juce::Time makeUtc(int year, int mon, int day, int hh, int mm, int ss)
{
    std::tm tm{}; tm.tm_year = year - 1900; tm.tm_mon = mon - 1; tm.tm_mday = day;
    tm.tm_hour = hh; tm.tm_min = mm; tm.tm_sec = ss;
#if JUCE_WINDOWS
    const auto t = _mkgmtime(&tm);
#else
    const auto t = timegm(&tm);
#endif
    if (t == -1) return juce::Time((juce::int64) 0);
    return juce::Time((juce::int64) t * 1000);
}

} // anonymous

namespace sanctsound
{
bool timeLessThan(const juce::Time& a, const juce::Time& b);
bool timeLessThanOrEqual(const juce::Time& a, const juce::Time& b);
bool isNumeric(const juce::String& text);

namespace
{
juce::var fetchGcsJson(const juce::String& bucket,
                       const juce::String& prefix,
                       const juce::String& delimiter = "/")
{
    const juce::String url =
        "https://storage.googleapis.com/storage/v1/b/" + bucket +
        "/o?prefix=" + juce::URL::addEscapeChars(prefix, true) +
        "&delimiter=" + juce::URL::addEscapeChars(delimiter, true);

    juce::URL request(url);
    auto stream = request.createInputStream(juce::URL::InputStreamOptions(juce::URL::ParameterHandling::inAddress)
                                                .withConnectionTimeoutMs(15000));

    if (stream == nullptr)
        throw std::runtime_error("Failed to fetch GCS JSON: no response stream");

    const juce::String jsonText = stream->readEntireStreamAsString();
    const juce::var parsed = juce::JSON::parse(jsonText);

    if (parsed.isVoid() || parsed.isUndefined())
        throw std::runtime_error("Failed to parse GCS JSON response");

    return parsed;
}

const juce::StringArray kKnownCodes {
    "ci01","ci02","ci03","ci04","ci05",
    "fk01","fk02","fk03","fk04",
    "gr01","gr02","gr03",
    "hi01","hi03","hi04","hi05","hi06",
    "mb01","mb02","mb03",
    "oc01","oc02","oc03","oc04",
    "pm01","pm02","pm05",
    "sb01","sb02","sb03"
};

const std::map<juce::String, juce::String, std::less<>> kSitePrefixName {
    { "ci", "Channel Islands" },
    { "fk", "Florida Keys" },
    { "gr", "Gray's Reef" },
    { "hi", "Hawaiian Islands" },
    { "mb", "Monterey Bay" },
    { "oc", "Olympic Coast" },
    { "pm", "Papah\u0101naumoku\u0101kea" },
    { "sb", "Stellwagen Bank" }
};

juce::var fetchGcsJsonInternal(const juce::String& bucket,
                               const juce::String& prefix,
                               const juce::String& delimiter,
                               const juce::String& pageToken)
{
    juce::String url =
        "https://storage.googleapis.com/storage/v1/b/" + bucket +
        "/o?prefix=" + juce::URL::addEscapeChars(prefix, true) +
        "&delimiter=" + juce::URL::addEscapeChars(delimiter, true);

    if (pageToken.isNotEmpty())
        url += "&pageToken=" + juce::URL::addEscapeChars(pageToken, true);

    juce::URL u(url);
    auto stream = u.createInputStream(juce::URL::InputStreamOptions(juce::URL::ParameterHandling::inAddress)
                                          .withConnectionTimeoutMs(15000));
    if (stream == nullptr)
        throw std::runtime_error("GCS request failed: no stream");

    juce::String jsonText = stream->readEntireStreamAsString();
    juce::var v = juce::JSON::parse(jsonText);
    if (v.isVoid() || v.isUndefined())
        throw std::runtime_error("GCS parse failed");
    return v;
}

juce::var fetchGcsJsonPage(const juce::String& bucket,
                           const juce::String& prefix,
                           const juce::String& delimiter,
                           const juce::String& pageToken)
{
    return fetchGcsJsonInternal(bucket, prefix, delimiter, pageToken);
}

juce::var fetchGcsJsonFirstPage(const juce::String& bucket,
                                const juce::String& prefix,
                                const juce::String& delimiter = "/")
{
    return fetchGcsJsonInternal(bucket, prefix, delimiter, {});
}

static bool hasAudioExt(const juce::String& name)
{
    auto lower = name.toLowerCase();
    return lower.endsWith(".flac") || lower.endsWith(".wav");
}

static juce::String normKey(const juce::String& name)
{
    return name.toLowerCase().removeCharacters("\r\n");
}

static void writeLines(const juce::File& f, const juce::StringArray& lines)
{
    f.deleteFile();
    f.create();
    f.appendText(lines.joinIntoString("\n") + "\n");
}

static juce::StringArray runAndCapture(const juce::StringArray& cmd, int& exitCode)
{
    juce::StringArray out;
    juce::ChildProcess p;
    exitCode = -999;
    if (!p.start(cmd)) return out;

    juce::MemoryOutputStream buffer;
    for (;;)
    {
        char tmp[8192];
        const int n = p.readProcessOutput(tmp, sizeof(tmp));
        if (n <= 0) break;
        buffer.write(tmp, (size_t)n);
    }
    exitCode = p.waitForProcessToFinish(120000);
    out.addLines(buffer.toString());
    return out;
}

// List deployment folders: gs://.../audio/<site>/ → keep entries that end with '/'
static juce::StringArray gcsListFolders(const juce::String& siteLower, int& lsExit)
{
    juce::StringArray cmd { "gsutil", "ls", "gs://noaa-passive-bioacoustic/sanctsound/audio/" + siteLower + "/" };
    auto lines = runAndCapture(cmd, lsExit);

    juce::StringArray folders;
    for (auto& L : lines)
    {
        auto s = L.trim();
        if (s.endsWithChar('/'))
        {
            auto parts = juce::StringArray::fromTokens(s, "/", "");
            if (parts.size() >= 2)
            {
                auto name = parts[parts.size()-2];
                if (name.startsWithIgnoreCase("sanctsound_"))
                    folders.addIfNotAlreadyThere(name.toLowerCase());
            }
        }
    }
    folders.sort(true);
    return folders;
}

// Dump *all* audio files for <site> across all deployments to files on disk.
static void dumpAllAudioForSite(const juce::String& siteLower, const juce::File& dbgDir)
{
    int exitCodeFolders = 0;
    auto folders = gcsListFolders(siteLower, exitCodeFolders);

    // Record folder listing & exit code
    juce::StringArray meta;
    meta.add("CMD: gsutil ls gs://noaa-passive-bioacoustic/sanctsound/audio/" + siteLower + "/");
    meta.add("EXIT: " + juce::String(exitCodeFolders));
    meta.add("FOLDERS(" + juce::String(folders.size()) + "): " + folders.joinIntoString(", "));
    writeLines(dbgDir.getChildFile("__folders.txt"), meta);

    juce::StringArray all; // all URLs (every folder)
    for (auto folder : folders)
    {
        const juce::String pattern =
            "gs://noaa-passive-bioacoustic/sanctsound/audio/" + siteLower + "/" + folder + "/audio/*.flac";

        int exitCode = 0;
        juce::StringArray cmd { "gsutil", "ls", pattern };
        auto lines = runAndCapture(cmd, exitCode);

        juce::StringArray urls;
        for (auto& L : lines)
        {
            auto s = L.trim();
            if (s.startsWith("gs://") && s.endsWithIgnoreCase(".flac"))
                urls.add(s);
        }

        writeLines(dbgDir.getChildFile("ALL_" + folder + "_urls.txt"), urls);
        all.addArray(urls);
    }

    all.removeEmptyStrings(true);
    all.removeDuplicates(true);
    all.sort(true);
    writeLines(dbgDir.getChildFile("ALL_" + siteLower + "_urls.txt"), all);
}

// After your selection/filter step, dump those URLs too.
static void dumpFilteredSelection(const juce::String& setName, const juce::StringArray& selectedUrls, const juce::File& dbgDir)
{
    juce::StringArray clean = selectedUrls;
    clean.removeEmptyStrings(true);
    clean.removeDuplicates(true);
    clean.sort(true);
    writeLines(dbgDir.getChildFile("FILTERED_" + setName + "_urls.txt"), clean);
}

struct CsvWindow
{
    juce::Time start;
    juce::Time end;
    juce::String url;
    juce::String source;
};


static juce::String normaliseHeaderName(const juce::String& header)
{
    auto lower = header.trim().toLowerCase();
    juce::String cleaned;
    for (auto ch : lower)
    {
        if (juce::CharacterFunctions::isLetterOrDigit(ch))
            cleaned << ch;
    }
    return cleaned;
}

static int findColumnByAliases(const juce::StringArray& header,
                               const std::vector<juce::String>& aliases)
{
    if (header.isEmpty() || aliases.empty())
        return -1;

    juce::Array<juce::String> normalisedHeader;
    normalisedHeader.ensureStorageAllocated(header.size());
    for (auto& h : header)
        normalisedHeader.add(normaliseHeaderName(h));

    juce::Array<juce::String> normalisedAliases;
    normalisedAliases.ensureStorageAllocated((int) aliases.size());
    for (auto& alias : aliases)
        normalisedAliases.add(normaliseHeaderName(alias));

    for (int col = 0; col < normalisedHeader.size(); ++col)
    {
        auto& candidate = normalisedHeader.getReference(col);
        for (auto& alias : normalisedAliases)
            if (candidate == alias)
                return col;
    }

    for (int col = 0; col < normalisedHeader.size(); ++col)
    {
        auto& candidate = normalisedHeader.getReference(col);
        for (auto& alias : normalisedAliases)
            if (candidate.contains(alias))
                return col;
    }

    return -1;
}

static int findDurationColumn(const juce::StringArray& header)
{
    for (int col = 0; col < header.size(); ++col)
    {
        auto normalised = normaliseHeaderName(header[col]);
        if (normalised.contains("duration") || normalised.contains("dur") || normalised.contains("length"))
            return col;
    }
    return -1;
}

juce::String stampForFilename(const juce::Time& t)
{
    return t.formatted("%Y%m%dT%H%M%S");
}

} // namespace

static bool parseTimeUTCImpl(const juce::String& text, juce::Time& out)
{
    auto trimmed = text.trim();
    if (trimmed.isEmpty())
        return false;

    auto digitsOnly = trimmed.containsOnly("0123456789");
    if (digitsOnly)
    {
        auto yearCandidate = trimmed.substring(0, 4).getIntValue();
        if (trimmed.length() >= 14 && yearCandidate >= 1900 && yearCandidate <= 2200)
        {
            int month  = trimmed.substring(4, 6).getIntValue();
            int day    = trimmed.substring(6, 8).getIntValue();
            int hour   = trimmed.substring(8, 10).getIntValue();
            int minute = trimmed.substring(10, 12).getIntValue();
            int second = trimmed.substring(12, 14).getIntValue();
            juce::Time base(yearCandidate, month, day, hour, minute, second, 0, false);
            if (trimmed.length() > 14)
            {
                auto fractionalDigits = trimmed.substring(14);
                double fractionalSeconds = 0.0;
                if (fractionalDigits.containsOnly("0123456789"))
                {
                    auto fractionInt = fractionalDigits.getLargeIntValue();
                    double scale = std::pow(10.0, fractionalDigits.length());
                    fractionalSeconds = fractionInt / scale;
                }
                base = base + juce::RelativeTime::seconds(fractionalSeconds);
            }
            out = base;
            return true;
        }

        auto value = trimmed.getLargeIntValue();
        bool treatAsMillis = trimmed.length() > 10 || value >= 1000000000000LL;
        juce::int64 millis = treatAsMillis ? value : (value * 1000);
        out = juce::Time(millis);
        return true;
    }

    auto numericWithDot = trimmed.containsOnly("0123456789.");
    if (numericWithDot && trimmed.containsChar('.'))
    {
        auto value = trimmed.getDoubleValue();
        if (value != 0.0 || trimmed.startsWith("0") || trimmed.startsWithChar('.'))
        {
            auto millis = static_cast<juce::int64>(std::llround(value * 1000.0));
            out = juce::Time(millis);
            return true;
        }
    }

    auto addCompactCandidate = [](juce::StringArray& list, const juce::String& candidate)
    {
        auto trimmedCandidate = candidate.trim();
        if (trimmedCandidate.isEmpty())
            return;
        auto digits = trimmedCandidate.retainCharacters("0123456789");
        if (digits.length() >= 14)
        {
            auto year   = digits.substring(0, 4);
            auto month  = digits.substring(4, 6);
            auto day    = digits.substring(6, 8);
            auto hour   = digits.substring(8, 10);
            auto minute = digits.substring(10, 12);
            auto second = digits.substring(12, 14);
            juce::String iso = year + "-" + month + "-" + day + "T" + hour + ":" + minute + ":" + second;
            if (digits.length() > 14)
                iso << "." << digits.substring(14);
            iso << "Z";
            if (! list.contains(iso))
                list.add(iso);
        }
    };

    juce::StringArray candidates;
    auto addCandidate = [&](const juce::String& candidate)
    {
        auto value = candidate.trim();
        if (value.isNotEmpty() && ! candidates.contains(value))
            candidates.add(value);
    };

    addCandidate(trimmed);
    addCandidate(trimmed.replaceCharacter('/', '-'));
    if (trimmed.containsChar(' '))
        addCandidate(trimmed.replaceCharacter(' ', 'T'));

    auto canonical = trimmed.replaceCharacter('/', '-');
    if (canonical.containsChar(' '))
        addCandidate(canonical.replaceCharacter(' ', 'T'));

    auto currentSize = candidates.size();
    for (int i = 0; i < currentSize; ++i)
    {
        auto candidate = candidates[i];
        if (! candidate.endsWithIgnoreCase("Z"))
            addCandidate(candidate + "Z");
    }

    addCompactCandidate(candidates, trimmed);
    addCompactCandidate(candidates, canonical);

    for (auto candidate : candidates)
    {
        auto parsed = juce::Time::fromISO8601(candidate);
        if (parsed == juce::Time())
        {
            if (! candidate.containsIgnoreCase("1970-01-01"))
                continue;
        }
        out = parsed;
        return true;
    }

    return false;
}

static bool parseAudioTimesFromName(const juce::String& name,
                                    juce::Time& start,
                                    juce::Time& end)
{
    auto base = name.fromLastOccurrenceOf("/", false, false);
    if (base.isEmpty())
        base = name;

    auto dot = base.lastIndexOfChar('.');
    if (dot <= 0)
        return false;

    auto stem = base.substring(0, dot);
    juce::StringArray parts;
    parts.addTokens(stem, "_", "");
    parts.removeEmptyStrings();
    if (parts.size() < 4)
        return false;

    auto startToken = parts[parts.size() - 2];
    auto endToken   = parts[parts.size() - 1];

    juce::Time parsedStart;
    juce::Time parsedEnd;
    if (! parseTimeUTCImpl(startToken, parsedStart))
        return false;
    if (! parseTimeUTCImpl(endToken, parsedEnd))
        return false;

    if (! timeLessThan(parsedStart, parsedEnd))
    {
        if (parsedStart == parsedEnd)
            parsedEnd = parsedStart + juce::RelativeTime::seconds(1);
        else if (timeLessThan(parsedEnd, parsedStart))
            std::swap(parsedStart, parsedEnd);
    }

    start = parsedStart;
    end   = parsedEnd;
    return true;
}

static std::vector<CsvWindow> parseCsvWindows(const juce::Array<juce::File>& csvFiles)
{
    std::vector<CsvWindow> windows;

    const std::vector<juce::String> startAliases { "start_time", "start", "start_utc", "begin", "window_start" };
    const std::vector<juce::String> endAliases   { "end_time", "end", "end_utc", "finish", "window_end" };
    const std::vector<juce::String> urlAliases   { "audio_url", "url", "gcs_url", "gcs_uri" };

    for (auto& csv : csvFiles)
    {
        auto table = readCsvFile(csv);
        if (table.rows.isEmpty())
            continue;

        auto startCol = findColumnByAliases(table.header, startAliases);
        if (startCol < 0)
            throw std::runtime_error("No usable start column in " + csv.getFileName().toStdString());

        auto endCol = findColumnByAliases(table.header, endAliases);
        auto urlCol = findColumnByAliases(table.header, urlAliases);
        int durationCol = -1;
        if (endCol < 0)
            durationCol = findDurationColumn(table.header);

        for (int rowIdx = 0; rowIdx < table.rows.size(); ++rowIdx)
        {
            auto& row = table.rows.getReference(rowIdx);
            if (startCol >= row.size())
                continue;

            juce::Time startTime;
            if (! parseTimeUTCImpl(row[startCol], startTime))
                continue;

            juce::Time endTime;
            bool haveEnd = false;
            if (endCol >= 0 && endCol < row.size())
                haveEnd = parseTimeUTCImpl(row[endCol], endTime);

            if (! haveEnd)
            {
                double durationSeconds = 0.0;
                if (durationCol >= 0 && durationCol < row.size() && isNumeric(row[durationCol]))
                    durationSeconds = std::max(0.0, row[durationCol].getDoubleValue());
                if (durationSeconds <= 0.0)
                    durationSeconds = 60.0;
                endTime = startTime + juce::RelativeTime::seconds(durationSeconds);
            }

            if (! timeLessThan(startTime, endTime))
                endTime = startTime + juce::RelativeTime::seconds(1);

            juce::String url;
            if (urlCol >= 0 && urlCol < row.size())
                url = row[urlCol].trim();

            windows.push_back({ startTime, endTime, url, csv.getFileName() });
        }
    }

    std::sort(windows.begin(), windows.end(), [](const CsvWindow& a, const CsvWindow& b)
    {
        if (timeLessThan(a.start, b.start)) return true;
        if (timeLessThan(b.start, a.start)) return false;
        return a.source.compareIgnoreCase(b.source) < 0;
    });

    return windows;
}

static void gcsListRecursive(const juce::String& bucket,
                             const juce::String& prefix,
                             juce::StringArray& outObjects,
                             std::unordered_set<std::string>& visited)
{
    const auto key = prefix.toStdString();
    if (! visited.insert(key).second)
        return;

    juce::String pageToken;
    for (;;)
    {
        juce::String url = "https://storage.googleapis.com/storage/v1/b/" + bucket
                           + "/o?prefix=" + juce::URL::addEscapeChars(prefix, true)
                           + "&delimiter=/";
        if (pageToken.isNotEmpty())
            url << "&pageToken=" << juce::URL::addEscapeChars(pageToken, true);

        juce::URL u(url);
        auto stream = u.createInputStream(juce::URL::InputStreamOptions(juce::URL::ParameterHandling::inAddress)
                                              .withConnectionTimeoutMs(15000));
        if (stream == nullptr)
            throw std::runtime_error("GCS list failed: " + url.toStdString());
        auto v = juce::JSON::parse(stream->readEntireStreamAsString());
        if (v.isVoid() || v.isUndefined())
            throw std::runtime_error("GCS parse failed: " + url.toStdString());

        if (auto* items = v.getProperty("items", juce::var()).getArray())
        {
            for (auto& it : *items)
            {
                if (it.hasProperty("name"))
                    outObjects.add(it.getProperty("name", juce::String()));
            }
        }

        if (auto* prefs = v.getProperty("prefixes", juce::var()).getArray())
        {
            for (auto& p : *prefs)
                gcsListRecursive(bucket, p.toString(), outObjects, visited);
        }

        auto pt = v.getProperty("nextPageToken", juce::var());
        if (pt.isString() && pt.toString().isNotEmpty())
            pageToken = pt.toString();
        else
            break;
    }
}

juce::String makeGsUrl(const juce::String& bucket, const juce::String& objectName)
{
    return "gs://" + bucket + "/" + objectName;
}

juce::String makeHttpsUrl(const juce::String& bucket, const juce::String& objectName)
{
    return "https://storage.googleapis.com/" + bucket + "/" + juce::URL::addEscapeChars(objectName, false);
}

bool parseGsUrl(const juce::String& url, juce::String& bucketOut, juce::String& objectOut)
{
    auto trimmed = url.trim();
    if (! trimmed.startsWithIgnoreCase("gs://"))
        return false;

    auto remainder = trimmed.substring(5);
    auto slash = remainder.indexOfChar('/');
    if (slash < 0)
        return false;

    bucketOut = remainder.substring(0, slash);
    objectOut = remainder.substring(slash + 1);
    return bucketOut.isNotEmpty() && objectOut.isNotEmpty();
}

static void ensureParentDir(const juce::File& f)
{
    auto parent = f.getParentDirectory();
    if (parent.exists())
    {
        if (! parent.isDirectory())
            throw std::runtime_error("Failed to create directory: " + parent.getFullPathName().toStdString());
        return;
    }

    if (! parent.createDirectory())
        throw std::runtime_error("Failed to create directory: " + parent.getFullPathName().toStdString());
}

static void writeLines(const juce::File& f, const juce::StringArray& lines)
{
    ensureParentDir(f);
    juce::FileOutputStream os(f);
    if (! os.openedOk())
        return;
    for (auto& s : lines)
        os.writeText(s + "\n", false, false, "\n");
    os.flush();
}

static void appendLine(const juce::File& f, const juce::String& line)
{
    if (f.getFullPathName().isEmpty())
        return;

    ensureParentDir(f);
    juce::FileOutputStream os(f, true);
    if (! os.openedOk())
        return;
    os.writeText(line + "\n", false, false, "\n");
    os.flush();
}

static void overwriteLines(const juce::File& f, const juce::StringArray& lines)
{
    if (f.getFullPathName().isEmpty())
        return;

    ensureParentDir(f);
    juce::FileOutputStream os(f);
    if (! os.openedOk())
        return;
    for (auto& s : lines)
        os.writeText(s + "\n", false, false, "\n");
    os.flush();
}

static void writeTextFile(const juce::File& f, const juce::String& text)
{
    ensureParentDir(f);
    juce::FileOutputStream out(f);
    if (! out.openedOk())
        throw std::runtime_error("Failed to open file for writing: " + f.getFullPathName().toStdString());
    out.writeText(text, false, false, "\n");
    out.flush();
    if (out.getStatus().failed())
        throw std::runtime_error("Write failed: " + out.getStatus().getErrorMessage().toStdString());
}

static juce::var readJsonFile(const juce::File& file)
{
    if (! file.existsAsFile())
        return {};
    auto text = file.loadFileAsString();
    if (text.isEmpty())
        return {};
    auto parsed = juce::JSON::parse(text);
    if (parsed.isVoid() || parsed.isUndefined())
        return {};
    return parsed;
}

juce::String siteLabelForCode(const juce::String& code)
{
    auto c = code.trim().toLowerCase();
    auto prefix = c.substring(0, 2);
    auto friendly = kSitePrefixName.count(prefix) ? kSitePrefixName.at(prefix) : prefix.toUpperCase();
    return friendly + " - " + c.toUpperCase();
}

juce::String labelToCode(const juce::String& label)
{
    const auto emDash = juce::String::charToString(0x2014);
    if (label.contains(emDash))
        return label.fromLastOccurrenceOf(emDash, false, false).trim().toLowerCase();
    if (label.contains("-"))
        return label.fromLastOccurrenceOf("-", false, false).trim().toLowerCase();
    return label.trim().toLowerCase();
}

juce::StringArray chooseBestFiles(const juce::StringArray& paths)
{
    juce::StringArray preferred { ".csv", ".nc", ".json" };
    for (auto& ext : preferred)
    {
        juce::StringArray matches;
        for (auto& p : paths)
            if (p.endsWithIgnoreCase(ext))
                matches.add(p);
        if (! matches.isEmpty())
            return matches;
    }
    return paths;
}

struct AudioReference
{
    juce::String url;
    juce::String name;
    juce::Time start;
    juce::Time end;
    juce::String folder;
};

struct LocalAudio
{
    juce::File file;
    juce::String name;
    juce::Time start;
    juce::Time end;
    juce::String folder;
    double sampleRate = 0.0;
    juce::int64 lengthSamples = 0;
    int numChannels = 0;
};

bool timeLessThan(const juce::Time& a, const juce::Time& b)
{
    return a.toMilliseconds() < b.toMilliseconds();
}

bool timeLessThanOrEqual(const juce::Time& a, const juce::Time& b)
{
    return a.toMilliseconds() <= b.toMilliseconds();
}

void removeDuplicateTimesInPlace(juce::Array<juce::Time>& values)
{
    juce::Array<juce::Time> unique;
    unique.ensureStorageAllocated(values.size());

    for (auto& value : values)
    {
        bool seen = false;

        for (auto& existing : unique)
        {
            if (existing == value)
            {
                seen = true;
                break;
            }
        }

        if (! seen)
            unique.add(value);
    }

    values.swapWith(unique);
}

struct CsvContent
{
    juce::StringArray header;
    std::vector<juce::StringArray> rows;
};

static CsvContent readCsvLoose(const juce::File& file)
{
    CsvContent content;
    juce::FileInputStream stream(file);
    if (! stream.openedOk())
        throw std::runtime_error("Failed to open CSV: " + file.getFullPathName().toStdString());

    juce::MemoryBlock data;
    stream.readIntoMemoryBlock(data);
    auto text = juce::String::fromUTF8(static_cast<const char*>(data.getData()), (int) data.getSize());

    juce::StringArray lines;
    lines.addLines(text);

    bool headerSet = false;
    for (auto line : lines)
    {
        auto trimmed = line.trim();
        if (trimmed.isEmpty())
            continue;

        auto fields = splitCsvLine(line);
        if (! headerSet)
        {
            content.header = fields;
            headerSet = true;
        }
        else
        {
            content.rows.push_back(fields);
        }
    }

    return content;
}

static int maxColumnCount(const juce::StringArray& header,
                          const std::vector<juce::StringArray>& rows)
{
    int maxCols = header.size();
    for (auto& row : rows)
        maxCols = std::max(maxCols, row.size());
    return maxCols;
}

static int countDatetimeHits(int column, const std::vector<juce::StringArray>& rows)
{
    int hits = 0;
    for (auto& row : rows)
    {
        if (column >= row.size())
            continue;
        juce::Time dummy;
        if (parseTimeUTCImpl(row[column], dummy))
            ++hits;
    }
    return hits;
}

static int detectDatetimeColumn(const juce::StringArray& header,
                                const std::vector<juce::StringArray>& rows,
                                int startColumn = 0)
{
    const int maxCols = maxColumnCount(header, rows);
    if (rows.empty())
        return -1;

    const int rowCount = static_cast<int>(rows.size());
    const int minHits = std::max(1, std::min(rowCount, std::max(3, (int) std::ceil(rowCount * 0.05))));

    for (int col = startColumn; col < maxCols; ++col)
    {
        if (countDatetimeHits(col, rows) >= minHits)
            return col;
    }

    return -1;
}

static bool headerLooksLikeEnd(const juce::String& name)
{
    auto lower = name.toLowerCase();
    return lower.contains("end") || lower.contains("stop") || lower.contains("finish");
}

static bool headerSuggestsPresence(const juce::String& name)
{
    auto lower = name.toLowerCase();
    return lower.contains("present") || lower.contains("presence") || lower.contains("detect")
           || lower.contains("call") || lower.contains("heard") || lower.contains("flag");
}

static bool parsePresenceFlag(const juce::String& text, int& out)
{
    auto trimmed = text.trim();
    if (trimmed.isEmpty())
        return false;

    if (! isNumeric(trimmed))
        return false;

    double value = trimmed.getDoubleValue();
    int rounded = (int) std::llround(value);
    if (rounded != 0 && rounded != 1)
        return false;

    out = rounded;
    return true;
}

static int detectPresenceColumn(const juce::StringArray& header,
                                const std::vector<juce::StringArray>& rows,
                                int skipCol)
{
    const int maxCols = maxColumnCount(header, rows);
    if (rows.empty())
        return -1;

    struct Candidate
    {
        int column = -1;
        int ones = 0;
        int invalid = 0;
        int valid = 0;
        bool headerHint = false;
    };

    Candidate best;

    for (int col = 0; col < maxCols; ++col)
    {
        if (col == skipCol)
            continue;

        int ones = 0;
        int valid = 0;
        int invalid = 0;

        for (auto& row : rows)
        {
            if (col >= row.size())
                continue;

            int flag = 0;
            if (! parsePresenceFlag(row[col], flag))
            {
                if (row[col].trim().isNotEmpty())
                    ++invalid;
                continue;
            }

            if (flag == 1)
                ++ones;
            ++valid;
        }

        if (valid == 0 || ones == 0)
            continue;

        if (invalid > std::max(2, valid / 2))
            continue;

        Candidate candidate;
        candidate.column = col;
        candidate.ones = ones;
        candidate.invalid = invalid;
        candidate.valid = valid;
        candidate.headerHint = (col < header.size()) && headerSuggestsPresence(header[col]);

        auto better = [&](const Candidate& lhs, const Candidate& rhs)
        {
            if (lhs.column < 0)
                return true;
            if (rhs.column < 0)
                return false;
            if (lhs.headerHint != rhs.headerHint)
                return rhs.headerHint && ! lhs.headerHint;
            if (lhs.ones != rhs.ones)
                return rhs.ones > lhs.ones;
            if (lhs.invalid != rhs.invalid)
                return rhs.invalid < lhs.invalid;
            return rhs.valid > lhs.valid;
        };

        if (better(best, candidate))
            best = candidate;
    }

    return best.column;
}

static juce::Time normaliseToHour(const juce::Time& t)
{
    const juce::int64 hourMs = 3600 * 1000;
    auto ms = t.toMilliseconds();
    auto snapped = (ms / hourMs) * hourMs;
    return juce::Time(snapped);
}

static juce::Time normaliseToDay(const juce::Time& t)
{
    const juce::int64 dayMs = 24 * 3600 * 1000;
    auto ms = t.toMilliseconds();
    auto snapped = (ms / dayMs) * dayMs;
    return juce::Time(snapped);
}

juce::String sanctsound::SanctSoundClient::folderFromSetName(const juce::String& setName)
{
    static const std::regex re(R"(sanctsound_[a-z]{2}\d{2}_\d{2})", std::regex::icase);
    const std::string s = setName.toStdString();
    std::smatch m;
    if (std::regex_search(s, m, re))
        return juce::String(m.str()).toLowerCase();
    return {};
}

static juce::StringArray runAndCollect(const juce::StringArray& cmd, int& exitCode)
{
    exitCode = -1;
    juce::StringArray lines;

    juce::ChildProcess process;
    if (! process.start(cmd, juce::ChildProcess::wantStdOut | juce::ChildProcess::wantStdErr))
        return lines;

    auto output = process.readAllProcessOutput();
    exitCode = process.waitForProcessToFinish(-1);
    lines.addLines(output);
    return lines;
}

int SanctSoundClient::AudioHourSorter::compareElements(const AudioHour& a, const AudioHour& b) const
{
    auto diff = a.startUtc.toMilliseconds() - b.startUtc.toMilliseconds();
    if (diff < 0) return -1;
    if (diff > 0) return 1;
    return a.folder.compareIgnoreCase (b.folder);
}

bool SanctSoundClient::parseTimeUTC(const juce::String& text, juce::Time& out)
{
    return parseTimeUTCImpl(text, out);
}

juce::Array<PreviewWindow> groupConsecutive(const juce::Array<juce::Time>& points, const juce::RelativeTime& step)
{
    juce::Array<PreviewWindow> runs;
    if (points.isEmpty())
        return runs;

    auto start = points[0];
    auto prev = points[0];
    auto stepMs = step.inMilliseconds();

    for (int i = 1; i < points.size(); ++i)
    {
        auto p = points[i];
        auto diff = p.toMilliseconds() - prev.toMilliseconds();
        if (std::llabs(diff - (long long) stepMs) <= 1)
        {
            prev = p;
            continue;
        }

        runs.add({ start, prev + step });
        start = p;
        prev = p;
    }
    runs.add({ start, prev + step });
    return runs;
}

juce::Array<juce::Time> expandRuns(const juce::Array<PreviewWindow>& runs, const juce::RelativeTime& step)
{
    juce::Array<juce::Time> out;
    for (auto& r : runs)
    {
        auto t = r.start;
        while (timeLessThan(t, r.end))
        {
            out.add(t);
            t = t + step;
        }
    }
    return out;
}

bool isNumeric(const juce::String& text)
{
    auto t = text.trim();
    if (t.isEmpty())
        return false;
    for (auto ch : t)
        if (! (juce::CharacterFunctions::isDigit(ch) || ch == '.' || ch == '-' || ch == '+'))
            return false;
    return true;
}

std::vector<std::pair<int, int>> detectDatetimeColumns(const CsvTable& table,
                                                       double minFraction,
                                                       int minAbsolute)
{
    std::vector<std::pair<int, int>> matches;
    if (table.rows.isEmpty())
        return matches;

    const int rowCount = table.rows.size();
    const int required = juce::jmax(minAbsolute, (int) std::ceil(rowCount * minFraction));

    for (int col = 0; col < table.header.size(); ++col)
    {
        int count = 0;
        for (auto& row : table.rows)
        {
            if (col >= row.size())
                continue;
            juce::Time parsed;
            if (parseTimeUTCImpl(row[col], parsed))
                ++count;
        }

        if (count >= required)
            matches.emplace_back(col, count);
    }

    std::sort(matches.begin(), matches.end(), [](const auto& a, const auto& b)
    {
        if (a.second != b.second)
            return a.second > b.second;
        return a.first < b.first;
    });

    return matches;
}

int detectBinaryColumn(const CsvTable& table, int skipCol)
{
    int bestCol = -1;
    int bestCount = 0;
    for (int col = 0; col < table.header.size(); ++col)
    {
        if (col == skipCol)
            continue;
        int ones = 0;
        int total = 0;
        bool valid = false;
        for (auto& row : table.rows)
        {
            if (col >= row.size())
                continue;
            auto field = row[col].trim();
            if (! isNumeric(field))
                continue;
            auto value = field.getDoubleValue();
            int rounded = static_cast<int>(std::round(value));
            if (rounded == 0 || rounded == 1)
            {
                valid = true;
                ++total;
                if (rounded == 1)
                    ++ones;
            }
        }
        if (valid && ones > 0 && total > 0 && ones > bestCount)
        {
            bestCol = col;
            bestCount = ones;
        }
    }
    return bestCol;
}

std::vector<juce::Time> SanctSoundClient::parsePresenceHoursFromCsv(const juce::File& file)
{
    auto csv = readCsvLoose(file);
    if (csv.header.isEmpty())
        return {};

    const int timeCol = detectDatetimeColumn(csv.header, csv.rows);
    if (timeCol < 0)
        throw std::runtime_error("Could not detect hour column in " + file.getFileName().toStdString());

    const int presenceCol = detectPresenceColumn(csv.header, csv.rows, timeCol);
    if (presenceCol < 0)
        throw std::runtime_error("Could not detect presence column in " + file.getFileName().toStdString());

    std::set<juce::int64> seen;
    std::vector<juce::Time> hours;

    for (auto& row : csv.rows)
    {
        if (timeCol >= row.size() || presenceCol >= row.size())
            continue;

        juce::Time stamp;
        if (! parseTimeUTC(row[timeCol], stamp))
            continue;

        int flag = 0;
        if (! parsePresenceFlag(row[presenceCol], flag) || flag != 1)
            continue;

        auto hour = normaliseToHour(stamp);
        if (seen.insert(hour.toMilliseconds()).second)
            hours.push_back(hour);
    }

    std::sort(hours.begin(), hours.end(), [](const juce::Time& a, const juce::Time& b)
    {
        return timeLessThan(a, b);
    });

    return hours;
}

std::vector<juce::Time> SanctSoundClient::parsePresenceDaysFromCsv(const juce::File& file)
{
    auto csv = readCsvLoose(file);
    if (csv.header.isEmpty())
        return {};

    const int timeCol = detectDatetimeColumn(csv.header, csv.rows);
    if (timeCol < 0)
        throw std::runtime_error("Could not detect date column in " + file.getFileName().toStdString());

    const int presenceCol = detectPresenceColumn(csv.header, csv.rows, timeCol);
    if (presenceCol < 0)
        throw std::runtime_error("Could not detect presence column in " + file.getFileName().toStdString());

    std::set<juce::int64> seen;
    std::vector<juce::Time> days;

    for (auto& row : csv.rows)
    {
        if (timeCol >= row.size() || presenceCol >= row.size())
            continue;

        juce::Time stamp;
        if (! parseTimeUTC(row[timeCol], stamp))
            continue;

        int flag = 0;
        if (! parsePresenceFlag(row[presenceCol], flag) || flag != 1)
            continue;

        auto day = normaliseToDay(stamp);
        if (seen.insert(day.toMilliseconds()).second)
            days.push_back(day);
    }

    std::sort(days.begin(), days.end(), [](const juce::Time& a, const juce::Time& b)
    {
        return timeLessThan(a, b);
    });

    return days;
}

namespace
{

static bool parseIsoOrPlainUTC(const juce::String& value, juce::Time& out)
{
    auto s = value.trim();
    if (s.isEmpty())
        return false;

    if (s.containsChar('T'))
    {
        auto iso = juce::Time::fromISO8601(s);
        if (iso.toMilliseconds() != 0)
        {
            out = iso;
            return true;
        }
    }

    int Y = 0, M = 0, D = 0, h = 0, m = 0, sec = 0;
    if (std::sscanf(s.toRawUTF8(), "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &m, &sec) == 6
        || std::sscanf(s.toRawUTF8(), "%d/%d/%d %d:%d:%d", &Y, &M, &D, &h, &m, &sec) == 6)
    {
        out = makeUtc(Y, M, D, h, m, sec);
        return out.toMilliseconds() != 0;
    }

    if (std::sscanf(s.toRawUTF8(), "%d-%d-%d", &Y, &M, &D) == 3)
    {
        out = makeUtc(Y, M, D, 0, 0, 0);
        return out.toMilliseconds() != 0;
    }

    return false;
}

} // namespace

bool SanctSoundClient::parseEventsFromCsv(const juce::File& csvFile, juce::Array<EventWindow>& out)
{
    out.clearQuick();

    if (! csvFile.existsAsFile())
        return false;

    juce::FileInputStream in(csvFile);
    if (! in.openedOk())
        return false;

    auto header = in.readNextLine();
    if (header.isEmpty())
        return false;

    juce::StringArray columns;
    columns.addTokens(header, ",", "\"");
    for (int i = 0; i < columns.size(); ++i)
        columns.set(i, columns[i].trim());
    columns.removeEmptyStrings(true);

    auto findColumn = [&columns](std::initializer_list<const char*> keys) -> int
    {
        for (int idx = 0; idx < columns.size(); ++idx)
        {
            auto lower = columns[idx].toLowerCase();
            for (auto* key : keys)
            {
                if (lower.contains(juce::String(key).toLowerCase()))
                    return idx;
            }
        }
        return -1;
    };

    const int startIdx = findColumn({ "start", "timestamp", "time", "utc" });
    const int endIdx   = findColumn({ "end" });
    const int durIdx   = findColumn({ "duration", "dur", "length" });

    if (startIdx < 0)
        return false;

    std::vector<EventWindow> events;

    constexpr double kFallbackSeconds = 60.0;

    while (! in.isExhausted())
    {
        auto line = in.readNextLine();
        if (line.isEmpty())
            continue;

        juce::StringArray row;
        row.addTokens(line, ",", "\"");

        if (row.size() <= startIdx)
            continue;

        juce::Time startUTC;
        if (! parseIsoOrPlainUTC(row[startIdx], startUTC))
            continue;

        juce::Time endUTC;
        bool haveEnd = false;

        if (endIdx >= 0 && endIdx < row.size())
            haveEnd = parseIsoOrPlainUTC(row[endIdx], endUTC);

        if (! haveEnd)
        {
            double seconds = kFallbackSeconds;
            if (durIdx >= 0 && durIdx < row.size())
            {
                auto raw = row[durIdx].trim();
                if (raw.isNotEmpty())
                {
                    double candidate = raw.getDoubleValue();
                    if (candidate > 0.0)
                    {
                        seconds = candidate;
                    }
                    else if (raw.containsIgnoreCase("PT") && raw.endsWithIgnoreCase("S"))
                    {
                        auto inside = raw.fromFirstOccurrenceOf("PT", false, false)
                                          .upToLastOccurrenceOf("S", false, false);
                        candidate = inside.getDoubleValue();
                        if (candidate > 0.0)
                            seconds = candidate;
                    }
                    else if (raw.containsChar(':'))
                    {
                        int hh = 0, mm = 0, ss = 0;
                        if (std::sscanf(raw.toRawUTF8(), "%d:%d:%d", &hh, &mm, &ss) == 3)
                            seconds = hh * 3600.0 + mm * 60.0 + ss;
                    }
                }
            }

            endUTC = startUTC + juce::RelativeTime::seconds(seconds);
        }

        if (endUTC <= startUTC)
            endUTC = startUTC + juce::RelativeTime::seconds(kFallbackSeconds);

        events.push_back({ startUTC, endUTC });
    }

    if (events.empty())
        return false;

    std::sort(events.begin(), events.end(), [](const EventWindow& a, const EventWindow& b)
    {
        auto sa = a.startUTC.toMilliseconds();
        auto sb = b.startUTC.toMilliseconds();
        if (sa != sb)
            return sa < sb;
        return a.endUTC.toMilliseconds() < b.endUTC.toMilliseconds();
    });

    events.erase(std::unique(events.begin(), events.end(), [](const EventWindow& a, const EventWindow& b)
    {
        return a.startUTC.toMilliseconds() == b.startUTC.toMilliseconds()
            && a.endUTC.toMilliseconds() == b.endUTC.toMilliseconds();
    }), events.end());

    for (const auto& evt : events)
        out.add(evt);

    return ! out.isEmpty();
}

int SanctSoundClient::runAndStream(const juce::StringArray& args,
                                   const std::function<void(const juce::String&)>& log)
{
    if (args.isEmpty())
        return -1;

    juce::ChildProcess child;
    if (! child.start(args, juce::ChildProcess::wantStdOut | juce::ChildProcess::wantStdErr))
    {
        if (log)
            log("Failed to start: " + formatCommand(args));
        return -1;
    }

    auto output = child.readAllProcessOutput();
    auto exitCode = child.waitForProcessToFinish(-1);

    if (log && output.isNotEmpty())
    {
        juce::StringArray lines;
        lines.addLines(output);
        for (auto& line : lines)
            log(line);
    }

    return exitCode;
}

void SanctSoundClient::minimalFilesForWindows(const juce::Array<AudioHour>& files,
                                             const std::vector<std::pair<juce::Time, juce::Time>>& windows,
                                             juce::StringArray& outUrls,
                                             juce::StringArray& outNames,
                                             juce::Array<NeededFileRow>& rows,
                                             int& unmatchedCount,
                                             const juce::File& debugDir) const
{
    outUrls.clear();
    outNames.clear();
    rows.clear();
    unmatchedCount = 0;

    if (files.isEmpty() || windows.empty())
        return;

    std::vector<juce::int64> starts;
    starts.reserve((size_t) files.size());
    for (auto& file : files)
        starts.push_back(file.startUtc.toMilliseconds());

    auto pick = [&] (juce::Time start, juce::Time end) -> juce::Array<int>
    {
        juce::Array<int> result;
        auto millis = start.toMilliseconds();
        auto it = std::upper_bound(starts.begin(), starts.end(), millis);
        int index = static_cast<int>(std::distance(starts.begin(), it)) - 1;
        if (index < 0)
            return result;

        result.add(index);
        const auto& first = files.getReference(index);
        if (first.endUtc.toMilliseconds() >= end.toMilliseconds())
            return result;

        if (index + 1 < files.size())
            result.add(index + 1);

        return result;
    };

    for (auto& window : windows)
    {
        auto idxs = pick(window.first, window.second);
        const bool needTwo = idxs.size() >= 2;
        const AudioHour* first = idxs.size() >= 1 ? &files.getReference(idxs[0]) : nullptr;
        const AudioHour* second = needTwo ? &files.getReference(idxs[1]) : nullptr;

        juce::String windowLabel = toIsoUTC (window.first) + " .. " + toIsoUTC (window.second);
        logPreviewExplain("window " + windowLabel);

        if (debugDir.getFullPathName().isNotEmpty())
        {
            juce::File m = debugDir.getChildFile ("MATCH_log.txt");
            juce::FileOutputStream os (m, true);
            if (os.openedOk())
            {
                os << "----\n";
                os << "window " << toIsoUTC (window.first) << " .. " << toIsoUTC (window.second) << "\n";
                os << "picked: "
                   << (first != nullptr ? first->fname : juce::String("(none)"));
                if (needTwo)
                    os << " + " << (second != nullptr ? second->fname : juce::String("(none)"));
                os << "\n";
            }
        }

        if (idxs.isEmpty())
        {
            logPreviewExplain("  -> no audio match");
            rows.add({ window.first, window.second, {}, {} });
            ++unmatchedCount;
            continue;
        }

        if (first != nullptr)
        {
            logPreviewExplain("  keep " + first->fname
                               + " [" + toIsoUTC(first->startUtc) + " .. " + toIsoUTC(first->endUtc) + "]");
        }
        if (needTwo)
        {
            if (second != nullptr)
                logPreviewExplain("  keep " + second->fname
                                   + " [" + toIsoUTC(second->startUtc) + " .. " + toIsoUTC(second->endUtc) + "]");
            else
                logPreviewExplain("  second file missing for coverage");
        }

        juce::StringArray namesThis;
        juce::StringArray urlsThis;

        for (auto idx : idxs)
        {
            if (idx < 0 || idx >= files.size())
                continue;

            const auto& file = files.getReference(idx);
            namesThis.add(file.fname);
            urlsThis.add(file.url);
            outUrls.add(file.url);
            outNames.add(file.fname);
        }

        rows.add({ window.first, window.second,
                   namesThis.joinIntoString(";"),
                   urlsThis.joinIntoString(";") });
    }

    outUrls.removeDuplicates(true);
    outNames.removeDuplicates(true);
    outUrls.sort(true);
    outNames.sort(true);
}

juce::File SanctSoundClient::makePreviewDebugDir (const juce::File& destDir, const juce::String& setName) const
{
    auto dbgRoot = destDir.getChildFile("__audio_debug");
    dbgRoot.createDirectory();

    auto dbg = dbgRoot.getChildFile("preview_" + setName);
    dbg.createDirectory();

    dbgRoot.getChildFile("LAST_DEBUG_DIR.txt").replaceWithText(dbg.getFullPathName());
    return dbg;
}

juce::StringArray SanctSoundClient::rowsToUrls (const juce::Array<AudioHour>& rows) const
{
    juce::StringArray a;
    for (auto const& r : rows)
        a.add(r.url);
    return a;
}

void SanctSoundClient::dumpLines (const juce::File& outFile, const juce::StringArray& lines)
{
    juce::String joined;
    for (auto& s : lines)
        joined << s << "\n";
    outFile.replaceWithText(joined);
}

juce::StringArray SanctSoundClient::listDeploymentsForSite(const juce::String& site,
                                                           const std::function<void(const juce::String&)>& log) const
{
    juce::StringArray folders;

    auto siteCode = site.trim().toLowerCase();
    if (siteCode.isEmpty())
        return folders;

    juce::String prefix = audioPrefix;
    if (! prefix.endsWithChar('/'))
        prefix << "/";

    const auto base = "gs://" + gcsBucket + "/" + prefix + siteCode + "/";
    int exitCode = 0;
    auto lines = runAndCollect({ "gsutil", "ls", base }, exitCode);

    for (auto& line : lines)
    {
        auto trimmed = line.trim();
        if (! trimmed.endsWithChar('/'))
            continue;

        juce::URL url(trimmed);
        auto name = url.getFileName();
        name = name.upToLastOccurrenceOf("/", false, false);
        if (name.startsWithIgnoreCase("sanctsound_"))
            folders.addIfNotAlreadyThere(name.toLowerCase());
    }

    folders.sort(true);

    if (log)
    {
        log("Deployments ls exit=" + juce::String(exitCode) + " for " + base + "\n");
        if (exitCode != 0)
            log("[WARN] gsutil ls failed: " + juce::String(exitCode) + "\n");
        if (! lines.isEmpty())
        {
            log("Deployments raw (" + juce::String(lines.size()) + "):\n");
            auto limit = juce::jmin(10, lines.size());
            for (int i = 0; i < limit; ++i)
                log("  " + lines[i] + "\n");
            if (lines.size() > limit)
                log("  ... (" + juce::String(lines.size() - limit) + " more)\n");
        }
        log("Deployments: " + folders.joinIntoString(", ") + "\n");
    }

    return folders;
}

juce::Array<SanctSoundClient::AudioHour> SanctSoundClient::listAudioInFolder(const juce::String& site,
                                                                             const juce::String& folder,
                                                                             std::optional<juce::Time> tmin,
                                                                             std::optional<juce::Time> tmax,
                                                                             const std::function<void(const juce::String&)>& log,
                                                                             const juce::File& debugDir) const
{
    juce::Array<AudioHour> out;

    const auto siteCode = site.trim().toLowerCase();
    const auto folderName = folder.trim().toLowerCase();
    if (siteCode.isEmpty() || folderName.isEmpty())
        return out;

    logPreviewExplain("list-audio folder=" + siteCode + "/" + folderName);

    juce::String prefix = audioPrefix;
    if (! prefix.endsWithChar('/'))
        prefix << "/";

    juce::StringArray lines;
    juce::String pattern;
    int exitCode = 0;

    auto logOffline = [&]()
    {
        if (offlineEnabled)
            logPreviewExplain("offline listing used for " + folderName + " entries=" + juce::String(lines.size()));
    };

    if (offlineEnabled)
    {
        auto offlineFile = offlineDataRoot.getChildFile("audio_index")
                                          .getChildFile(siteCode + "__" + folderName + ".json");
        auto parsed = readJsonFile(offlineFile);

        if (parsed.isVoid() || parsed.isUndefined())
        {
            if (log)
                log("[WARN] Offline audio index missing: " + offlineFile.getFullPathName() + "\n");
        }
        else
        {
            auto appendObject = [&](const juce::String& objectName)
            {
                auto trimmed = objectName.trim();
                if (trimmed.isEmpty())
                    return;

                juce::String full = trimmed;
                if (! full.startsWithIgnoreCase("gs://"))
                    full = makeGsUrl(gcsBucket, full);

                lines.add(full);
            };

            if (auto* arr = parsed.getArray())
            {
                for (auto& value : *arr)
                {
                    if (value.isString())
                        appendObject(value.toString());
                }
            }
            else if (auto* obj = parsed.getDynamicObject())
            {
                for (auto& prop : obj->getProperties())
                {
                    if (prop.value.isString())
                        appendObject(prop.value.toString());
                }
            }
            else if (parsed.isString())
            {
                appendObject(parsed.toString());
            }

            if (log)
                log("[offline] audio index " + offlineFile.getFullPathName()
                    + " entries=" + juce::String(lines.size()) + "\n");
        }

        logOffline();
    }
    else
    {
        pattern = "gs://" + gcsBucket + "/" + prefix + siteCode + "/" + folderName + "/audio/*.flac";
        lines = runAndCollect({ "gsutil", "ls", "-r", pattern }, exitCode);
    }

    writeListingDebug(folderName, pattern, lines, exitCode);

    std::optional<AudioHour> left;
    int parsed = 0;

    for (auto& raw : lines)
    {
        auto trimmed = raw.trim();
        if (! trimmed.startsWith("gs://") || ! trimmed.endsWithIgnoreCase(".flac"))
            continue;

        ++parsed;

        const juce::String url = trimmed;
        auto fname = juce::URL(trimmed).getFileName();

        juce::Time startUtc;
        if (! parseAudioStartFromName(fname, startUtc))
        {
            logPreviewExplain("skip parse failure: " + fname);
            if (debugDir.getFullPathName().isNotEmpty())
            {
                juce::File pf = debugDir.getChildFile("parse_failures.txt");
                juce::FileOutputStream os(pf, true);
                if (os.openedOk())
                    os << folderName << "/" << fname << "\n";
            }
            continue;
        }

        if (tmin.has_value() && timeLessThan(startUtc, *tmin))
        {
            juce::String note = "drop before window: " + fname
                                 + " start=" + toIsoUTC(startUtc)
                                 + " < tmin=" + toIsoUTC(*tmin);
            logPreviewExplain(note);
            if (! left.has_value() || timeLessThan(left->startUtc, startUtc))
                left = AudioHour { url, fname, folderName, startUtc, startUtc + juce::RelativeTime::hours(1) };
            continue;
        }

        if (tmax.has_value() && timeLessThan(*tmax, startUtc))
        {
            logPreviewExplain("drop after window: " + fname
                               + " start=" + toIsoUTC(startUtc)
                               + " > tmax=" + toIsoUTC(*tmax));
            continue;
        }

        out.add({ url, fname, folderName, startUtc, startUtc + juce::RelativeTime::hours(1) });
        logPreviewExplain("candidate in-range: " + fname
                           + " start=" + toIsoUTC(startUtc));
    }

    if (left.has_value())
    {
        bool include = true;
        if (tmin.has_value())
        {
            auto diff = tmin->toMilliseconds() - left->startUtc.toMilliseconds();
            include = (diff >= 0 && diff <= (juce::int64) (6 * 3600 * 1000));
        }
        if (include)
        {
            out.add(*left);
            if (log)
                log("include left-boundary: " + left->fname + "\n");

            juce::String diffText;
            if (tmin.has_value())
            {
                auto diffMs = tmin->toMilliseconds() - left->startUtc.toMilliseconds();
                diffText = " diff_secs=" + juce::String(diffMs / 1000.0, 3);
            }
            logPreviewExplain("left-boundary add: " + left->fname
                               + " start=" + toIsoUTC(left->startUtc) + diffText);
        }
        else if (tmin.has_value())
        {
            auto diffMs = tmin->toMilliseconds() - left->startUtc.toMilliseconds();
            logPreviewExplain("left-boundary skipped: " + left->fname
                               + " diff_secs=" + juce::String(diffMs / 1000.0, 3));
        }
    }

    AudioHourSorter sorter;
    out.sort(sorter, false);

    for (int i = 0; i < out.size(); ++i)
    {
        auto& current = out.getReference(i);
        juce::Time endUtc = current.startUtc + juce::RelativeTime::hours(1);
        if (i + 1 < out.size() && out[i + 1].folder.equalsIgnoreCase(current.folder))
        {
            endUtc = out[i + 1].startUtc;
            if (! timeLessThan(current.startUtc, endUtc))
                endUtc = current.startUtc + juce::RelativeTime::seconds(1);
        }
        current.endUtc = endUtc;
    }

    if (log)
    {
        log("Folder " + folderName + ": exit=" + juce::String(exitCode)
            + " parsed=" + juce::String(parsed)
            + " kept=" + juce::String(out.size()) + "\n");
        if (exitCode != 0 && pattern.isNotEmpty())
            log("[WARN] gsutil ls exit " + juce::String(exitCode) + " for " + pattern + "\n");

        if (! lines.isEmpty())
        {
            log("  gsutil output lines=" + juce::String(lines.size()) + "\n");
            auto limit = juce::jmin(6, lines.size());
            for (int i = 0; i < limit; ++i)
                log("    " + lines[i] + "\n");
            if (lines.size() > limit)
                log("    ... (" + juce::String(lines.size() - limit) + " more)\n");
        }

        if (! out.isEmpty())
        {
            auto describe = [] (const AudioHour& hour)
            {
                return hour.fname + "@" + hour.startUtc.toISO8601(true);
            };

            juce::StringArray firstSample;
            const auto firstCount = juce::jmin(3, out.size());
            for (int i = 0; i < firstCount; ++i)
                firstSample.add(describe(out[i]));
            log("  first: " + firstSample.joinIntoString(", ") + "\n");

            if (out.size() > 3)
            {
                juce::StringArray lastSample;
                const int startIndex = juce::jmax(0, out.size() - 3);
                for (int i = startIndex; i < out.size(); ++i)
                    lastSample.add(describe(out[i]));
                log("  last: " + lastSample.joinIntoString(", ") + "\n");
            }
        }
    }

    return out;
}

juce::Array<SanctSoundClient::AudioHour> SanctSoundClient::listAudioAcross(const juce::String& site,
                                                                          const juce::String& preferFolder,
                                                                          std::optional<juce::Time> tmin,
                                                                          std::optional<juce::Time> tmax,
                                                                          const std::function<void(const juce::String&)>& log,
                                                                          const juce::File& debugDir) const
{
    juce::Array<AudioHour> all;

    auto folders = listDeploymentsForSite(site, log);
    juce::StringArray ordered;

    auto preferLower = preferFolder.trim().toLowerCase();
    if (preferLower.isNotEmpty() && folders.contains(preferLower))
        ordered.add(preferLower);

    for (auto& f : folders)
    {
        if (preferLower.isNotEmpty() && f.equalsIgnoreCase(preferLower))
            continue;
        ordered.addIfNotAlreadyThere(f);
    }

    if (ordered.isEmpty())
        ordered = folders;

    if (ordered.isEmpty() && log)
        log("[WARN] No deployments found for site " + site + "\n");
    if (! ordered.isEmpty())
        logPreviewExplain("folder order: " + ordered.joinIntoString(", "));

    for (auto& folderName : ordered)
    {
        auto files = listAudioInFolder(site, folderName, tmin, tmax, log, debugDir);
        all.addArray(files);
    }

    AudioHourSorter sorter;
    all.sort(sorter, false);

    if (log)
    {
        log("files across (total): " + juce::String(all.size()) + "\n");
        if (! ordered.isEmpty())
            log("folders ordered: " + ordered.joinIntoString(", ") + "\n");
    }

    return all;
}

juce::var findFirst(const juce::var& obj, const std::vector<juce::String>& keys)
{
    if (obj.isObject())
    {
        if (auto* dict = obj.getDynamicObject())
        {
            for (auto& k : dict->getProperties())
            {
                auto keyLower = k.name.toString().toLowerCase();
                for (auto& target : keys)
                    if (keyLower == target.toLowerCase())
                        return k.value;
            }
            for (auto& k : dict->getProperties())
            {
                auto child = findFirst(k.value, keys);
                if (! child.isVoid())
                    return child;
            }
        }
    }
    else if (obj.isArray())
    {
        if (auto* arr = obj.getArray())
        {
            for (auto& item : *arr)
            {
                auto child = findFirst(item, keys);
                if (! child.isVoid())
                    return child;
            }
        }
    }
    return {};
}

MetadataSummary buildSummaryFromJson(const juce::var& meta)
{
    MetadataSummary summary;
    auto pick = [&](std::initializer_list<juce::String> names)
    {
        std::vector<juce::String> keys(names);
        auto value = findFirst(meta, keys);
        if (value.isString())
            return value.toString();
        if (value.isInt())
            return juce::String(static_cast<int>(value));
        if (value.isDouble())
            return juce::String(static_cast<double>(value));
        return juce::String();
    };

    summary.site = pick({ "site_name", "site" });
    summary.deployment = pick({ "deployment_name", "deployment" });
    summary.platform = pick({ "platform_name", "platform" });
    summary.recorder = pick({ "recorder", "model", "instrument_model" });
    auto lat = pick({ "latitude", "lat" });
    auto lon = pick({ "longitude", "lon" });
    auto depth = pick({ "depth", "water_depth", "sensor_depth" });
    juce::StringArray coordParts;
    if (lat.isNotEmpty()) coordParts.add(lat);
    if (lon.isNotEmpty()) coordParts.add(lon);
    if (depth.isNotEmpty()) coordParts.add(depth + " m");
    summary.coordinates = coordParts.joinIntoString(", ");
    summary.start = pick({ "start_time", "start" });
    summary.end = pick({ "end_time", "end" });
    summary.sampleRate = pick({ "sample_rate", "sample_rate_hz", "sampling_rate" });
    summary.note = pick({ "location_note", "comments" });
    return summary;
}

juce::Array<AudioReference> buildAudioReferencesFromObjects(const std::vector<juce::String>& objects,
                                                            const juce::String& bucket,
                                                            const juce::Time& tmin,
                                                            const juce::Time& tmax,
                                                            const juce::String& fallbackFolder)
{
    juce::Array<AudioReference> files;
    juce::Optional<AudioReference> leftCandidate;

    for (auto& objectName : objects)
    {
        auto url = makeGsUrl(bucket, objectName);
        auto name = objectName.fromLastOccurrenceOf("/", false, false);

        juce::Time start;
        if (! SanctSoundClient::parseAudioStartFromName(name, start))
            continue;

        juce::String folder = SanctSoundClient::folderFromSetName(name);
        if (folder.isEmpty())
            folder = fallbackFolder;

        if (tmin.toMilliseconds() != 0 && timeLessThan(start, tmin))
        {
            AudioReference candidate { url, name, start, {}, folder };
            if (! leftCandidate.hasValue() || timeLessThan(leftCandidate->start, start))
                leftCandidate = candidate;
            continue;
        }

        if (tmax.toMilliseconds() != 0 && timeLessThan(tmax, start))
            continue;

        files.add({ url, name, start, {}, folder });
    }

    if (leftCandidate.hasValue())
    {
        if (tmin.toMilliseconds() == 0 || (tmin - leftCandidate->start) <= juce::RelativeTime::hours(6))
            files.add(*leftCandidate);
    }

    std::sort(files.begin(), files.end(), [](const AudioReference& a, const AudioReference& b)
    {
        if (timeLessThan(a.start, b.start)) return true;
        if (timeLessThan(b.start, a.start)) return false;
        return a.folder < b.folder;
    });

    for (int i = 0; i < files.size(); ++i)
    {
        if (i + 1 < files.size() && files[i + 1].folder == files[i].folder)
        {
            auto end = files[i + 1].start;
            if (! timeLessThan(files[i].start, end))
                end = files[i].start + juce::RelativeTime::seconds(1);
            files.getReference(i).end = end;
        }
        else
        {
            files.getReference(i).end = files[i].start + juce::RelativeTime::hours(1);
        }
    }

    return files;
}

struct DownloadedFile
{
    juce::File localFile;
    juce::String url;
};

static juce::File offlineBucketFile(const juce::File& offlineRoot,
                                    const juce::String& bucket,
                                    const juce::String& objectName)
{
    if (! offlineRoot.isDirectory())
        return {};

    auto gcsRoot = offlineRoot.getChildFile("gcs");
    return gcsRoot.getChildFile(bucket).getChildFile(objectName);
}

std::vector<DownloadedFile> downloadFilesTo(const juce::StringArray& urls,
                                            const juce::File& dest,
                                            const juce::File& offlineRoot,
                                            SanctSoundClient::LogFn log)
{
    std::vector<DownloadedFile> out;
    if (dest.exists())
    {
        if (! dest.isDirectory())
            throw std::runtime_error("Destination exists but is not a directory: "
                                     + dest.getFullPathName().toStdString());
    }
    else if (! dest.createDirectory())
    {
        throw std::runtime_error("Failed to create directory: " + dest.getFullPathName().toStdString());
    }

    for (auto& url : urls)
    {
        juce::String bucket;
        juce::String objectName;
        juce::String httpUrl;

        juce::String base;
        if (objectName.isNotEmpty())
            base = objectName.fromLastOccurrenceOf("/", false, false);
        if (base.isEmpty())
            base = url.fromLastOccurrenceOf("/", false, false);

        auto local = dest.getChildFile(base);

        bool handledOffline = false;
        if (offlineRoot.isDirectory() && parseGsUrl(url, bucket, objectName))
        {
            auto offlineFile = offlineBucketFile(offlineRoot, bucket, objectName);
            if (offlineFile.existsAsFile())
            {
                if (log)
                    log("[offline] copy " + offlineFile.getFullPathName());
                ensureParentDir(local);
                if (local.existsAsFile())
                    local.deleteFile();
                if (! offlineFile.copyFileTo(local))
                    throw std::runtime_error("Failed to copy offline file: " + offlineFile.getFullPathName().toStdString());
                handledOffline = true;
            }
        }

        if (! handledOffline)
        {
            if (parseGsUrl(url, bucket, objectName))
                httpUrl = makeHttpsUrl(bucket, objectName);
            else
                httpUrl = url;

            if (log)
                log("[http] GET " + httpUrl);

            int statusCode = 0;
            juce::StringPairArray headers;
            juce::URL u(httpUrl);
            auto stream = u.createInputStream(juce::URL::InputStreamOptions(juce::URL::ParameterHandling::inAddress)
                                                  .withConnectionTimeoutMs(15000)
                                                  .withResponseHeaders(&headers)
                                                  .withStatusCode(&statusCode));
            if (stream == nullptr || statusCode >= 400)
            {
                throw std::runtime_error("GCS download failed: HTTP " + std::to_string(statusCode));
            }

            ensureParentDir(local);
            juce::FileOutputStream outStream(local);
            if (! outStream.openedOk())
                throw std::runtime_error("Failed to open file for writing: " + local.getFullPathName().toStdString());

            if (outStream.writeFromInputStream(*stream, -1) < 0)
                throw std::runtime_error("Failed to write file: " + local.getFullPathName().toStdString());

            outStream.flush();
            if (outStream.getStatus().failed())
                throw std::runtime_error("Write failed: " + outStream.getStatus().getErrorMessage().toStdString());
        }

        out.push_back({ local, url });
    }
    return out;
}

void SanctSoundClient::downloadFiles(const juce::StringArray& urls,
                                     const std::function<void(const juce::String&)>& log) const
{
    if (urls.isEmpty())
        return;

    auto dest = destinationDir;
    if (dest.exists())
    {
        if (! dest.isDirectory())
            throw std::runtime_error("Destination exists but is not a directory: "
                                     + dest.getFullPathName().toStdString());
    }
    else if (! dest.createDirectory())
    {
        throw std::runtime_error("Failed to create directory: " + dest.getFullPathName().toStdString());
    }

    juce::String destPath = dest.getFullPathName();
    if (! destPath.endsWithChar('/'))
        destPath << "/";

    for (auto& url : urls)
    {
        juce::StringArray args { "gsutil", "cp", url, destPath };
        if (log)
            log("$ " + formatCommand(args));

        auto exitCode = runAndStream(args, log);
        if (exitCode != 0)
        {
            throw std::runtime_error("gsutil cp failed for " + url.toStdString()
                                     + " (exit code " + std::to_string(exitCode) + ")");
        }

        if (log)
            log("[ok] " + url);
    }
}

juce::var parseJson(const juce::String& text)
{
    return juce::JSON::parse(text);
}

juce::String ffprobeDuration(const juce::File& file, double& outSeconds)
{
    auto result = runCommand({ "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file.getFullPathName() });
    if (result.exitCode != 0)
        return humaniseError("ffprobe", result);
    outSeconds = result.output.trim().getDoubleValue();
    return {};
}

juce::String ffmpegCut(const juce::File& source,
                       double startSeconds,
                       double durationSeconds,
                       const juce::File& outFile,
                       int sampleRate,
                       bool mono,
                       const juce::String& sampleFmt)
{
    juce::StringArray args { "ffmpeg", "-y", "-loglevel", "error", "-ss", juce::String(startSeconds, 3), "-t", juce::String(durationSeconds, 3), "-i", source.getFullPathName() };
    if (mono)
        args.addArray({ "-ac", "1" });
    args.addArray({ "-ar", juce::String(sampleRate), "-sample_fmt", sampleFmt, outFile.getFullPathName() });
    auto result = runCommand(args);
    if (result.exitCode != 0)
        return humaniseError("ffmpeg", result);
    return {};
}

juce::String ffmpegConcat(const juce::File& wav1, const juce::File& wav2, const juce::File& outFile)
{
    auto temp = juce::File::createTempFile("concat_list.txt");
    try
    {
        juce::String text;
        text << "file '" << wav1.getFullPathName() << "'\n";
        text << "file '" << wav2.getFullPathName() << "'\n";
        writeTextFile(temp, text);
    }
    catch (const std::runtime_error& e)
    {
        temp.deleteFile();
        return e.what();
    }
    auto result = runCommand({ "ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", temp.getFullPathName(), "-c", "copy", outFile.getFullPathName() });
    temp.deleteFile();
    if (result.exitCode != 0)
        return humaniseError("ffmpeg concat", result);
    return {};
}


SanctSoundClient::SanctSoundClient()
{
    auto defaultDir = juce::File::getSpecialLocation(juce::File::userDocumentsDirectory)
                          .getChildFile("SanctSound");
    if (! setDestinationDirectory(defaultDir))
        throw std::runtime_error("Failed to initialise destination directory: "
                                 + defaultDir.getFullPathName().toStdString());

    gcsBucket = "noaa-passive-bioacoustic";
    audioPrefix = "sanctsound/audio/";
    productsPrefix = "sanctsound/products/detections";

    auto offlineEnv = juce::SystemStats::getEnvironmentVariable("SANCTSOUND_OFFLINE_ROOT", {});
    if (offlineEnv.isNotEmpty())
        setOfflineDataRoot(juce::File(offlineEnv));
}

void SanctSoundClient::setPreviewDebugDirectory(const juce::File& directory)
{
    previewDebugOverrideDir = directory;
    if (previewDebugOverrideDir.getFullPathName().isNotEmpty())
        previewDebugOverrideDir.createDirectory();
}

juce::File SanctSoundClient::getPreviewDebugDirectory() const
{
    return previewDebugOverrideDir;
}

void SanctSoundClient::setPreviewDebugSinks (const PreviewDebugSinks& sinks) const
{
    previewDebugSinks = sinks;

    auto resetFile = [] (const juce::File& file)
    {
        if (file.getFullPathName().isEmpty())
            return;
        ensureParentDir(file);
        if (file.existsAsFile())
            file.deleteFile();
    };

    resetFile(sinks.folderListings);
    resetFile(sinks.folderListingCommands);
    resetFile(sinks.candidateUrls);
    resetFile(sinks.candidateNames);
    resetFile(sinks.selectedUrls);
    resetFile(sinks.selectedNames);
    resetFile(sinks.explainLog);
    resetFile(sinks.windowsTsv);
}

void SanctSoundClient::clearPreviewDebugSinks() const
{
    previewDebugSinks.reset();
}

const SanctSoundClient::PreviewDebugSinks* SanctSoundClient::getPreviewDebugSinks() const
{
    return previewDebugSinks ? &previewDebugSinks.value() : nullptr;
}

void SanctSoundClient::logPreviewExplain (const juce::String& message) const
{
    if (message.isEmpty())
        return;

    if (const auto* sinks = getPreviewDebugSinks())
        appendLine(sinks->explainLog, message);
}

void SanctSoundClient::writeListingDebug (const juce::String& folder,
                                          const juce::String& pattern,
                                          const juce::StringArray& lines,
                                          int exitCode) const
{
    const auto* sinks = getPreviewDebugSinks();
    if (sinks == nullptr)
        return;

    juce::String header = "## folder=" + folder;
    if (pattern.isNotEmpty())
        header += " pattern=" + pattern;
    header += " exit=" + juce::String(exitCode);
    appendLine(sinks->folderListings, header);
    for (auto& line : lines)
        appendLine(sinks->folderListings, line);
    appendLine(sinks->folderListings, juce::String());

    if (sinks->folderListingCommands.getFullPathName().isNotEmpty())
    {
        juce::StringArray cmd { "gsutil", "ls", "-r", pattern }; // matches call site
        if (pattern.isEmpty())
            cmd.removeRange(3, 1);
        appendLine(sinks->folderListingCommands,
                   formatCommand(cmd) + " | exit=" + juce::String(exitCode));
    }
}

void SanctSoundClient::writeCandidateDebug (const juce::Array<AudioHour>& rows) const
{
    const auto* sinks = getPreviewDebugSinks();
    if (sinks == nullptr)
        return;

    juce::StringArray urls;
    juce::StringArray names;
    for (auto const& row : rows)
    {
        urls.add(row.url);
        names.add(row.fname);
    }
    overwriteLines(sinks->candidateUrls, urls);
    overwriteLines(sinks->candidateNames, names);
}

void SanctSoundClient::writeSelectedDebug (const juce::StringArray& urls) const
{
    const auto* sinks = getPreviewDebugSinks();
    if (sinks == nullptr)
        return;

    juce::StringArray names;
    for (auto const& url : urls)
        names.add(juce::URL(url).getFileName());

    overwriteLines(sinks->selectedUrls, urls);
    overwriteLines(sinks->selectedNames, names);
}

void SanctSoundClient::writeWindowsDebug (const std::vector<std::pair<juce::Time, juce::Time>>& windows) const
{
    const auto* sinks = getPreviewDebugSinks();
    if (sinks == nullptr)
        return;

    juce::StringArray lines;
    for (const auto& window : windows)
        lines.add(toIsoUTC(window.first) + "\t" + toIsoUTC(window.second));
    overwriteLines(sinks->windowsTsv, lines);
}

juce::Array<SanctSoundClient::AudioHour> SanctSoundClient::listAudioForFolder(const juce::String& site,
                                                                             const juce::String& folder,
                                                                             std::optional<juce::Time> tmin,
                                                                             std::optional<juce::Time> tmax,
                                                                             const std::function<void(const juce::String&)>& log,
                                                                             const juce::File& debugDir) const
{
    return listAudioInFolder(site, folder, tmin, tmax, log, debugDir);
}

void SanctSoundClient::setOfflineDataRoot(const juce::File& directory)
{
    offlineDataRoot = directory;
    offlineEnabled = directory.exists() && directory.isDirectory();
}

const juce::File& SanctSoundClient::getOfflineDataRoot() const
{
    return offlineDataRoot;
}

SanctSoundClient::AudioListingResult SanctSoundClient::listAudioObjectsForGroup(const juce::String& site,
                                                                               const juce::String& groupName,
                                                                               LogFn log) const
{
    AudioListingResult listing;

    auto audioRoot = audioPrefix;
    if (! audioRoot.endsWithChar('/'))
        audioRoot += "/";

    const juce::String bucket = gcsBucket;
    const juce::String siteCode = site.trim();
    const juce::String group = groupName.trim();
    listing.prefix = audioRoot + siteCode + "/" + group + "/";

    juce::StringArray objects;
    if (offlineEnabled)
    {
        auto offlineFile = offlineDataRoot.getChildFile("audio_index")
                                          .getChildFile(siteCode + "__" + group + ".json");
        auto parsed = readJsonFile(offlineFile);
        if (parsed.isVoid() || parsed.isUndefined())
        {
            juce::String message = "Offline audio index missing for " + siteCode + "/" + group;
            throw std::runtime_error(message.toStdString());
        }

        if (auto* arr = parsed.getArray())
        {
            for (auto& value : *arr)
            {
                if (value.isString())
                    objects.add(value.toString());
            }
        }
        else if (auto* obj = parsed.getDynamicObject())
        {
            for (auto& prop : obj->getProperties())
            {
                if (prop.value.isString())
                    objects.add(prop.value.toString());
            }
        }
        else if (parsed.isString())
        {
            objects.add(parsed.toString());
        }

        if (log)
            log("[offline] audio index " + offlineFile.getFullPathName());
    }
    else
    {
        if (log)
            log("[gcs] list " + makeGsUrl(bucket, listing.prefix));

        std::unordered_set<std::string> visited;
        gcsListRecursive(bucket, listing.prefix, objects, visited);
    }

    listing.totalListed = objects.size();

    std::vector<juce::String> audioObjects;
    audioObjects.reserve(objects.size());
    for (auto& obj : objects)
        if (hasAudioExt(obj))
            audioObjects.push_back(obj);

    std::sort(audioObjects.begin(), audioObjects.end(), [](const juce::String& a, const juce::String& b)
    {
        return a.compareIgnoreCase(b) < 0;
    });

    for (size_t i = 0; i < std::min<size_t>(30, audioObjects.size()); ++i)
        listing.sampleAll.add(audioObjects[i]);

    std::unordered_set<std::string> seenObjects;
    seenObjects.reserve(audioObjects.size());

    for (auto& obj : audioObjects)
    {
        auto key = obj.toStdString();
        if (! seenObjects.insert(key).second)
            continue;

        juce::Time spanStart;
        juce::Time spanEnd;
        if (parseAudioTimesFromName(obj, spanStart, spanEnd))
        {
            listing.spans.push_back({ obj, spanStart, spanEnd });
        }
        else
        {
            if (listing.sampleDropped.size() < 30)
                listing.sampleDropped.add(obj + " | parse_failed");
        }
    }

    std::sort(listing.spans.begin(), listing.spans.end(), [](const AudioSpan& a, const AudioSpan& b)
    {
        if (timeLessThan(a.start, b.start)) return true;
        if (timeLessThan(b.start, a.start)) return false;
        return a.object.compareIgnoreCase(b.object) < 0;
    });

    listing.uniqueObjects.clear();
    listing.uniqueObjects.reserve(listing.spans.size());
    for (auto& span : listing.spans)
        listing.uniqueObjects.push_back(span.object);

    for (size_t i = 0; i < std::min<size_t>(30, listing.uniqueObjects.size()); ++i)
        listing.sampleKept.add(listing.uniqueObjects[i]);

    if (log)
        log("Audio spans indexed: " + juce::String((int) listing.spans.size()));

    return listing;
}

bool SanctSoundClient::setDestinationDirectory(const juce::File& directory)
{
    juce::File target = directory;
    if (target.exists())
    {
        if (! target.isDirectory())
            return false;
    }
    else
    {
        if (! target.createDirectory())
            return false;
    }

    destinationDir = target;
    return true;
}

juce::File SanctSoundClient::getDestinationDirectory() const
{
    return destinationDir;
}

juce::StringArray SanctSoundClient::siteLabels() const
{
    juce::StringArray labels;
    for (auto& code : kKnownCodes)
        labels.add(siteLabelForCode(code));
    labels.sort(true);
    return labels;
}

juce::String SanctSoundClient::codeForLabel(const juce::String& label) const
{
    return labelToCode(label);
}

std::vector<ProductGroup> SanctSoundClient::listProductGroups(const juce::String& site,
                                                              const juce::String& tag,
                                                              LogFn log) const
{
    if (offlineEnabled)
    {
        std::vector<ProductGroup> offlineGroups;
        auto offlineFile = offlineDataRoot.getChildFile("product_groups.json");
        auto parsed = readJsonFile(offlineFile);
        if (parsed.isVoid() || parsed.isUndefined())
            throw std::runtime_error("Offline product_groups.json missing");

        auto siteEntry = parsed.getProperty(site, juce::var());
        if (siteEntry.isVoid() || siteEntry.isUndefined())
            return offlineGroups;

        const auto tagLower = tag.trim().toLowerCase();

        auto processGroup = [&](const juce::String& groupName, const juce::var& value)
        {
            if (groupName.isEmpty())
                return;
            if (tagLower.isNotEmpty() && ! groupName.toLowerCase().contains(tagLower))
                return;

            ProductGroup group;
            group.name = groupName;

            if (value.isObject())
            {
                group.mode = value.getProperty("mode", juce::String());
                auto pathsVar = value.getProperty("paths", juce::var());
                if (auto* arr = pathsVar.getArray())
                {
                    for (auto& entry : *arr)
                        if (entry.isString())
                            group.paths.add(entry.toString());
                }
                else if (pathsVar.isString())
                {
                    group.paths.add(pathsVar.toString());
                }
            }
            else if (value.isString())
            {
                group.paths.add(value.toString());
            }

            if (group.paths.isEmpty())
                return;

            for (auto& path : group.paths)
            {
                auto ext = path.fromLastOccurrenceOf(".", true, false).toLowerCase();
                if (ext.isNotEmpty())
                    group.extCounts[ext]++;
            }

            offlineGroups.push_back(group);
        };

        if (auto* dyn = siteEntry.getDynamicObject())
        {
            for (auto& prop : dyn->getProperties())
                processGroup(prop.name.toString(), prop.value);
        }
        else if (auto* arr = siteEntry.getArray())
        {
            for (auto& entry : *arr)
            {
                juce::String groupName = entry.getProperty("name", juce::String());
                if (groupName.isEmpty())
                    continue;
                processGroup(groupName, entry);
            }
        }

        std::sort(offlineGroups.begin(), offlineGroups.end(), [](const ProductGroup& a, const ProductGroup& b)
        {
            return a.name.compareIgnoreCase(b.name) < 0;
        });

        if (log)
            log("[offline] product groups " + offlineFile.getFullPathName());

        return offlineGroups;
    }

    const juce::String sitePrefix = productsPrefix + "/" + site + "/";
    const juce::String tagLower = tag.trim().toLowerCase();

    std::map<juce::String, ProductGroup, std::less<>> groups;
    bool foundAny = false;

    if (log)
        log("[gcs] list " + makeGsUrl(gcsBucket, sitePrefix));

    juce::StringArray objects;
    std::unordered_set<std::string> visited;
    gcsListRecursive(gcsBucket, sitePrefix, objects, visited);

    for (auto& objectName : objects)
    {
        if (tagLower.isNotEmpty() && ! objectName.toLowerCase().contains(tagLower))
            continue;
        if (! objectName.endsWithIgnoreCase(".csv"))
            continue;

        juce::String groupName;
        if (objectName.startsWith(sitePrefix))
        {
            auto remainder = objectName.substring(sitePrefix.length());
            groupName = remainder.upToFirstOccurrenceOf("/", false, false);
        }

        if (groupName.isEmpty())
            groupName = objectName.fromLastOccurrenceOf("/", false, false)
                                   .upToLastOccurrenceOf(".", false, false);

        auto& group = groups[groupName];
        group.name = groupName;
        auto gsUrl = makeGsUrl(gcsBucket, objectName);
        group.paths.add(gsUrl);
        auto ext = gsUrl.fromLastOccurrenceOf(".", true, false).toLowerCase();
        group.extCounts[ext]++;
        foundAny = true;
    }

    if (! foundAny && log)
        log("  -> no matches");

    std::vector<ProductGroup> out;
    for (auto& kv : groups)
        out.push_back(kv.second);
    std::sort(out.begin(), out.end(), [](const ProductGroup& a, const ProductGroup& b) { return a.name < b.name; });
    return out;
}

MetadataSummary SanctSoundClient::fetchMetadataSummary(const juce::String& site,
                                                       const juce::String& group,
                                                       juce::String& rawText,
                                                       LogFn log) const
{
    const juce::String metadataPrefix = productsPrefix + "/" + site + "/" + group + "/metadata/";

    if (log)
        log("[gcs] list " + makeGsUrl(gcsBucket, metadataPrefix));

    juce::StringArray urls;

    juce::String pageToken;
    do
    {
        juce::var response = pageToken.isEmpty()
                                 ? fetchGcsJsonFirstPage(gcsBucket, metadataPrefix)
                                 : fetchGcsJsonPage(gcsBucket, metadataPrefix, "/", pageToken);

        auto* obj = response.getDynamicObject();
        if (obj == nullptr)
            break;

        auto itemsVar = obj->getProperty("items");
        if (auto* itemsArray = itemsVar.getArray())
        {
            for (auto& item : *itemsArray)
            {
                auto* itemObj = item.getDynamicObject();
                if (itemObj == nullptr)
                    continue;
                auto nameVar = itemObj->getProperty("name");
                if (! nameVar.isString())
                    continue;
                auto objectName = nameVar.toString();
                if (! objectName.endsWithIgnoreCase(".json"))
                    continue;
                urls.add(makeGsUrl(gcsBucket, objectName));
            }
        }

        auto nextVar = obj->getProperty("nextPageToken");
        pageToken = nextVar.isString() ? nextVar.toString() : juce::String();
    } while (pageToken.isNotEmpty());

    urls.sort(true);

    if (urls.isEmpty())
    {
        rawText = "(no metadata/*.json files found)";
        return {};
    }

    juce::StringArray snippets;
    juce::var parsed;

    for (int i = 0; i < juce::jmin(2, urls.size()); ++i)
    {
        auto url = urls[i];
        juce::String bucket;
        juce::String objectName;
        if (! parseGsUrl(url, bucket, objectName))
            continue;

        auto httpUrl = makeHttpsUrl(bucket, objectName);
        if (log)
            log("[http] GET " + httpUrl);

        int statusCode = 0;
        juce::StringPairArray headers;
        juce::URL u(httpUrl);
        auto stream = u.createInputStream(juce::URL::InputStreamOptions(juce::URL::ParameterHandling::inAddress)
                                              .withConnectionTimeoutMs(15000)
                                              .withResponseHeaders(&headers)
                                              .withStatusCode(&statusCode));
        if (stream == nullptr || statusCode >= 400)
            continue;

        auto text = stream->readEntireStreamAsString().trim();
        snippets.add("// [" + juce::String(i + 1) + "/" + juce::String(urls.size()) + "] " + url + "\n" + text);
        if (parsed.isVoid())
            parsed = parseJson(text);
    }

    rawText = snippets.joinIntoString("\n\n");
    if (parsed.isVoid())
        return {};

    auto summary = buildSummaryFromJson(parsed);
    summary.rawText = rawText;
    return summary;
}

PreviewResult SanctSoundClient::previewGroup(const juce::String& site,
                                             const ProductGroup& group,
                                             bool onlyLongRuns,
                                             LogFn log) const
{
    PreviewResult result;

    juce::String mode = group.mode;
    if (mode.isEmpty())
    {
        auto lowerName = group.name.toLowerCase();
        if (lowerName.endsWith("_1h"))
            mode = "HOUR";
        else if (lowerName.endsWith("_1d"))
            mode = "DAY";
        else
            mode = "EVENT";
    }
    else
    {
        mode = mode.toUpperCase();
        if (mode != "HOUR" && mode != "DAY" && mode != "EVENT")
        {
            auto lowerName = group.name.toLowerCase();
            if (lowerName.endsWith("_1h"))
                mode = "HOUR";
            else if (lowerName.endsWith("_1d"))
                mode = "DAY";
            else
                mode = "EVENT";
        }
    }

    result.mode = mode;

    auto siteCode = site.trim().toLowerCase();
    auto preferredFolder = folderFromSetName(group.name);

    if (log)
    {
        juce::String msg = "preview: site=" + siteCode + " prefFolder="
                            + (preferredFolder.isNotEmpty() ? preferredFolder : juce::String("<none>"));
        log(msg + "\n");
    }

    if (preferredFolder.isNotEmpty())
        logPreviewExplain("resolved folder=" + preferredFolder);
    else
        logPreviewExplain("resolved folder=<none>");

    const juce::String siteLower = siteCode;
    const juce::String setName = group.name;
    juce::File dbg = previewDebugOverrideDir;
    if (dbg.getFullPathName().isEmpty())
        dbg = makePreviewDebugDir(destinationDir, setName);
    else
        dbg.createDirectory();
    dumpAllAudioForSite(siteLower, dbg);

    auto bestFiles = chooseBestFiles(group.paths);
    auto downloaded = downloadFilesTo(bestFiles, destinationDir, offlineDataRoot, log);

    juce::Array<juce::File> localCsvs;
    for (auto& item : downloaded)
        if (item.localFile.hasFileExtension("csv"))
            localCsvs.add(item.localFile);

    if (localCsvs.isEmpty())
        throw std::runtime_error("Preview expects at least one CSV artifact");

    std::vector<std::pair<juce::Time, juce::Time>> windows;
    juce::String runsText;
    std::optional<juce::Time> tmin;
    std::optional<juce::Time> tmax;

    if (mode == "HOUR")
    {
        std::vector<juce::Time> combinedHours;
        std::set<juce::int64> seenHours;
        for (auto& csv : localCsvs)
        {
            auto hours = parsePresenceHoursFromCsv(csv);
            for (auto& h : hours)
                if (seenHours.insert(h.toMilliseconds()).second)
                    combinedHours.push_back(h);
        }

        std::sort(combinedHours.begin(), combinedHours.end(), [](const juce::Time& a, const juce::Time& b)
        {
            return timeLessThan(a, b);
        });

        juce::Array<juce::Time> hoursArray;
        for (auto& h : combinedHours)
            hoursArray.add(h);

        auto runs = groupConsecutive(hoursArray, juce::RelativeTime::hours(1));
        juce::Array<PreviewWindow> runsToUse = runs;
        if (onlyLongRuns)
        {
            juce::Array<PreviewWindow> filtered;
            for (auto& r : runsToUse)
                if ((r.end - r.start).inHours() >= 2.0)
                    filtered.add(r);
            runsToUse = filtered;
        }

        auto expanded = expandRuns(runsToUse, juce::RelativeTime::hours(1));

        windows.clear();
        windows.reserve((size_t) expanded.size());
        for (auto& hour : expanded)
            windows.emplace_back(hour, hour + juce::RelativeTime::hours(1));

        if (! expanded.isEmpty())
        {
            tmin = expanded.getFirst();
            tmax = expanded.getLast() + juce::RelativeTime::hours(1);
        }

        runsText << "Runs (" << runsToUse.size() << "):\n";
        int idx = 1;
        for (auto& run : runsToUse)
        {
            runsText << juce::String(idx++).paddedLeft('0', 2) << ". "
                     << toIso(run.start) << " -> " << toIso(run.end) << "\n";
        }
    }
    else if (mode == "DAY")
    {
        std::vector<juce::Time> combinedDays;
        std::set<juce::int64> seenDays;
        for (auto& csv : localCsvs)
        {
            auto days = parsePresenceDaysFromCsv(csv);
            for (auto& d : days)
                if (seenDays.insert(d.toMilliseconds()).second)
                    combinedDays.push_back(d);
        }

        std::sort(combinedDays.begin(), combinedDays.end(), [](const juce::Time& a, const juce::Time& b)
        {
            return timeLessThan(a, b);
        });

        windows.clear();
        windows.reserve(combinedDays.size());
        for (auto& day : combinedDays)
            windows.emplace_back(day, day + juce::RelativeTime::days(1));

        if (! combinedDays.empty())
        {
            tmin = combinedDays.front();
            tmax = combinedDays.back() + juce::RelativeTime::days(1);
        }

        runsText << "Days: " << combinedDays.size() << "\n";
    }
    else
    {
        std::vector<EventWindow> combinedEvents;
        for (auto& csv : localCsvs)
        {
            juce::Array<EventWindow> parsed;
            if (SanctSoundClient::parseEventsFromCsv(csv, parsed))
            {
                for (const auto& evt : parsed)
                    combinedEvents.push_back(evt);
            }
        }

        std::sort(combinedEvents.begin(), combinedEvents.end(), [](const EventWindow& a, const EventWindow& b)
        {
            auto sa = a.startUTC.toMilliseconds();
            auto sb = b.startUTC.toMilliseconds();
            if (sa != sb)
                return sa < sb;
            return a.endUTC.toMilliseconds() < b.endUTC.toMilliseconds();
        });

        combinedEvents.erase(std::unique(combinedEvents.begin(), combinedEvents.end(), [](const EventWindow& a, const EventWindow& b)
        {
            return a.startUTC.toMilliseconds() == b.startUTC.toMilliseconds()
                && a.endUTC.toMilliseconds() == b.endUTC.toMilliseconds();
        }), combinedEvents.end());

        windows.clear();
        windows.reserve(combinedEvents.size());
        for (const auto& evt : combinedEvents)
            windows.emplace_back(evt.startUTC, evt.endUTC);

        if (! combinedEvents.empty())
        {
            tmin = combinedEvents.front().startUTC;
            tmax = combinedEvents.back().endUTC;
        }

        runsText << "Events: " << combinedEvents.size() << "\n";
    }

    if (log)
        log("CSV windows parsed: " + juce::String((int) windows.size()) + "\n");

    if (log)
    {
        juce::String tminText = tmin.has_value() ? toIso(*tmin) : juce::String("<none>");
        juce::String tmaxText = tmax.has_value() ? toIso(*tmax) : juce::String("<none>");
        log("Windows: " + juce::String((int) windows.size())
            + " | tmin=" + tminText + " | tmax=" + tmaxText + "\n");
    }

    writeWindowsDebug(windows);

    if (windows.empty() && log)
    {
        log("[WARN] No windows parsed from CSV artifacts.\n");
        if (! localCsvs.isEmpty())
        {
            auto sample = readCsvLoose(localCsvs.getFirst());
            log("Detected columns: " + sample.header.joinIntoString(", ") + "\n");
            if (! sample.rows.empty())
            {
                auto preview = sample.rows.front().joinIntoString(", ");
                log("Sample row: " + preview + "\n");
            }
        }
    }

    juce::Array<AudioHour> audioRows;
    if (! windows.empty())
        audioRows = listAudioAcross(siteCode, preferredFolder, tmin, tmax, log, dbg);
    if (log)
        log("Audio rows collected: " + juce::String(audioRows.size()) + "\n");

    writeCandidateDebug(audioRows);

    {
        juce::File dbgFile = dbg.getChildFile("PARSE_" + setName + ".txt");
        juce::FileOutputStream os(dbgFile, true);
        if (os.openedOk())
        {
            for (const auto& h : audioRows)
                os << h.fname << "  ->  " << toIsoUTC(h.startUtc) << "  ..  " << toIsoUTC(h.endUtc) << "\n";
        }
    }

    juce::StringArray urls;
    juce::StringArray names;
    juce::Array<NeededFileRow> neededRows;
    int unmatched = 0;
    {
        juce::File w = dbg.getChildFile("WINDOWS_summary.txt");
        juce::FileOutputStream os(w);
        if (os.openedOk())
        {
            os << "windows: " << (int) windows.size() << "\n";
            for (size_t i = 0; i < windows.size(); ++i)
                os << juce::String((int) (i + 1)).paddedLeft('0', 3) << "  "
                   << toIsoUTC(windows[i].first) << "  ..  " << toIsoUTC(windows[i].second) << "\n";
        }
    }
    minimalFilesForWindows(audioRows, windows, urls, names, neededRows, unmatched, dbg);

    writeSelectedDebug(urls);

    if (log)
    {
        log("Minimal selection urls=" + juce::String(urls.size())
            + " names=" + juce::String(names.size()) + "\n");
        if (! names.isEmpty())
            log("Selected basenames: " + names.joinIntoString(", ") + "\n");
        if (unmatched > 0)
            log("[WARN] unmatched windows: " + juce::String(unmatched) + "\n");
        for (auto& row : neededRows)
        {
            auto label = toIso(row.start) + " -> " + toIso(row.end);
            if (row.names.isEmpty())
                log("  window " + label + " -> (no audio)\n");
            else
                log("  window " + label + " -> " + row.names + "\n");
        }
    }

    dumpLines(dbg.getChildFile("ALL_candidates_urls.txt"),
              rowsToUrls(audioRows));

    juce::StringArray filteredUrls;
    for (auto const& u : urls)
        filteredUrls.add(u);
    dumpLines(dbg.getChildFile("FILTERED_" + setName + "_urls.txt"), filteredUrls);

    juce::StringArray filteredNames;
    for (auto const& u : filteredUrls)
        filteredNames.add(juce::URL(u).getFileName());
    dumpLines(dbg.getChildFile("FILTERED_" + setName + "_basenames.txt"), filteredNames);

    DBG("SanctSound: debug written to: " + dbg.getFullPathName());

    dumpFilteredSelection(setName, urls, dbg);

    result.urls = urls;
    result.names = names;
    result.matchedObjects = urls;

    juce::Array<PreviewWindow> previewWindows;
    for (auto& w : windows)
        previewWindows.add({ w.first, w.second });
    result.windows = previewWindows;
    result.matchedWindows = juce::jmax(0, (int) windows.size() - unmatched);
    result.unmatchedWindows = unmatched;
    result.runsText = runsText;

    juce::StringArray unmatchedSummaries;
    for (auto& row : neededRows)
    {
        if (row.names.isEmpty())
            unmatchedSummaries.add(toIso(row.start) + " -> " + toIso(row.end));
    }
    result.unmatchedSummaries = unmatchedSummaries;

    juce::String countLabel("events");
    if (mode == "HOUR")
        countLabel = "hours";
    else if (mode == "DAY")
        countLabel = "days";

    const juce::String modeLabel = mode.toLowerCase();

    result.summary = group.name + " | mode: " + modeLabel
                   + " | " + countLabel + ": " + juce::String((int) windows.size())
                   + " | unique files: " + juce::String(names.size());

    juce::Array<ListedFile> listed;
    std::map<juce::String, const AudioHour*, std::less<>> byName;
    for (auto& row : audioRows)
    {
        if (byName.find(row.fname) == byName.end())
            byName[row.fname] = &row;
    }

    for (auto& name : names)
    {
        auto it = byName.find(name);
        if (it == byName.end())
            continue;
        const auto* row = it->second;
        ListedFile lf;
        lf.url = row->url;
        lf.name = row->fname;
        lf.start = row->startUtc;
        lf.end = row->endUtc;
        lf.folder = row->folder;
        listed.add(lf);
    }
    result.files = listed;

    auto dest = getDestinationDirectory();
    writeTextFile(dest.getChildFile(group.name + "_preview_needed_files.txt"), urls.joinIntoString("\n"));
    writeTextFile(dest.getChildFile(group.name + "_preview_needed_fnames.txt"), names.joinIntoString("\n"));

    return result;
}

ClipSummary SanctSoundClient::clipGroups(const juce::Array<juce::String>& groups,
                                         const std::map<juce::String, PreviewCache>& cache,
                                         const juce::StringArray& selectedBasenames,
                                         LogFn log) const
{
    ClipSummary summary;
    summary.totalWindows = 0;
    summary.written = 0;
    summary.skipped = 0;

    if (groups.isEmpty())
        return summary;

    juce::Array<LocalAudio> local;
    std::set<juce::String, std::less<>> selectedSet;
    for (auto& base : selectedBasenames)
        selectedSet.insert(base);

    for (auto& base : selectedBasenames)
    {
        auto src = destinationDir.getChildFile(base);
        if (! src.existsAsFile())
        {
            if (log)
                log("missing source: " + src.getFullPathName());
            continue;
        }

        juce::Time parsedStart;
        if (! parseAudioStartFromName(base, parsedStart))
        {
            if (log)
                log("[WARN] Unable to parse start time from " + base);
            continue;
        }

        LocalAudio audio;
        audio.file = src;
        audio.name = base;
        audio.start = parsedStart;
        audio.folder = folderFromSetName(audio.name);
        local.add(audio);
    }

    std::sort(local.begin(), local.end(), [](const LocalAudio& a, const LocalAudio& b)
    {
        return timeLessThan(a.start, b.start);
    });

    for (int i = 0; i < local.size(); ++i)
    {
        if (i + 1 < local.size())
        {
            auto end = local[i + 1].start;
            if (! timeLessThan(local[i].start, end))
                end = local[i].start + juce::RelativeTime::seconds(1);
            local.getReference(i).end = end;
        }
        else
        {
            double durationSeconds = 0.0;
            auto err = ffprobeDuration(local[i].file, durationSeconds);
            if (err.isNotEmpty() || durationSeconds <= 1.0)
                durationSeconds = 3600.0;
            local.getReference(i).end = local[i].start + juce::RelativeTime::seconds(durationSeconds);
        }
    }

    auto coverAndNext = [&](const juce::Time& ts) -> std::pair<LocalAudio*, LocalAudio*>
    {
        LocalAudio* current = nullptr;
        LocalAudio* next = nullptr;
        for (int i = 0; i < local.size(); ++i)
        {
            if (timeLessThanOrEqual(local[i].start, ts))
                current = &local.getReference(i);
            else
            {
                next = &local.getReference(i);
                break;
            }
        }
        return { current, next };
    };

    auto clipsRoot = destinationDir.getChildFile("clips");
    if (! clipsRoot.exists())
    {
        if (! clipsRoot.createDirectory())
            throw std::runtime_error("Failed to create directory: " + clipsRoot.getFullPathName().toStdString());
    }
    else if (! clipsRoot.isDirectory())
    {
        throw std::runtime_error("Destination exists but is not a directory: "
                                 + clipsRoot.getFullPathName().toStdString());
    }

    for (auto& grp : groups)
    {
        auto itCache = cache.find(grp);
        if (itCache == cache.end())
        {
            if (log)
                log("[WARN] No preview cache for " + grp);
            continue;
        }

        const auto& preview = itCache->second;
        summary.mode = preview.mode;

        auto outDir = clipsRoot.getChildFile(grp);
        if (outDir.exists())
        {
            if (! outDir.isDirectory())
                throw std::runtime_error("Destination exists but is not a directory: "
                                         + outDir.getFullPathName().toStdString());
        }
        else if (! outDir.createDirectory())
        {
            throw std::runtime_error("Failed to create directory: " + outDir.getFullPathName().toStdString());
        }

        summary.directory = outDir;

        juce::Array<ClipRow> manifest;
        int written = 0;
        int skipped = 0;

        for (auto& window : preview.windows)
        {
            summary.totalWindows++;

            ClipRow row;
            row.startIso = window.start.toISO8601(true);
            row.endIso = window.end.toISO8601(true);
            row.mode = preview.mode;
            row.durationSeconds = (window.end - window.start).inSeconds();

            auto [current, next] = coverAndNext(window.start);

            if (current == nullptr)
            {
                row.status = "missing_source";
                manifest.add(row);
                skipped++;
                continue;
            }

            bool spansNext = timeLessThan(current->end, window.end);
            if (! selectedSet.count(current->name))
            {
                row.status = "not_selected";
                row.sourceNames = current->name;
                manifest.add(row);
                skipped++;
                continue;
            }

            juce::StringArray sourceNames;
            sourceNames.add(current->name);

            double totalDuration = (window.end - window.start).inSeconds();
            if (totalDuration <= 0.0)
            {
                row.status = "nonpositive_window";
                row.sourceNames = sourceNames.joinIntoString(" + ");
                manifest.add(row);
                skipped++;
                continue;
            }

            juce::File outFile;
            juce::String clipName;
            juce::String errMessage;

            if (! spansNext || next == nullptr)
            {
                double startSeconds = (window.start - current->start).inSeconds();
                double durationSeconds = totalDuration;

                clipName = current->name.upToLastOccurrenceOf(".", false, false)
                           + "__" + stampForFilename(window.start)
                           + "_" + stampForFilename(window.end) + ".wav";
                outFile = outDir.getChildFile(clipName);

                errMessage = ffmpegCut(current->file, startSeconds, durationSeconds, outFile,
                                        clipSampleRate, clipMono, clipSampleFormat);
            }
            else
            {
                if (! selectedSet.count(next->name))
                {
                    row.status = "requires_next";
                    row.sourceNames = current->name + " + " + next->name;
                    manifest.add(row);
                    skipped++;
                    continue;
                }

                double partA = (current->end - window.start).inSeconds();
                double partB = (window.end - next->start).inSeconds();
                if (partA <= 0.0 || partB <= 0.0)
                {
                    row.status = "invalid_span";
                    row.sourceNames = current->name + " + " + next->name;
                    manifest.add(row);
                    skipped++;
                    continue;
                }

                auto tempA = juce::File::createTempFile("clip_a.wav");
                auto tempB = juce::File::createTempFile("clip_b.wav");

                clipName = current->name.upToLastOccurrenceOf(".", false, false)
                           + "__" + stampForFilename(window.start)
                           + "_" + stampForFilename(window.end) + ".wav";
                outFile = outDir.getChildFile(clipName);

                errMessage = ffmpegCut(current->file, (window.start - current->start).inSeconds(), partA,
                                       tempA, clipSampleRate, clipMono, clipSampleFormat);
                if (errMessage.isEmpty())
                    errMessage = ffmpegCut(next->file, 0.0, partB,
                                           tempB, clipSampleRate, clipMono, clipSampleFormat);
                if (errMessage.isEmpty())
                    errMessage = ffmpegConcat(tempA, tempB, outFile);

                tempA.deleteFile();
                tempB.deleteFile();

                sourceNames.add(next->name);
            }

            if (errMessage.isNotEmpty())
            {
                if (log)
                    log("[WARN] " + errMessage);
                if (outFile.existsAsFile())
                    outFile.deleteFile();
                row.status = "ffmpeg_error";
                row.sourceNames = sourceNames.joinIntoString(" + ");
                manifest.add(row);
                skipped++;
                continue;
            }

            if (! outFile.existsAsFile() || outFile.getSize() < clipMinBytes)
            {
                if (outFile.existsAsFile())
                    outFile.deleteFile();
                row.status = "too_small";
                row.sourceNames = sourceNames.joinIntoString(" + ");
                manifest.add(row);
                skipped++;
                continue;
            }

            row.clipName = outFile.getFileName();
            row.writtenPath = outFile.getFullPathName();
            row.sourceNames = sourceNames.joinIntoString(" + ");
            row.status = "written";
            manifest.add(row);
            written++;
        }

        summary.written += written;
        summary.skipped += skipped;
        summary.manifestRows.addArray(manifest);

        auto manifestFile = outDir.getChildFile("clips_manifest.csv");
        juce::FileOutputStream manifestStream(manifestFile);
        if (! manifestStream.openedOk())
            throw std::runtime_error("Failed to open manifest: " + manifestFile.getFullPathName().toStdString());
        manifestStream.writeText("clip_wav,source_flac(s),start_utc,end_utc,duration_sec,mode,status\n", false, false, "\n");
        for (auto& row : manifest)
        {
            juce::StringArray fields;
            fields.add(row.clipName);
            fields.add(row.sourceNames);
            fields.add(row.startIso);
            fields.add(row.endIso);
            fields.add(juce::String(row.durationSeconds, 3));
            fields.add(row.mode);
            fields.add(row.status);
            juce::StringArray escaped;
            for (auto& f : fields)
                escaped.add("\"" + f.replace("\"", "\"\"") + "\"");
            manifestStream.writeText(escaped.joinIntoString(",") + "\n", false, false, "\n");
        }

        auto summaryFile = outDir.getChildFile("clips_summary.txt");
        juce::String text;
        text << "Windows: " << preview.windows.size()
             << " | Clips: " << written
             << " | Skipped: " << skipped
             << " | Mode: " << preview.mode << "\n"
             << "Dir: " << outDir.getFullPathName() << "\n";
        writeTextFile(summaryFile, text);
    }

    return summary;
}

} // namespace sanctsound
