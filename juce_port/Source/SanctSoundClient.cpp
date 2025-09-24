#include "SanctSoundClient.h"

#include "Utilities.h"

#include <juce_core/juce_core.h>
#include <juce_data_structures/juce_data_structures.h>
#include <juce_audio_basics/juce_audio_basics.h>
#include <juce_audio_formats/juce_audio_formats.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <memory>
#include <string>
#include <stdexcept>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <limits>
#include <vector>

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

struct HourRow
{
    juce::String url;
    juce::String fname;
    juce::String folder;
    juce::Time   startUTC;
    juce::Time   endUTC;
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

static bool parseAudioStartFromName(const juce::String& name, juce::Time& outUTC)
{
    static const juce::RegExp re1("_([0-9]{8}T[0-9]{6}Z)\\.(flac|wav)$", true);
    static const juce::RegExp re2("_([0-9]{12})\\.(flac|wav)$", true);

    juce::String m;
    if (re1.fullMatch(name, &m))
    {
        auto ts = m; // YYYYMMDDThhmmssZ
        int y = ts.substring(0, 4).getIntValue();
        int M = ts.substring(4, 6).getIntValue();
        int d = ts.substring(6, 8).getIntValue();
        int h = ts.substring(9, 11).getIntValue();
        int n = ts.substring(11, 13).getIntValue();
        int s = ts.substring(13, 15).getIntValue();
        outUTC = juce::Time(juce::Time::convertedToUTC({ y, M, d, h, n, s }));
        return true;
    }
    else if (re2.fullMatch(name, &m))
    {
        auto ts = m; // YYMMDDhhmmss
        int yy  = ts.substring(0, 2).getIntValue();
        int yr  = (yy <= 69 ? 2000 + yy : 1900 + yy);
        int M   = ts.substring(2, 4).getIntValue();
        int d   = ts.substring(4, 6).getIntValue();
        int h   = ts.substring(6, 8).getIntValue();
        int n   = ts.substring(8, 10).getIntValue();
        int s   = ts.substring(10, 12).getIntValue();
        outUTC = juce::Time(juce::Time::convertedToUTC({ yr, M, d, h, n, s }));
        return true;
    }
    return false;
}

static juce::String folderFromSet(const juce::String& setName)
{
    static const juce::RegExp re("(sanctsound_[a-z]{2}[0-9]{2}_[0-9]{2})", true);
    juce::String m;
    if (re.fullMatch(setName.toLowerCase(), &m))
        return m;
    return {};
}

bool SanctSoundClient::parseTimeUTC(const juce::String& text, juce::Time& out)
{
    return parseTimeUTCImpl(text, out);
}

bool SanctSoundClient::parseAudioStartFromName(const juce::String& name, juce::Time& outUTC)
{
    return ::sanctsound::parseAudioStartFromName(name, outUTC);
}

juce::String SanctSoundClient::folderFromSet(const juce::String& setName)
{
    return ::sanctsound::folderFromSet(setName);
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

std::vector<std::pair<juce::Time, juce::Time>> SanctSoundClient::parseEventsFromCsv(const juce::File& file)
{
    auto csv = readCsvLoose(file);
    if (csv.header.isEmpty())
        return {};

    const int startCol = detectDatetimeColumn(csv.header, csv.rows);
    if (startCol < 0)
        throw std::runtime_error("No usable datetime column in " + file.getFileName().toStdString());

    const int maxCols = maxColumnCount(csv.header, csv.rows);
    const int minHits = std::max(1, std::min((int) csv.rows.size(), std::max(3, (int) std::ceil(csv.rows.size() * 0.05))));

    int endCol = -1;
    for (int col = 0; col < maxCols; ++col)
    {
        if (col == startCol)
            continue;
        if (col < csv.header.size() && headerLooksLikeEnd(csv.header[col]) && countDatetimeHits(col, csv.rows) > 0)
        {
            endCol = col;
            break;
        }
    }

    if (endCol < 0)
    {
        for (int col = 0; col < maxCols; ++col)
        {
            if (col == startCol)
                continue;
            if (countDatetimeHits(col, csv.rows) >= minHits)
            {
                endCol = col;
                break;
            }
        }
    }

    const int durationCol = findDurationColumn(csv.header);
    std::set<std::pair<juce::int64, juce::int64>> seen;
    std::vector<std::pair<juce::Time, juce::Time>> events;

    for (auto& row : csv.rows)
    {
        if (startCol >= row.size())
            continue;

        juce::Time start;
        if (! parseTimeUTC(row[startCol], start))
            continue;

        juce::Time end;
        bool haveEnd = false;

        if (endCol >= 0 && endCol < row.size())
            haveEnd = parseTimeUTC(row[endCol], end);

        if (! haveEnd && durationCol >= 0 && durationCol < row.size())
        {
            auto field = row[durationCol].trim();
            if (field.isNotEmpty() && isNumeric(field))
            {
                auto seconds = field.getDoubleValue();
                if (seconds > 0.0)
                {
                    end = start + juce::RelativeTime::seconds(seconds);
                    haveEnd = true;
                }
            }
        }

        if (! haveEnd)
            end = start + juce::RelativeTime::seconds(60.0);

        if (! timeLessThan(start, end))
            continue;

        auto key = std::make_pair(start.toMilliseconds(), end.toMilliseconds());
        if (seen.insert(key).second)
            events.emplace_back(start, end);
    }

    std::sort(events.begin(), events.end(), [](const auto& a, const auto& b)
    {
        if (timeLessThan(a.first, b.first))
            return true;
        if (timeLessThan(b.first, a.first))
            return false;
        return timeLessThan(a.second, b.second);
    });

    events.erase(std::unique(events.begin(), events.end(), [](const auto& a, const auto& b)
    {
        return a.first.toMilliseconds() == b.first.toMilliseconds()
            && a.second.toMilliseconds() == b.second.toMilliseconds();
    }), events.end());

    return events;
}

void SanctSoundClient::buildHoursFromRows(std::vector<HourRow>& rows)
{
    std::sort(rows.begin(), rows.end(),
              [](const HourRow& a, const HourRow& b)
              {
                  if (timeLessThan(a.startUTC, b.startUTC))
                      return true;
                  if (timeLessThan(b.startUTC, a.startUTC))
                      return false;
                  return a.folder.compareIgnoreCase(b.folder) < 0;
              });

    for (size_t i = 0; i < rows.size(); ++i)
    {
        if (i + 1 < rows.size() && rows[i + 1].folder.equalsIgnoreCase(rows[i].folder))
        {
            rows[i].endUTC = rows[i + 1].startUTC;
            if (! timeLessThan(rows[i].startUTC, rows[i].endUTC))
                rows[i].endUTC = rows[i].startUTC + juce::RelativeTime::seconds(1);
        }
        else
        {
            rows[i].endUTC = rows[i].startUTC + juce::RelativeTime::hours(1);
        }
    }
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

void SanctSoundClient::minimalUnionForWindows(const std::vector<HourRow>& files,
                                              const std::vector<std::pair<juce::Time, juce::Time>>& windows,
                                              juce::StringArray& outUrls,
                                              juce::StringArray& outNames)
{
    outUrls.clear();
    outNames.clear();

    if (files.empty() || windows.empty())
        return;

    std::vector<const HourRow*> sortedFiles;
    sortedFiles.reserve(files.size());
    for (auto& f : files)
        sortedFiles.push_back(&f);

    std::sort(sortedFiles.begin(), sortedFiles.end(), [](const HourRow* a, const HourRow* b)
    {
        if (timeLessThan(a->startUTC, b->startUTC))
            return true;
        if (timeLessThan(b->startUTC, a->startUTC))
            return false;
        return a->folder.compareIgnoreCase(b->folder) < 0;
    });

    std::vector<juce::int64> starts;
    starts.reserve(sortedFiles.size());
    for (auto* f : sortedFiles)
        starts.push_back(f->startUTC.toMilliseconds());

    std::set<juce::String, std::less<>> urlSet;
    std::set<juce::String, std::less<>> nameSet;

    for (auto& window : windows)
    {
        auto it = std::upper_bound(starts.begin(), starts.end(), window.first.toMilliseconds());
        if (it == starts.begin())
            continue;

        size_t index = static_cast<size_t>(std::distance(starts.begin(), it - 1));
        const auto* first = sortedFiles[index];
        urlSet.insert(first->url);
        nameSet.insert(first->fname);

        bool covers = ! timeLessThan(first->endUTC, window.second);
        if (! covers && index + 1 < sortedFiles.size())
        {
            const auto* second = sortedFiles[index + 1];
            urlSet.insert(second->url);
            nameSet.insert(second->fname);
        }
    }

    for (auto& url : urlSet)
        outUrls.add(url);
    for (auto& name : nameSet)
        outNames.add(name);
}

std::vector<juce::String> SanctSoundClient::listSiteDeployments(const juce::String& site,
                                                               const std::function<void(const juce::String&)>& log) const
{
    std::vector<juce::String> deployments;

    auto siteCode = site.trim().toLowerCase();
    if (siteCode.isEmpty())
        return deployments;

    juce::String prefix = audioPrefix;
    if (! prefix.endsWithChar('/'))
        prefix << "/";

    juce::String base = "gs://" + gcsBucket + "/" + prefix + siteCode + "/";

    juce::StringArray cmd { "gsutil", "ls", base };
    if (log)
        log("LIST deployments: " + cmd.joinIntoString(" ") + "\n");

    juce::ChildProcess process;
    if (! process.start(cmd))
    {
        if (log)
            log("Failed to start gsutil\n");
        return deployments;
    }

    juce::String allOutput = process.readAllProcessOutput();
    int exitCode = process.waitForProcessToFinish(-1);
    if (log)
        log("deployments exit: " + juce::String(exitCode) + "\n");

    juce::StringArray lines;
    lines.addLines(allOutput);
    if (log)
        log("deployments lines: " + juce::String(lines.size()) + "\n");

    for (auto& line : lines)
    {
        juce::String trimmed = line.trim();
        if (! trimmed.startsWithIgnoreCase("gs://"))
            continue;
        if (! trimmed.endsWithChar('/'))
            continue;

        auto withoutSlash = trimmed.substring(0, trimmed.length() - 1);
        auto lastSlash = withoutSlash.lastIndexOfChar('/');
        if (lastSlash < 0)
            continue;

        juce::String name = withoutSlash.substring(lastSlash + 1).toLowerCase();
        if (name.startsWithIgnoreCase("sanctsound_"))
            deployments.push_back(name);
    }

    std::sort(deployments.begin(), deployments.end());
    deployments.erase(std::unique(deployments.begin(), deployments.end()), deployments.end());

    if (log)
        log("deployments kept: " + juce::String((int) deployments.size()) + "\n");

    return deployments;
}

std::vector<SanctSoundClient::HourRow> SanctSoundClient::listAudioFilesInFolder(const juce::String& site,
                                                                                const juce::String& folder,
                                                                                juce::Optional<juce::Time> tmin,
                                                                                juce::Optional<juce::Time> tmax,
                                                                                const std::function<void(const juce::String&)>& log,
                                                                                juce::String* commandOut,
                                                                                juce::StringArray* linesOut) const
{
    std::vector<HourRow> rows;

    auto siteCode = site.trim().toLowerCase();
    auto folderName = folder.trim().toLowerCase();
    if (siteCode.isEmpty() || folderName.isEmpty())
        return rows;

    juce::String prefix = audioPrefix;
    if (! prefix.endsWithChar('/'))
        prefix << "/";

    juce::String pattern = "gs://" + gcsBucket + "/" + prefix + siteCode + "/" + folderName + "/audio/*.flac";
    juce::StringArray cmd { "gsutil", "ls", "-r", pattern };
    juce::String cmdString = cmd.joinIntoString(" ");
    if (commandOut)
        *commandOut = cmdString;
    if (log)
        log("LIST audio: " + cmdString + "\n");

    juce::ChildProcess process;
    if (! process.start(cmd))
    {
        if (log)
            log("Failed to start gsutil\n");
        return rows;
    }

    juce::String output = process.readAllProcessOutput();
    int exitCode = process.waitForProcessToFinish(-1);
    if (log)
        log("audio exit: " + juce::String(exitCode) + "\n");

    juce::StringArray lines;
    lines.addLines(output);
    if (linesOut)
    {
        linesOut->clear();
        auto limit = juce::jmin(50, lines.size());
        for (int i = 0; i < limit; ++i)
            linesOut->add(lines[i]);
    }
    if (log)
        log("audio lines: " + juce::String(lines.size()) + "\n");

    HourRow leftCandidate{};
    bool haveLeft = false;
    int parsed = 0;
    int kept = 0;
    int skippedBefore = 0;
    int skippedAfter = 0;
    int skippedNoTimestamp = 0;
    int skippedOther = 0;

    for (auto& line : lines)
    {
        juce::String url = line.trim();
        if (! url.startsWith("gs://"))
        {
            if (url.isNotEmpty())
                ++skippedOther;
            continue;
        }

        if (! url.endsWithIgnoreCase(".flac"))
        {
            ++skippedOther;
            continue;
        }

        ++parsed;

        juce::String fname = url.fromLastOccurrenceOf("/", false, false);
        juce::Time startUTC;
        if (! parseAudioStartFromName(fname, startUTC))
        {
            ++skippedNoTimestamp;
            if (log)
                log("skip (no ts): " + fname + "\n");
            continue;
        }

        if (tmin.hasValue() && timeLessThan(startUTC, *tmin))
        {
            ++skippedBefore;
            if (! haveLeft || timeLessThan(leftCandidate.startUTC, startUTC))
            {
                leftCandidate = { url, fname, folderName, startUTC, {} };
                haveLeft = true;
            }
            if (log)
                log("skip (before tmin): " + fname + " start=" + toIso(startUTC) + "\n");
            continue;
        }

        if (tmax.hasValue() && timeLessThan(*tmax, startUTC))
        {
            ++skippedAfter;
            if (log)
                log("skip (after tmax): " + fname + " start=" + toIso(startUTC) + "\n");
            continue;
        }

        rows.push_back({ url, fname, folderName, startUTC, {} });
        ++kept;
    }

    if (haveLeft)
    {
        auto diff = tmin.hasValue()
                    ? (tmin->toMilliseconds() - leftCandidate.startUTC.toMilliseconds())
                    : std::numeric_limits<juce::int64>::max();
        if (diff >= 0 && diff <= (juce::int64) (6 * 3600 * 1000))
        {
            if (log)
                log("include left-boundary: " + leftCandidate.fname + "\n");
            rows.push_back(leftCandidate);
            ++kept;
        }
    }

    buildHoursFromRows(rows);

    if (log)
    {
        log("audio stats parsed=" + juce::String(parsed)
            + " kept=" + juce::String(kept)
            + " before=" + juce::String(skippedBefore)
            + " after=" + juce::String(skippedAfter)
            + " no-ts=" + juce::String(skippedNoTimestamp)
            + " other=" + juce::String(skippedOther) + "\n");
        log("kept audio rows: " + juce::String((int) rows.size()) + "\n");
    }

    return rows;
}

std::vector<SanctSoundClient::HourRow> SanctSoundClient::listAudioFilesAcross(const juce::String& site,
                                                                              const juce::String& preferredFolder,
                                                                              juce::Optional<juce::Time> tmin,
                                                                              juce::Optional<juce::Time> tmax,
                                                                              const std::function<void(const juce::String&)>& log) const
{
    std::vector<HourRow> allRows;

    auto siteCode = site.trim().toLowerCase();
    auto deployments = listSiteDeployments(siteCode, log);

    std::vector<juce::String> ordered;
    if (preferredFolder.isNotEmpty())
        ordered.push_back(preferredFolder);
    for (auto& d : deployments)
    {
        if (preferredFolder.isNotEmpty() && preferredFolder.equalsIgnoreCase(d))
            continue;
        ordered.push_back(d);
    }
    if (ordered.empty())
        ordered = deployments;

    struct DebugInfo
    {
        juce::String folder;
        juce::String command;
        juce::StringArray lines;
    };

    std::vector<DebugInfo> debug;
    debug.reserve(ordered.size());

    for (auto& folderName : ordered)
    {
        juce::String cmd;
        juce::StringArray rawLines;
        auto rows = listAudioFilesInFolder(siteCode, folderName, tmin, tmax, log, &cmd, &rawLines);
        if (cmd.isNotEmpty() || ! rawLines.isEmpty())
            debug.push_back({ folderName, cmd, rawLines });
        allRows.insert(allRows.end(), rows.begin(), rows.end());
    }

    std::sort(allRows.begin(), allRows.end(), [](const HourRow& a, const HourRow& b)
    {
        if (timeLessThan(a.startUTC, b.startUTC))
            return true;
        if (timeLessThan(b.startUTC, a.startUTC))
            return false;
        return a.folder.compareIgnoreCase(b.folder) < 0;
    });

    if (log)
        log("files across (total): " + juce::String((int) allRows.size()) + "\n");

    if (allRows.empty() && log)
    {
        if (debug.empty())
        {
            log("files across: no gsutil commands captured\n");
        }
        else
        {
            for (size_t i = 0; i < debug.size(); ++i)
            {
                const auto& info = debug[i];
                juce::String header = "files across: command[" + juce::String((int) (i + 1)) + "] ";
                if (info.folder.isNotEmpty())
                    header += "[" + info.folder + "] ";
                header += info.command;
                log(header + "\n");

                if (info.lines.isEmpty())
                {
                    log("  (no output captured)\n");
                    continue;
                }

                int limit = juce::jmin(10, info.lines.size());
                for (int j = 0; j < limit; ++j)
                    log("  " + info.lines[j] + "\n");
                if (info.lines.size() > limit)
                    log("  ... (" + juce::String(info.lines.size() - limit) + " more lines)\n");
            }
        }
    }

    return allRows;
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
        if (! parseAudioStartFromName(name, start))
            continue;

        juce::String folder = folderFromSet(name);
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
    auto preferredFolder = folderFromSet(group.name);

    if (log)
    {
        juce::String msg = "preview: site=" + siteCode + " prefFolder="
                            + (preferredFolder.isNotEmpty() ? preferredFolder : juce::String("<none>"));
        log(msg + "\n");
    }

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
    juce::Optional<juce::Time> tmin;
    juce::Optional<juce::Time> tmax;

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
        std::vector<std::pair<juce::Time, juce::Time>> combinedEvents;
        std::set<std::pair<juce::int64, juce::int64>> seenEvents;
        for (auto& csv : localCsvs)
        {
            auto events = parseEventsFromCsv(csv);
            for (auto& evt : events)
            {
                auto key = std::make_pair(evt.first.toMilliseconds(), evt.second.toMilliseconds());
                if (seenEvents.insert(key).second)
                    combinedEvents.push_back(evt);
            }
        }

        std::sort(combinedEvents.begin(), combinedEvents.end(), [](const auto& a, const auto& b)
        {
            if (timeLessThan(a.first, b.first))
                return true;
            if (timeLessThan(b.first, a.first))
                return false;
            return timeLessThan(a.second, b.second);
        });

        windows = combinedEvents;

        if (! combinedEvents.empty())
        {
            tmin = combinedEvents.front().first;
            tmax = combinedEvents.back().second;
        }

        runsText << "Events: " << combinedEvents.size() << "\n";
    }

    if (log)
        log("CSV windows parsed: " + juce::String((int) windows.size()) + "\n");

    if (log)
    {
        juce::String tminText = tmin.hasValue() ? toIso(*tmin) : juce::String("<none>");
        juce::String tmaxText = tmax.hasValue() ? toIso(*tmax) : juce::String("<none>");
        log("window summary count=" + juce::String((int) windows.size())
            + " tmin=" + tminText + " tmax=" + tmaxText + "\n");
    }

    std::vector<HourRow> audioRows;
    if (! windows.empty())
        audioRows = listAudioFilesAcross(siteCode, preferredFolder, tmin, tmax, log);
    if (log)
        log("Audio rows collected: " + juce::String((int) audioRows.size()));

    juce::StringArray urls;
    juce::StringArray names;
    minimalUnionForWindows(audioRows, windows, urls, names);

    if (log)
        log("Minimal union selected files=" + juce::String(names.size())
            + " urls=" + juce::String(urls.size()));

    result.urls = urls;
    result.names = names;
    result.matchedObjects = urls;

    juce::Array<PreviewWindow> previewWindows;
    for (auto& w : windows)
        previewWindows.add({ w.first, w.second });
    result.windows = previewWindows;
    result.matchedWindows = (int) windows.size();
    result.unmatchedWindows = 0;
    result.runsText = runsText;

    juce::String modeLower = mode.toLowerCase();
    result.summary = group.name + " | mode: " + modeLower + " | unique files: " + juce::String(names.size());

    juce::Array<ListedFile> listed;
    std::map<juce::String, const HourRow*, std::less<>> byName;
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
        lf.start = row->startUTC;
        lf.end = row->endUTC;
        lf.folder = row->folder;
        listed.add(lf);
    }
    result.files = listed;

    auto dest = getDestinationDirectory();
    writeLines(dest.getChildFile(group.name + "_preview_needed_files.txt"), urls);
    writeLines(dest.getChildFile(group.name + "_preview_needed_fnames.txt"), names);

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
        audio.folder = folderFromSet(audio.name);
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
