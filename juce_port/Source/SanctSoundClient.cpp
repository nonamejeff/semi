#include "SanctSoundClient.h"

#include "Utilities.h"

#include <juce_core/juce_core.h>
#include <juce_data_structures/juce_data_structures.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <string>
#include <stdexcept>
#include <vector>

namespace sanctsound
{
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

juce::Optional<juce::Time> parseAudioStartFromName(const juce::String& name)
{
    auto trimmed = name.trim();
    auto underscore = trimmed.lastIndexOfChar('_');
    auto dot = trimmed.lastIndexOfChar('.');
    if (underscore < 0 || dot < 0 || dot <= underscore)
        return {};

    auto token = trimmed.substring(underscore + 1, dot);
    juce::Time parsed;
    if (parseTimestamp(token, parsed))
        return parsed;

    if (token.length() == 12 && token.containsOnly("0123456789"))
    {
        int yy = token.substring(0, 2).getIntValue();
        int year = yy <= 69 ? 2000 + yy : 1900 + yy;
        int month = token.substring(2, 4).getIntValue();
        int day = token.substring(4, 6).getIntValue();
        int hour = token.substring(6, 8).getIntValue();
        int minute = token.substring(8, 10).getIntValue();
        int second = token.substring(10, 12).getIntValue();
        return juce::Time(year, month, day, hour, minute, second, 0, false);
    }

    return {};
}

juce::String folderFromSet(const juce::String& setName)
{
    auto lower = setName.toLowerCase();
    auto pos = lower.indexOf("sanctsound_");
    if (pos < 0)
        return {};
    for (int i = pos; i + 18 <= lower.length(); ++i)
    {
        auto candidate = lower.substring(i, i + 18);
        if (candidate.startsWith("sanctsound_") &&
            juce::CharacterFunctions::isLetter(candidate[11]) &&
            juce::CharacterFunctions::isLetter(candidate[12]) &&
            juce::CharacterFunctions::isDigit(candidate[13]) &&
            juce::CharacterFunctions::isDigit(candidate[14]) &&
            candidate[15] == '_' &&
            juce::CharacterFunctions::isDigit(candidate[16]) &&
            juce::CharacterFunctions::isDigit(candidate[17]))
            return candidate;
    }
    return {};
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

int detectDatetimeColumn(const CsvTable& table, double minFraction)
{
    if (table.rows.isEmpty())
        return -1;
    auto rowCount = table.rows.size();
    int bestCol = -1;
    int bestMatches = 0;
    for (int col = 0; col < table.header.size(); ++col)
    {
        int matches = 0;
        for (auto& row : table.rows)
        {
            if (col >= row.size())
                continue;
            juce::Time parsed;
            if (parseTimestamp(row[col], parsed))
                ++matches;
        }
        if (matches > bestMatches && matches >= static_cast<int>(std::ceil(rowCount * minFraction)))
        {
            bestMatches = matches;
            bestCol = col;
        }
    }
    return bestCol;
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

juce::Array<juce::Time> parsePresenceHoursFromCsv(const juce::File& file)
{
    auto table = readCsvFile(file);
    auto hourCol = detectDatetimeColumn(table, 0.1);
    auto presenceCol = detectBinaryColumn(table, hourCol);
    if (hourCol < 0 || presenceCol < 0)
        throw std::runtime_error("Could not detect hour/presence columns in " + file.getFileName().toStdString());

    juce::Array<juce::Time> hours;
    for (auto& row : table.rows)
    {
        if (hourCol >= row.size() || presenceCol >= row.size())
            continue;
        auto flag = row[presenceCol].trim();
        if (! isNumeric(flag))
            continue;
        auto val = row[presenceCol].getDoubleValue();
        if (std::round(val) != 1)
            continue;
        juce::Time parsed;
        if (! parseTimestamp(row[hourCol], parsed))
            continue;
        auto truncated = juce::Time(parsed.getYear(), parsed.getMonth(), parsed.getDayOfMonth(), parsed.getHours(), 0, 0, 0, false);
        hours.addIfNotAlreadyThere(truncated);
    }
    std::sort(hours.begin(), hours.end(), [](const juce::Time& a, const juce::Time& b) { return timeLessThan(a, b); });
    return hours;
}

juce::Array<juce::Time> parsePresenceDaysFromCsv(const juce::File& file)
{
    auto table = readCsvFile(file);
    auto dtCol = detectDatetimeColumn(table, 0.05);
    auto presenceCol = detectBinaryColumn(table, dtCol);
    if (dtCol < 0 || presenceCol < 0)
        throw std::runtime_error("Could not detect date/presence columns in " + file.getFileName().toStdString());

    juce::Array<juce::Time> days;
    for (auto& row : table.rows)
    {
        if (dtCol >= row.size() || presenceCol >= row.size())
            continue;
        auto flag = row[presenceCol].trim();
        if (! isNumeric(flag))
            continue;
        auto val = row[presenceCol].getDoubleValue();
        if (std::round(val) != 1)
            continue;
        juce::Time parsed;
        if (! parseTimestamp(row[dtCol], parsed))
            continue;
        auto truncated = juce::Time(parsed.getYear(), parsed.getMonth(), parsed.getDayOfMonth(), 0, 0, 0, 0, false);
        days.addIfNotAlreadyThere(truncated);
    }
    std::sort(days.begin(), days.end(), [](const juce::Time& a, const juce::Time& b) { return timeLessThan(a, b); });
    return days;
}

juce::Array<PreviewWindow> parseEventsFromCsv(const juce::File& file)
{
    auto table = readCsvFile(file);
    auto dtCol = detectDatetimeColumn(table, 0.05);
    if (dtCol < 0)
        throw std::runtime_error("No usable datetime column in " + file.getFileName().toStdString());

    int endCol = -1;
    for (int i = 0; i < table.header.size(); ++i)
    {
        if (i == dtCol)
            continue;
        auto header = table.header[i].toLowerCase();
        if (header.contains("end"))
        {
            endCol = i;
            break;
        }
    }

    int durationCol = -1;
    for (int i = 0; i < table.header.size(); ++i)
    {
        auto header = table.header[i].toLowerCase();
        if (header.contains("duration") || header.contains("dur") || header.contains("length"))
        {
            durationCol = i;
            break;
        }
    }

    juce::Array<PreviewWindow> events;
    const double fallbackSeconds = 60.0;

    for (int rowIdx = 0; rowIdx < table.rows.size(); ++rowIdx)
    {
        auto& row = table.rows.getReference(rowIdx);
        if (dtCol >= row.size())
            continue;
        juce::Time start;
        if (! parseTimestamp(row[dtCol], start))
            continue;

        juce::Time end;
        if (endCol >= 0 && endCol < row.size() && parseTimestamp(row[endCol], end))
        {
            if (! timeLessThan(start, end))
                end = start + juce::RelativeTime::seconds(fallbackSeconds);
        }
        else
        {
            double duration = fallbackSeconds;
            if (durationCol >= 0 && durationCol < row.size() && isNumeric(row[durationCol]))
            {
                auto v = row[durationCol].getDoubleValue();
                if (v > 0)
                    duration = v;
            }
            end = start + juce::RelativeTime::seconds(duration);
        }
        events.add({ start, end });
    }

    std::sort(events.begin(), events.end(), [](const PreviewWindow& a, const PreviewWindow& b)
    {
        return timeLessThan(a.start, b.start);
    });

    return events;
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

juce::Array<AudioReference> listAudioFilesInFolder(const juce::String& site,
                                                   const juce::String& folder,
                                                   const juce::Time& tmin,
                                                   const juce::Time& tmax,
                                                   const juce::String& audioPrefix,
                                                   const juce::String& bucket)
{
    juce::Array<AudioReference> files;
    auto prefix = audioPrefix + "/" + site + "/" + folder + "/audio/";

    juce::Optional<AudioReference> leftCandidate;

    juce::String pageToken;
    do
    {
        juce::var response = pageToken.isEmpty()
                                 ? fetchGcsJsonFirstPage(bucket, prefix)
                                 : fetchGcsJsonPage(bucket, prefix, "/", pageToken);

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
                if (! objectName.endsWithIgnoreCase(".flac"))
                    continue;

                auto url = makeGsUrl(bucket, objectName);
                auto name = url.fromLastOccurrenceOf("/", false, false);

                auto startOpt = parseAudioStartFromName(name);
                if (! startOpt)
                    continue;
                auto start = *startOpt;
                if (tmin.toMilliseconds() != 0 && timeLessThan(start, tmin))
                {
                    if (! leftCandidate.hasValue() || timeLessThan(leftCandidate->start, start))
                        leftCandidate = AudioReference{ url, name, start, {}, folder };
                    continue;
                }
                if (tmax.toMilliseconds() != 0 && timeLessThan(tmax, start))
                    continue;
                files.add({ url, name, start, {}, folder });
            }
        }

        auto nextVar = obj->getProperty("nextPageToken");
        pageToken = nextVar.isString() ? nextVar.toString() : juce::String();
    } while (pageToken.isNotEmpty());

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

juce::Array<AudioReference> listAudioFilesAcross(const juce::String& site,
                                                const juce::String& preferredFolder,
                                                const juce::Time& tmin,
                                                const juce::Time& tmax,
                                                const juce::String& audioPrefix,
                                                const juce::String& bucket)
{
    auto basePrefix = audioPrefix + "/" + site + "/";
    juce::StringArray folders;

    juce::String pageToken;
    do
    {
        juce::var response = pageToken.isEmpty()
                                 ? fetchGcsJsonFirstPage(bucket, basePrefix)
                                 : fetchGcsJsonPage(bucket, basePrefix, "/", pageToken);

        auto* obj = response.getDynamicObject();
        if (obj == nullptr)
            break;

        auto prefixesVar = obj->getProperty("prefixes");
        if (auto* prefixArray = prefixesVar.getArray())
        {
            for (auto& entry : *prefixArray)
            {
                if (! entry.isString())
                    continue;
                auto prefix = entry.toString();
                auto trimmed = prefix.trimCharactersAtEnd("/");
                auto name = trimmed.fromLastOccurrenceOf("/", false, false);
                if (name.startsWithIgnoreCase("sanctsound_"))
                    folders.addIfNotAlreadyThere(name);
            }
        }

        auto nextVar = obj->getProperty("nextPageToken");
        pageToken = nextVar.isString() ? nextVar.toString() : juce::String();
    } while (pageToken.isNotEmpty());

    juce::StringArray ordered;
    if (preferredFolder.isNotEmpty())
        ordered.add(preferredFolder);
    for (auto& f : folders)
        if (! ordered.contains(f))
            ordered.add(f);

    juce::Array<AudioReference> all;
    for (auto& folder : ordered)
    {
        auto files = listAudioFilesInFolder(site, folder, tmin, tmax, audioPrefix, bucket);
        all.addArray(files);
    }

    std::sort(all.begin(), all.end(), [](const AudioReference& a, const AudioReference& b)
    {
        if (timeLessThan(a.start, b.start)) return true;
        if (timeLessThan(b.start, a.start)) return false;
        return a.folder < b.folder;
    });
    return all;
}

void minimalUnionForWindows(const juce::Array<AudioReference>& files,
                            const juce::Array<PreviewWindow>& windows,
                            juce::StringArray& urls,
                            juce::StringArray& names)
{
    if (files.isEmpty() || windows.isEmpty())
        return;

    auto pickWindow = [&](const PreviewWindow& w)
    {
        juce::Array<int> chosen;
        int index = -1;
        for (int i = 0; i < files.size(); ++i)
        {
            if (timeLessThanOrEqual(files[i].start, w.start))
                index = i;
            else
                break;
        }
        if (index < 0)
            return chosen;
        chosen.add(index);
        if (timeLessThan(files[index].end, w.end) && index + 1 < files.size())
            chosen.add(index + 1);
        return chosen;
    };

    for (auto& w : windows)
    {
        auto chosen = pickWindow(w);
        for (auto idx : chosen)
        {
            urls.addIfNotAlreadyThere(files[idx].url);
            names.addIfNotAlreadyThere(files[idx].name);
        }
    }
}

struct DownloadedFile
{
    juce::File localFile;
    juce::String url;
};

std::vector<DownloadedFile> downloadFilesTo(const juce::StringArray& urls,
                                            const juce::File& dest,
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

        if (parseGsUrl(url, bucket, objectName))
        {
            httpUrl = makeHttpsUrl(bucket, objectName);
        }
        else
        {
            httpUrl = url;
        }

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

        juce::String base;
        if (objectName.isNotEmpty())
            base = objectName.fromLastOccurrenceOf("/", false, false);
        if (base.isEmpty())
            base = url.fromLastOccurrenceOf("/", false, false);

        auto local = dest.getChildFile(base);
        ensureParentDir(local);
        juce::FileOutputStream outStream(local);
        if (! outStream.openedOk())
            throw std::runtime_error("Failed to open file for writing: " + local.getFullPathName().toStdString());

        if (outStream.writeFromInputStream(*stream, -1) < 0)
            throw std::runtime_error("Failed to write file: " + local.getFullPathName().toStdString());

        outStream.flush();
        if (outStream.getStatus().failed())
            throw std::runtime_error("Write failed: " + outStream.getStatus().getErrorMessage().toStdString());

        out.push_back({ local, url });
    }
    return out;
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

juce::String stampForFilename(const juce::Time& t)
{
    return t.formatted("%Y%m%dT%H%M%S");
}

} // namespace

SanctSoundClient::SanctSoundClient()
{
    auto defaultDir = juce::File::getSpecialLocation(juce::File::userDocumentsDirectory)
                          .getChildFile("SanctSound");
    if (! setDestinationDirectory(defaultDir))
        throw std::runtime_error("Failed to initialise destination directory: "
                                 + defaultDir.getFullPathName().toStdString());

    gcsBucket = "noaa-passive-bioacoustic";
    audioPrefix = "sanctsound/audio";
    productsPrefix = "sanctsound/products/detections";
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

const juce::File& SanctSoundClient::getDestinationDirectory() const
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
    const juce::String sitePrefix = productsPrefix + "/" + site + "/";
    const juce::String tagLower = tag.trim().toLowerCase();

    juce::Array<juce::String> pending;
    juce::StringArray visited;
    pending.add(sitePrefix);
    visited.add(sitePrefix);

    std::map<juce::String, ProductGroup, std::less<>> groups;
    bool foundAny = false;

    while (pending.size() > 0)
    {
        auto current = pending[0];
        pending.remove(0);

        if (log)
            log("[gcs] list " + makeGsUrl(gcsBucket, current));

        juce::String pageToken;
        do
        {
            juce::var response = pageToken.isEmpty()
                                     ? fetchGcsJsonFirstPage(gcsBucket, current)
                                     : fetchGcsJsonPage(gcsBucket, current, "/", pageToken);

            auto* obj = response.getDynamicObject();
            if (obj == nullptr)
                break;

            auto prefixesVar = obj->getProperty("prefixes");
            if (auto* prefixArray = prefixesVar.getArray())
            {
                for (auto& entry : *prefixArray)
                {
                    if (! entry.isString())
                        continue;
                    auto subPrefix = entry.toString();
                    if (! visited.contains(subPrefix))
                    {
                        visited.add(subPrefix);
                        pending.add(subPrefix);
                    }
                }
            }

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
            }

            auto nextVar = obj->getProperty("nextPageToken");
            pageToken = nextVar.isString() ? nextVar.toString() : juce::String();
        } while (pageToken.isNotEmpty());
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

    auto mode = juce::String("EVENT");
    auto lower = group.name.toLowerCase();
    if (lower.endsWith("_1h"))
        mode = "HOUR";
    else if (lower.endsWith("_1d"))
        mode = "DAY";
    result.mode = mode;

    auto preferredFolder = folderFromSet(group.name);

    auto bestFiles = chooseBestFiles(group.paths);
    auto downloaded = downloadFilesTo(bestFiles, destinationDir, log);

    juce::Array<juce::File> localCsvs;
    for (auto& item : downloaded)
        if (item.localFile.hasFileExtension("csv"))
            localCsvs.add(item.localFile);

    if (localCsvs.isEmpty())
        throw std::runtime_error("Preview expects at least one CSV artifact");

    juce::Array<PreviewWindow> windows;
    juce::String runsText;
    juce::String summaryText;
    juce::Time tmin;
    juce::Time tmax;

    if (mode == "HOUR")
    {
        juce::Array<juce::Time> hours;
        for (auto& csv : localCsvs)
            hours.addArray(parsePresenceHoursFromCsv(csv));
        removeDuplicateTimesInPlace(hours);
        std::sort(hours.begin(), hours.end(), [](auto& a, auto& b) { return timeLessThan(a, b); });

        auto runs = groupConsecutive(hours, juce::RelativeTime::hours(1));
        if (onlyLongRuns)
        {
            juce::Array<PreviewWindow> filtered;
            for (auto& r : runs)
                if ((r.end - r.start).inHours() >= 2.0)
                    filtered.add(r);
            runs = filtered;
            hours = expandRuns(runs, juce::RelativeTime::hours(1));
        }

        windows = juce::Array<PreviewWindow>();
        for (auto& h : hours)
            windows.add({ h, h + juce::RelativeTime::hours(1) });

        if (! hours.isEmpty())
        {
            tmin = hours.getFirst();
            tmax = hours.getLast() + juce::RelativeTime::hours(1);
        }

        runsText << "Runs (" << runs.size() << "):\n";
        int idx = 1;
        for (auto& r : runs)
        {
            runsText << juce::String(idx++).paddedLeft('0', 2) << ". "
                     << toIso(r.start) << " -> " << toIso(r.end) << "\n";
        }
        summaryText = group.name + " | mode: hour";
    }
    else if (mode == "DAY")
    {
        juce::Array<juce::Time> days;
        for (auto& csv : localCsvs)
            days.addArray(parsePresenceDaysFromCsv(csv));
        removeDuplicateTimesInPlace(days);
        std::sort(days.begin(), days.end(), [](auto& a, auto& b) { return timeLessThan(a, b); });

        windows = juce::Array<PreviewWindow>();
        for (auto& d : days)
            windows.add({ d, d + juce::RelativeTime::days(1) });
        if (! days.isEmpty())
        {
            tmin = days.getFirst();
            tmax = days.getLast() + juce::RelativeTime::days(1);
        }
        runsText << "Days: " << days.size() << "\n";
        summaryText = group.name + " | mode: day";
    }
    else
    {
        windows = juce::Array<PreviewWindow>();
        for (auto& csv : localCsvs)
            windows.addArray(parseEventsFromCsv(csv));
        std::sort(windows.begin(), windows.end(), [](auto& a, auto& b) { return timeLessThan(a.start, b.start); });
        if (! windows.isEmpty())
        {
            tmin = windows.getFirst().start;
            tmax = windows.getLast().end;
        }
        runsText << "Events: " << windows.size() << "\n";
        summaryText = group.name + " | mode: event";
    }

    auto audioFiles = listAudioFilesAcross(site, preferredFolder, tmin, tmax, audioPrefix, gcsBucket);
    juce::StringArray urls;
    juce::StringArray names;
    minimalUnionForWindows(audioFiles, windows, urls, names);

    result.summary = summaryText + " | unique files: " + juce::String(names.size());
    result.runsText = runsText;
    result.windows = windows;
    result.urls = urls;
    result.names = names;

    for (auto& file : audioFiles)
    {
        if (urls.contains(file.url))
        {
            ListedFile lf;
            lf.url = file.url;
            lf.name = file.name;
            lf.start = file.start;
            lf.end = file.end;
            lf.folder = file.folder;
            result.files.add(lf);
        }
    }

    return result;
}

void SanctSoundClient::downloadFiles(const juce::StringArray& urls, LogFn log) const
{
    downloadFilesTo(urls, destinationDir, log);
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
    for (const auto& entry : juce::RangedDirectoryIterator(destinationDir, false, "*.flac", juce::File::findFiles))
    {
        auto file = entry.getFile();
        if (! selectedBasenames.contains(file.getFileName()))
            continue;
        auto startOpt = parseAudioStartFromName(file.getFileName());
        if (! startOpt)
            continue;
        LocalAudio audio;
        audio.file = file;
        audio.name = file.getFileName();
        audio.start = *startOpt;
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
            double seconds = 3600.0;
            auto err = ffprobeDuration(local[i].file, seconds);
            if (! err.isEmpty())
                log("[WARN] " + err);
            if (seconds <= 1.0)
                seconds = 3600.0;
            local.getReference(i).end = local[i].start + juce::RelativeTime::seconds(seconds);
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

    for (auto& grp : groups)
    {
        auto itCache = cache.find(grp);
        if (itCache == cache.end())
        {
            log("[WARN] No preview cache for " + grp);
            continue;
        }

        const auto& preview = itCache->second;
        summary.mode = preview.mode;

        auto clipsDir = destinationDir.getChildFile("clips").getChildFile(grp);
        if (clipsDir.exists())
        {
            if (! clipsDir.isDirectory())
                throw std::runtime_error("Destination exists but is not a directory: "
                                         + clipsDir.getFullPathName().toStdString());
        }
        else if (! clipsDir.createDirectory())
        {
            throw std::runtime_error("Failed to create directory: " + clipsDir.getFullPathName().toStdString());
        }
        summary.directory = clipsDir;

        juce::Array<ClipRow> manifest;
        int written = 0;
        int skipped = 0;

        for (auto& window : preview.windows)
        {
            summary.totalWindows++;
            auto [current, next] = coverAndNext(window.start);
            if (current == nullptr)
            {
                skipped++;
                continue;
            }

            bool needTwo = timeLessThan(current->end, window.end);
            if (! needTwo)
            {
                if (! selectedBasenames.contains(current->name))
                {
                    skipped++;
                    continue;
                }
            }
            else
            {
                if (next == nullptr || ! selectedBasenames.contains(current->name) || ! selectedBasenames.contains(next->name))
                {
                    skipped++;
                    continue;
                }
            }

            auto clipName = current->name.upToLastOccurrenceOf(".", false, false)
                              + "__" + stampForFilename(window.start)
                              + "_" + stampForFilename(window.end) + ".wav";
            auto outFile = clipsDir.getChildFile(clipName);

            auto durationSeconds = (window.end - window.start).inSeconds();
            if (durationSeconds <= 0)
            {
                skipped++;
                continue;
            }

            juce::String err;
            if (! needTwo)
            {
                auto startOffset = (window.start - current->start).inSeconds();
                err = ffmpegCut(current->file, startOffset, durationSeconds, outFile, clipSampleRate, clipMono, clipSampleFormat);
            }
            else
            {
                auto startOffset = (window.start - current->start).inSeconds();
                auto partASeconds = (current->end - window.start).inSeconds();
                auto partBSeconds = (window.end - next->start).inSeconds();
                if (partASeconds <= 0 || partBSeconds <= 0)
                {
                    skipped++;
                    continue;
                }
                auto tempA = juce::File::createTempFile("clipA.wav");
                auto tempB = juce::File::createTempFile("clipB.wav");
                auto errA = ffmpegCut(current->file, startOffset, partASeconds, tempA, clipSampleRate, clipMono, clipSampleFormat);
                auto errB = ffmpegCut(next->file, 0.0, partBSeconds, tempB, clipSampleRate, clipMono, clipSampleFormat);
                if (errA.isNotEmpty() || errB.isNotEmpty())
                {
                    if (errA.isNotEmpty()) log("[WARN] " + errA);
                    if (errB.isNotEmpty()) log("[WARN] " + errB);
                    tempA.deleteFile();
                    tempB.deleteFile();
                    skipped++;
                    continue;
                }
                err = ffmpegConcat(tempA, tempB, outFile);
                tempA.deleteFile();
                tempB.deleteFile();
            }

            if (err.isNotEmpty())
            {
                log("[WARN] " + err);
                outFile.deleteFile();
                skipped++;
                continue;
            }

            if (outFile.getSize() < 10000)
            {
                outFile.deleteFile();
                skipped++;
                continue;
            }

            ClipRow row;
            row.clipName = outFile.getFileName();
            row.sourceNames = needTwo ? current->name + " + " + next->name : current->name;
            row.startIso = window.start.toISO8601(true);
            row.endIso = window.end.toISO8601(true);
            row.durationSeconds = durationSeconds;
            row.mode = preview.mode;
            manifest.add(row);
            written++;
        }

        summary.written += written;
        summary.skipped += skipped;
        summary.manifestRows.addArray(manifest);

        auto manifestFile = clipsDir.getChildFile("clips_manifest.csv");
        auto writeCsvLine = [](juce::FileOutputStream& stream, const juce::StringArray& fields)
        {
            juce::StringArray escaped;
            for (auto& f : fields)
                escaped.add("\"" + f.replace("\"", "\"\"") + "\"");
            stream.writeText(escaped.joinIntoString(",") + "\n", false, false, "\n");
        };

        ensureParentDir(manifestFile);
        juce::FileOutputStream out(manifestFile);
        if (! out.openedOk())
            throw std::runtime_error("Failed to open file for writing: " + manifestFile.getFullPathName().toStdString());

        writeCsvLine(out, { "clip_wav", "source_flac(s)", "start_utc", "end_utc", "duration_sec", "mode" });
        for (auto& row : manifest)
        {
            writeCsvLine(out, {
                row.clipName,
                row.sourceNames,
                row.startIso,
                row.endIso,
                juce::String(row.durationSeconds, 3),
                row.mode
            });
        }

        out.flush();
        if (out.getStatus().failed())
            throw std::runtime_error("Write failed: " + out.getStatus().getErrorMessage().toStdString());

        auto summaryFile = clipsDir.getChildFile("clips_summary.txt");
        juce::String summaryText;
        summaryText << "Windows: " << preview.windows.size()
                    << " | Clips: " << manifest.size()
                    << " | Skipped: " << skipped
                    << " | Mode: " << preview.mode << "\n"
                    << "Dir: " << clipsDir.getFullPathName() << "\n";
        writeTextFile(summaryFile, summaryText);

        log("Clips -> " + clipsDir.getFullPathName() + " | written " + juce::String(written) + ", skipped " + juce::String(skipped));
    }

    return summary;
}

} // namespace sanctsound
