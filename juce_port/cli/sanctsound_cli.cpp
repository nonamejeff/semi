#include <juce_core/juce_core.h>

#include "PreviewModels.h"
#include "SanctSoundClient.h"

#include <ctime>
#include <cstdio>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>

using namespace sanctsound;

namespace
{

struct ParsedArguments
{
    juce::String subcommand;
    std::map<juce::String, juce::String, std::less<>> options;
};

ParsedArguments parseCommandLine (int argc, char** argv)
{
    ParsedArguments parsed;
    if (argc >= 2)
        parsed.subcommand = juce::String (argv[1]).toLowerCase();

    for (int i = 2; i < argc; ++i)
    {
        juce::String token (argv[i]);
        if (! token.startsWith("--"))
            continue;

        juce::String key;
        juce::String value;
        const int eqIndex = token.indexOfChar('=');
        if (eqIndex >= 0)
        {
            key = token.substring(2, eqIndex);
            value = token.substring(eqIndex + 1);
        }
        else
        {
            key = token.substring(2);
            if (i + 1 < argc)
            {
                juce::String candidate (argv[i + 1]);
                if (! candidate.startsWith("--"))
                {
                    value = candidate;
                    ++i;
                }
            }
            if (value.isEmpty())
                value = "true";
        }

        parsed.options[key.toLowerCase()] = value;
    }

    return parsed;
}

bool ensureDirectory (const juce::File& dir, juce::String& error)
{
    if (dir.exists())
    {
        if (! dir.isDirectory())
        {
            error = "Path exists but is not a directory: " + dir.getFullPathName();
            return false;
        }
        return true;
    }

    if (! dir.createDirectory())
    {
        error = "Failed to create directory: " + dir.getFullPathName();
        return false;
    }
    return true;
}

bool parseBool (const juce::String& raw, bool& out)
{
    auto value = raw.trim().toLowerCase();
    if (value.isEmpty() || value == "1" || value == "true" || value == "yes" || value == "on")
    {
        out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off")
    {
        out = false;
        return true;
    }
    return false;
}

bool parseIsoUtc (const juce::String& text, juce::Time& out)
{
    auto trimmed = text.trim();
    if (trimmed.isEmpty())
        return false;

    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    if (std::sscanf(trimmed.toRawUTF8(), "%4d-%2d-%2dT%2d:%2d:%2dZ", &year, &month, &day, &hour, &minute, &second) != 6)
        return false;

    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;

#if JUCE_WINDOWS
    auto tt = _mkgmtime(&tm);
#else
    auto tt = timegm(&tm);
#endif

    if (tt == -1)
        return false;

    out = juce::Time(static_cast<juce::int64>(tt) * 1000);
    return true;
}

bool writeLinesToFile (const juce::File& file, const juce::StringArray& lines, juce::String& error)
{
    auto parent = file.getParentDirectory();
    if (parent != juce::File() && ! parent.exists())
    {
        if (! parent.createDirectory())
        {
            error = "Failed to create directory: " + parent.getFullPathName();
            return false;
        }
    }

    juce::FileOutputStream outStream(file);
    if (! outStream.openedOk())
    {
        error = "Failed to open file for writing: " + file.getFullPathName();
        return false;
    }

    for (auto& line : lines)
        outStream.writeText(line + "\n", false, false, "\n");
    outStream.flush();
    if (outStream.getStatus().failed())
    {
        error = "Write failed: " + outStream.getStatus().getErrorMessage();
        return false;
    }

    return true;
}

SanctSoundClient::PreviewDebugSinks makePreviewDebugSinks (const juce::File& dir)
{
    SanctSoundClient::PreviewDebugSinks sinks;
    sinks.folderListings = dir.getChildFile("debug_all_listing.txt");
    sinks.folderListingCommands = dir.getChildFile("debug_all_listing_cmd.txt");
    sinks.candidateUrls = dir.getChildFile("debug_candidates_urls.txt");
    sinks.candidateNames = dir.getChildFile("debug_candidates_fnames.txt");
    sinks.selectedUrls = dir.getChildFile("debug_selected_urls.txt");
    sinks.selectedNames = dir.getChildFile("debug_selected_fnames.txt");
    sinks.explainLog = dir.getChildFile("debug_explain.txt");
    sinks.windowsTsv = dir.getChildFile("debug_windows.tsv");
    return sinks;
}

juce::StringArray readExpectedList (const juce::File& file)
{
    juce::StringArray rows;
    if (! file.existsAsFile())
        throw std::runtime_error("Expected file not found: " + file.getFullPathName().toStdString());

    auto text = file.loadFileAsString();
    rows.addLines(text);
    for (auto& row : rows)
        row = row.trim();
    rows.removeEmptyStrings(true);
    return rows;
}

std::ostream& logStream()
{
    return std::cout;
}

auto makeLogFn()
{
    return [] (const juce::String& msg)
    {
        auto& stream = logStream();
        stream << msg.toStdString();
        if (! msg.endsWithChar('\n'))
            stream << '\n';
    };
}

juce::String labelForMode(const juce::String& mode)
{
    auto upper = mode.toUpperCase();
    if (upper == "HOUR")
        return "Hours";
    if (upper == "DAY")
        return "Days";
    return "Events";
}

ProductGroup findGroupForSet (SanctSoundClient& client,
                              const juce::String& site,
                              const juce::String& setName,
                              SanctSoundClient::LogFn log)
{
    auto groups = client.listProductGroups(site, setName, log);
    for (const auto& group : groups)
        if (group.name.equalsIgnoreCase(setName))
            return group;

    if (! groups.empty())
        return groups.front();

    throw std::runtime_error("No product group matched set: " + setName.toStdString());
}

int runPreviewCommand (const ParsedArguments& args)
{
    auto findOpt = [&args] (const juce::String& name) -> juce::String
    {
        auto it = args.options.find(name.toLowerCase());
        return it != args.options.end() ? it->second : juce::String();
    };

    const juce::String site = findOpt("site").trim();
    const juce::String setName = findOpt("set").trim();
    const juce::String destPath = findOpt("dest").trim();
    const juce::String dumpPath = findOpt("dump-debug").trim();
    juce::String dryValue = findOpt("dry-run");

    if (site.isEmpty() || setName.isEmpty() || destPath.isEmpty() || dumpPath.isEmpty())
        throw std::runtime_error("preview requires --site, --set, --dest, and --dump-debug");

    bool dryRun = true;
    if (dryValue.isNotEmpty() && ! parseBool(dryValue, dryRun))
        throw std::runtime_error("Invalid value for --dry-run: " + dryValue.toStdString());

    juce::File destDir(destPath);
    juce::File dumpDir(dumpPath);
    juce::String error;
    if (! ensureDirectory(destDir, error))
        throw std::runtime_error(error.toStdString());
    if (! ensureDirectory(dumpDir, error))
        throw std::runtime_error(error.toStdString());

    SanctSoundClient client;
    if (! client.setDestinationDirectory(destDir))
        throw std::runtime_error("Failed to set destination directory to " + destDir.getFullPathName().toStdString());

    client.setPreviewDebugDirectory(dumpDir);
    client.setPreviewDebugSinks(makePreviewDebugSinks(dumpDir));

    auto log = makeLogFn();
    auto group = findGroupForSet(client, site, setName, log);
    auto preview = client.previewGroup(site, group, false, log);

    auto label = labelForMode(preview.mode);
    std::cout << label << ": " << preview.windows.size()
              << " | unique files: " << preview.names.size() << std::endl;

    if (! dryRun)
        client.downloadFiles(preview.urls, log);

    return 0;
}

int runListAudioCommand (const ParsedArguments& args)
{
    auto findOpt = [&args] (const juce::String& name) -> juce::String
    {
        auto it = args.options.find(name.toLowerCase());
        return it != args.options.end() ? it->second : juce::String();
    };

    const juce::String site = findOpt("site").trim();
    const juce::String folder = findOpt("folder").trim();
    const juce::String tminRaw = findOpt("tmin").trim();
    const juce::String tmaxRaw = findOpt("tmax").trim();
    const juce::String dumpPath = findOpt("dump").trim();

    if (site.isEmpty() || folder.isEmpty() || tminRaw.isEmpty() || tmaxRaw.isEmpty() || dumpPath.isEmpty())
        throw std::runtime_error("list-audio requires --site, --folder, --tmin, --tmax, and --dump");

    juce::Time tminUtc, tmaxUtc;
    if (! parseIsoUtc(tminRaw, tminUtc))
        throw std::runtime_error("Invalid --tmin (expected YYYY-MM-DDTHH:MM:SSZ): " + tminRaw.toStdString());
    if (! parseIsoUtc(tmaxRaw, tmaxUtc))
        throw std::runtime_error("Invalid --tmax (expected YYYY-MM-DDTHH:MM:SSZ): " + tmaxRaw.toStdString());

    juce::File dumpFile(dumpPath);
    juce::String error;
    auto parent = dumpFile.getParentDirectory();
    if (parent != juce::File() && ! ensureDirectory(parent, error))
        throw std::runtime_error(error.toStdString());

    SanctSoundClient client;
    auto log = makeLogFn();
    juce::Array<SanctSoundClient::AudioHour> rows =
        client.listAudioForFolder(site, folder, std::optional<juce::Time>(tminUtc), std::optional<juce::Time>(tmaxUtc), log);

    juce::StringArray urls;
    for (auto const& row : rows)
        urls.add(row.url);

    if (! writeLinesToFile(dumpFile, urls, error))
        throw std::runtime_error(error.toStdString());

    std::cout << "Listed URLs: " << urls.size() << std::endl;
    return 0;
}

int runVerifyCommand (const ParsedArguments& args)
{
    auto findOpt = [&args] (const juce::String& name) -> juce::String
    {
        auto it = args.options.find(name.toLowerCase());
        return it != args.options.end() ? it->second : juce::String();
    };

    const juce::String site = findOpt("site").trim();
    const juce::String setName = findOpt("set").trim();
    const juce::String expectPath = findOpt("expect").trim();
    const juce::String dumpPath = findOpt("dump-debug").trim();
    juce::String destPath = findOpt("dest").trim();

    if (site.isEmpty() || setName.isEmpty() || expectPath.isEmpty() || dumpPath.isEmpty())
        throw std::runtime_error("verify-expected requires --site, --set, --expect, and --dump-debug");

    juce::File dumpDir(dumpPath);
    juce::String error;
    if (! ensureDirectory(dumpDir, error))
        throw std::runtime_error(error.toStdString());

    if (destPath.isEmpty())
        destPath = dumpDir.getFullPathName();

    juce::File destDir(destPath);
    if (! ensureDirectory(destDir, error))
        throw std::runtime_error(error.toStdString());

    auto expected = readExpectedList(juce::File(expectPath));

    SanctSoundClient client;
    if (! client.setDestinationDirectory(destDir))
        throw std::runtime_error("Failed to set destination directory to " + destDir.getFullPathName().toStdString());

    client.setPreviewDebugDirectory(dumpDir);
    client.setPreviewDebugSinks(makePreviewDebugSinks(dumpDir));

    auto log = makeLogFn();
    auto group = findGroupForSet(client, site, setName, log);
    auto preview = client.previewGroup(site, group, false, log);

    std::set<juce::String, std::less<>> expectedSet;
    for (auto& item : expected)
        expectedSet.insert(item);

    std::set<juce::String, std::less<>> selectedSet;
    for (auto& name : preview.names)
        selectedSet.insert(name);

    juce::StringArray missing;
    juce::StringArray unexpected;

    for (const auto& entry : expectedSet)
        if (selectedSet.find(entry) == selectedSet.end())
            missing.add(entry);
    for (const auto& entry : selectedSet)
        if (expectedSet.find(entry) == expectedSet.end())
            unexpected.add(entry);

    juce::File diffFile = dumpDir.getChildFile("debug_expected_vs_selected.txt");
    juce::StringArray lines;
    lines.add("MISSING_FROM_SELECTED=" + (missing.isEmpty() ? juce::String("-") : missing.joinIntoString(",")));
    lines.add("UNEXPECTED_IN_SELECTED=" + (unexpected.isEmpty() ? juce::String("-") : unexpected.joinIntoString(",")));

    if (! writeLinesToFile(diffFile, lines, error))
        throw std::runtime_error(error.toStdString());

    const bool ok = missing.isEmpty() && unexpected.isEmpty();
    std::cout << "Verification " << (ok ? "passed" : "FAILED") << std::endl;
    return ok ? 0 : 2;
}

void printUsage()
{
    std::cout << "Usage:\n"
              << "  sanctsound_cli preview --site <code> --set <group> --dest <dir> --dump-debug <dir> [--dry-run=<bool>]\n"
              << "  sanctsound_cli list-audio --site <code> --folder <deployment> --tmin <iso> --tmax <iso> --dump <file>\n"
              << "  sanctsound_cli verify-expected --site <code> --set <group> --expect <file> --dump-debug <dir> [--dest <dir>]\n";
}

} // namespace

int main (int argc, char** argv)
{
    if (argc <= 1)
    {
        printUsage();
        return 1;
    }

    auto parsed = parseCommandLine(argc, argv);
    if (parsed.subcommand.isEmpty())
    {
        printUsage();
        return 1;
    }

    try
    {
        if (parsed.subcommand == "preview")
            return runPreviewCommand(parsed);
        if (parsed.subcommand == "list-audio")
            return runListAudioCommand(parsed);
        if (parsed.subcommand == "verify-expected")
            return runVerifyCommand(parsed);

        printUsage();
        return 1;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 2;
    }
}

