#include "Utilities.h"

#include <juce_core/juce_core.h>
#include <stdexcept>

namespace sanctsound
{
CommandResult runCommand(const juce::StringArray& argv)
{
    CommandResult result;
    if (argv.isEmpty())
    {
        result.exitCode = -1;
        result.output = "<empty command>";
        return result;
    }

    juce::ChildProcess child;
    if (! child.start(argv, juce::ChildProcess::wantStdOut | juce::ChildProcess::wantStdErr))
    {
        result.exitCode = -1;
        result.output = "Failed to start process";
        return result;
    }

    result.output = child.readAllProcessOutput();
    result.lines.addLines(result.output);
    result.exitCode = child.waitForProcessToFinish(-1);
    return result;
}

juce::String formatCommand(const juce::StringArray& argv)
{
    juce::StringArray parts;
    for (auto arg : argv)
    {
        if (arg.containsAnyOf(" \""))
            parts.add("\"" + arg.replace("\"", "\\\"") + "\"");
        else
            parts.add(arg);
    }
    return parts.joinIntoString(" ");
}

juce::String humaniseError(const juce::String& context, const CommandResult& result)
{
    juce::String msg = context;
    if (result.exitCode != 0)
        msg << " failed with exit code " << result.exitCode << ".";

    auto trimmedOutput = result.output.trim();
    if (trimmedOutput.isNotEmpty())
        msg << "\n" << trimmedOutput;

    juce::StringArray hints;
    auto lowerContext = context.toLowerCase();
    auto lowerOutput  = trimmedOutput.toLowerCase();

    if (lowerContext.contains("gsutil"))
    {
        if (lowerOutput.contains("command not found") || result.exitCode == -1)
            hints.add("Check that the Google Cloud SDK is installed and that `gsutil` is on your PATH.");

        if (trimmedOutput.isEmpty()
            || lowerOutput.contains("not currently authenticated")
            || lowerOutput.contains("anonymous caller")
            || lowerOutput.contains("login"))
        {
            hints.add("Authenticate with Google Cloud before running the app (for example, run `gcloud auth login` or set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable).");
        }

        if (lowerOutput.contains("no urls matched"))
            hints.add("No matching objects were found. Adjust the site or tag filter and try again.");
    }
    else if (lowerContext.contains("ffmpeg"))
    {
        hints.add("Ensure `ffmpeg` is installed and available on your PATH.");
    }
    else if (lowerContext.contains("ffprobe"))
    {
        hints.add("Ensure `ffprobe` (part of ffmpeg) is installed and on your PATH.");
    }

    if (! hints.isEmpty())
        msg << "\n\n" << hints.joinIntoString(" ");

    return msg;
}

bool parseTimestamp(const juce::String& text, juce::Time& out)
{
    auto trimmed = text.trim();
    if (trimmed.isEmpty())
        return false;

    auto hasDigits = trimmed.containsAnyOf("0123456789");
    if (! hasDigits)
        return false;

    auto tryParseIso = [&](const juce::String& candidate)
    {
        if (candidate.isEmpty())
            return false;

        auto parsed = juce::Time::fromISO8601(candidate);

        if (parsed == juce::Time())
        {
            auto digits = candidate.retainCharacters("0123456789");
            if (! digits.startsWith("19700101"))
                return false;
        }

        out = parsed;
        return true;
    };

    juce::StringArray candidates { trimmed };

    if (trimmed.containsChar(' '))
        candidates.add(trimmed.replaceCharacter(' ', 'T'));

    if (trimmed.containsChar('/'))
    {
        auto withDashes = trimmed.replaceCharacter('/', '-');
        candidates.add(withDashes);

        if (withDashes.containsChar(' '))
            candidates.add(withDashes.replaceCharacter(' ', 'T'));
    }

    for (auto candidate : candidates)
    {
        if (tryParseIso(candidate))
            return true;

        if (! candidate.endsWithIgnoreCase("Z"))
        {
            auto withZone = candidate + "Z";
            if (tryParseIso(withZone))
                return true;
        }
    }

    return false;
}

juce::String toIso(const juce::Time& time)
{
    return time.toISO8601(true);
}

juce::StringArray splitCsvLine(const juce::String& line)
{
    juce::StringArray fields;
    juce::String current;
    bool inQuotes = false;

    auto len = line.length();
    for (int i = 0; i < len; ++i)
    {
        auto ch = line[i];
        if (ch == '"')
        {
            if (inQuotes && i + 1 < len && line[i + 1] == '"')
            {
                current << '"';
                ++i;
            }
            else
            {
                inQuotes = ! inQuotes;
            }
            continue;
        }

        if (ch == ',' && ! inQuotes)
        {
            fields.add(current.trim());
            current.clear();
            continue;
        }

        current << ch;
    }

    fields.add(current.trim());
    for (auto& f : fields)
        f = f.trim().unquoted();
    return fields;
}

CsvTable readCsvFile(const juce::File& file)
{
    CsvTable table;
    juce::FileInputStream stream(file);
    if (! stream.openedOk())
        throw std::runtime_error("Failed to open CSV: " + file.getFullPathName().toStdString());

    auto content = stream.readEntireStreamAsString();
    juce::StringArray lines;
    lines.addLines(content);
    lines.removeEmptyStrings(true);

    if (lines.isEmpty())
        return table;

    auto headerLine = lines[0];
    lines.remove(0);
    table.header = splitCsvLine(headerLine);
    for (auto& line : lines)
        table.rows.add(splitCsvLine(line));
    return table;
}

} // namespace sanctsound
