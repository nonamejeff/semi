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
    if (result.output.isNotEmpty())
        msg << "\n" << result.output;
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

    auto tryIso = juce::Time::fromISO8601(trimmed);
    if (trimmed.containsChar('-') && trimmed.containsChar(':'))
    {
        out = tryIso;
        return true;
    }

    if (! trimmed.containsAnyOf("zZ"))
    {
        auto isoWithZ = juce::Time::fromISO8601(trimmed + "Z");
        if (isoWithZ != juce::Time())
        {
            out = isoWithZ;
            return true;
        }
    }

    auto alt = juce::Time::fromString(trimmed, true);
    if (alt != juce::Time())
    {
        out = alt;
        return true;
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

    table.header = splitCsvLine(lines.removeAndReturn(0));
    for (auto& line : lines)
        table.rows.add(splitCsvLine(line));
    return table;
}

} // namespace sanctsound
