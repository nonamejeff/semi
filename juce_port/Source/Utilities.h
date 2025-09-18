#pragma once

#include <juce_core/juce_core.h>

namespace sanctsound
{
struct CommandResult
{
    int exitCode = -1;
    juce::String output;
    juce::StringArray lines;
};

CommandResult runCommand(const juce::StringArray& argv);

juce::String formatCommand(const juce::StringArray& argv);

juce::String humaniseError(const juce::String& context, const CommandResult& result);

bool parseTimestamp(const juce::String& text, juce::Time& out);

juce::String toIso(const juce::Time& time);

struct CsvTable
{
    juce::StringArray header;
    juce::Array<juce::StringArray> rows;
};

CsvTable readCsvFile(const juce::File& file);

juce::StringArray splitCsvLine(const juce::String& line);

} // namespace sanctsound
