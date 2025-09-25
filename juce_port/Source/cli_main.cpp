#include <juce_core/juce_core.h>

#include "SanctSoundClient.h"

using namespace sanctsound;

namespace
{
bool runSelfTest()
{
    juce::Logger::writeToLog("sanctsound_cli: selftest start");

    SanctSoundClient client;
    juce::ignoreUnused(client);

    bool ok = true;

    juce::Time parsedStart;
    const bool parsed = SanctSoundClient::parseAudioStartFromName(
        "sanctsound_ci01_02_20210101T030000Z.wav", parsedStart);
    juce::ignoreUnused(parsedStart);
    if (! parsed)
    {
        juce::Logger::writeToLog("selftest: parseAudioStartFromName failed");
        ok = false;
    }

    const auto folder = SanctSoundClient::folderFromSetName("SanctSound_CI01_02_BlueWhale");
    if (folder != "sanctsound_ci01_02")
    {
        juce::Logger::writeToLog("selftest: folderFromSetName unexpected result: " + folder);
        ok = false;
    }

    if (ok)
        juce::Logger::writeToLog("sanctsound_cli: selftest OK");
    else
        juce::Logger::writeToLog("sanctsound_cli: selftest FAILED");

    return ok;
}
} // namespace

int main (int argc, char** argv)
{
    bool runSelfTestMode = false;

    for (int i = 1; i < argc; ++i)
    {
        juce::String arg (argv[i]);
        if (arg == "--selftest")
        {
            runSelfTestMode = true;
            break;
        }
    }

    if (argc <= 1)
        runSelfTestMode = true;

    if (runSelfTestMode)
        return runSelfTest() ? 0 : 1;

    juce::Logger::writeToLog("sanctsound_cli: pass --selftest to run checks");
    return 0;
}
