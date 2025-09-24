#include <iostream>
#include "SanctSoundClient.h"

int main()
{
    using sanctsound::SanctSoundClient;

    juce::Time ts;
    const juce::String f1 = "SanctSound_CI01_02_20190624T120000Z.flac";
    const juce::String f2 = "SanctSound_CI01_02_190624120000.flac";
    const juce::String set = "sanctsound_ci01_02_bluewhale";

    const bool ok1 = SanctSoundClient::parseAudioStartFromName(f1, ts);
    const bool ok2 = SanctSoundClient::parseAudioStartFromName(f2, ts);
    const juce::String folder = SanctSoundClient::folderFromSetName(set);

    std::cout << "[parser] ISO-Z    : " << (ok1 ? "OK" : "FAIL") << "\n";
    std::cout << "[parser] short12  : " << (ok2 ? "OK" : "FAIL") << "\n";
    std::cout << "[parser] folder   : " << folder << "\n";

    if (!ok1 || !ok2 || folder.isEmpty())
        return 2;

    std::cout << "Self-test passed.\n";
    return 0;
}
