#include <juce_core/juce_core.h>
#include "SanctSoundClient.h"
#include "PreviewModels.h"
#include "Utilities.h"

using namespace juce;

// Minimal smoke test so Codex can run something
int main()
{
    Logger::writeToLog("SanctSound CLI starting...");
    try
    {
        sanctsound::SanctSoundClient client;
        // Do a light sanity check that doesn’t hit GUI or network-only paths
        // (Adjust to whatever is safe in Codex)
        auto labels = client.siteLabels(); // if this is static; otherwise just log
        Logger::writeToLog("Sites: " + String(labels.size()));

        Logger::writeToLog("OK");
        return 0;
    }
    catch (const std::exception& e)
    {
        Logger::writeToLog(String("Error: ") + e.what());
        return 1;
    }
}

