#include <juce_core/juce_core.h>
#include <algorithm>
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
        auto labels = client.siteLabels();
        Logger::writeToLog("Sites: " + String(labels.size()));

        const juce::String siteCode = "ci01";
        const juce::String groupName = "sanctsound_ci01_02_bluewhale";

        auto logFn = [](const juce::String& message)
        {
            juce::Logger::writeToLog(message);
        };

        auto listing = client.listAudioObjectsForGroup(siteCode, groupName, logFn);
        client.writeAudioListingDebugFiles(listing);

        Logger::writeToLog("Listing prefix: " + listing.prefix);
        Logger::writeToLog("Total listed=" + String(listing.totalListed)
                           + ", kept=" + String((int) listing.uniqueObjects.size()));

        Logger::writeToLog("First kept objects:");
        for (size_t i = 0; i < std::min<size_t>(12, listing.uniqueObjects.size()); ++i)
        {
            auto label = juce::String((int) (i + 1)).paddedLeft('0', 2);
            Logger::writeToLog("  [" + label + "] " + listing.uniqueObjects[i]);
        }

        Logger::writeToLog("SanctSound CLI parity check complete");
        return 0;
    }
    catch (const std::exception& e)
    {
        Logger::writeToLog(String("Error: ") + e.what());
        return 1;
    }
}

