#include <juce_core/juce_core.h>
#include <algorithm>
#include <stdexcept>
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
        auto cwd = juce::File::getCurrentWorkingDirectory();
        auto offlineRoot = cwd.getChildFile("offline_data");
        if (offlineRoot.isDirectory())
        {
            client.setOfflineDataRoot(offlineRoot);
            juce::Logger::writeToLog("Using offline data root: " + offlineRoot.getFullPathName());
        }

        auto destDir = cwd.getChildFile("cli_output");
        if (! client.setDestinationDirectory(destDir))
            throw std::runtime_error("Failed to set destination directory: " + destDir.getFullPathName().toStdString());

        auto labels = client.siteLabels();
        Logger::writeToLog("Sites: " + String(labels.size()));

        const juce::String siteCode = "ci01";
        const juce::String groupName = "sanctsound_ci01_02_bluewhale";

        auto logFn = [](const juce::String& message)
        {
            juce::Logger::writeToLog(message);
        };

        auto groups = client.listProductGroups(siteCode, {}, logFn);
        const sanctsound::ProductGroup* targetGroup = nullptr;
        for (auto& g : groups)
        {
            if (g.name == groupName)
            {
                targetGroup = &g;
                break;
            }
        }

        if (targetGroup == nullptr)
            throw std::runtime_error("Group not found: " + groupName.toStdString());

        auto preview = client.previewGroup(siteCode, *targetGroup, false, logFn);

        Logger::writeToLog("CSV windows=" + String(preview.windows.size())
                           + ", matched=" + String(preview.matchedWindows)
                           + ", unique_files=" + String(preview.files.size()));

        Logger::writeToLog("First 10 matched objects:");
        for (int i = 0; i < juce::jmin<int>(10, preview.urls.size()); ++i)
            Logger::writeToLog("  " + preview.urls[i]);

        Logger::writeToLog("SanctSound CLI parity check complete");
        return 0;
    }
    catch (const std::exception& e)
    {
        Logger::writeToLog(String("Error: ") + e.what());
        return 1;
    }
}

