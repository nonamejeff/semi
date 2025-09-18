#pragma once

#include <juce_gui_extra/juce_gui_extra.h>
#include <vector>
#include <memory>
#include "PreviewModels.h"

namespace sanctsound
{

class MetadataView : public juce::Component
{
public:
    MetadataView();
    ~MetadataView(); // <— out-of-line dtor so unique_ptr<SummaryPanel> is destroyed with full type

    void setGroupTitle(const juce::String& groupName);
    void setSummary(const MetadataSummary& summary);
    void setRawJson(const juce::String& rawText);
    void showMessage(const juce::String& message);

    void resized() override;

private:
    void initialiseSummaryCards();

    juce::Label titleLabel;
    juce::TabbedComponent tabs { juce::TabbedButtonBar::TabsAtTop };

    // Forward declaration; full def lives in .cpp
    class SummaryPanel;
    std::unique_ptr<SummaryPanel> summaryTab;

    juce::TextEditor rawEditor;
    juce::Label      messageLabel;

    std::vector<std::unique_ptr<juce::Label>> titleLabels;

    // “Value” labels placed inside SummaryPanel
    juce::Label siteValue;
    juce::Label deploymentValue;
    juce::Label platformValue;
    juce::Label recorderValue;
    juce::Label coordinatesValue;
    juce::Label startValue;
    juce::Label endValue;
    juce::Label sampleRateValue;
    juce::Label noteValue;
};

} // namespace sanctsound
