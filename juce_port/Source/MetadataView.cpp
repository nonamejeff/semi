#include "MetadataView.h"

namespace sanctsound
{

// ======================== SummaryPanel (full type) ========================

class MetadataView::SummaryPanel : public juce::Component
{
public:
    SummaryPanel(juce::Label& messageLabelRef,
                 juce::Label& site,
                 juce::Label& deployment,
                 juce::Label& platform,
                 juce::Label& recorder,
                 juce::Label& coordinates,
                 juce::Label& start,
                 juce::Label& end,
                 juce::Label& sampleRate,
                 juce::Label& note,
                 std::vector<std::unique_ptr<juce::Label>>& titlesRef)
        : message(messageLabelRef),
          siteValue(site),
          deploymentValue(deployment),
          platformValue(platform),
          recorderValue(recorder),
          coordinatesValue(coordinates),
          startValue(start),
          endValue(end),
          sampleRateValue(sampleRate),
          noteValue(note),
          titles(titlesRef)
    {}

    void resized() override
    {
        auto area = getLocalBounds().reduced(12);

        if (message.isVisible())
        {
            message.setBounds(area);
            return;
        }

        auto layoutCard = [](juce::Rectangle<int> bounds, juce::Label* title, juce::Label& value)
        {
            if (title != nullptr)
            {
                auto titleBounds = bounds.removeFromTop(18);
                title->setBounds(titleBounds);
            }
            value.setBounds(bounds.reduced(0, 2));
        };

        const int columnWidth = juce::jmax(120, area.getWidth() / 3);
        const int rowHeight   = 72;

        auto row1 = area.removeFromTop(rowHeight);
        layoutCard(row1.removeFromLeft(columnWidth), titles.size() > 0 ? titles[0].get() : nullptr, siteValue);
        layoutCard(row1.removeFromLeft(columnWidth), titles.size() > 1 ? titles[1].get() : nullptr, deploymentValue);
        layoutCard(row1,                                 titles.size() > 2 ? titles[2].get() : nullptr, platformValue);

        auto row2 = area.removeFromTop(rowHeight);
        layoutCard(row2.removeFromLeft(columnWidth), titles.size() > 3 ? titles[3].get() : nullptr, recorderValue);
        layoutCard(row2,                                 titles.size() > 4 ? titles[4].get() : nullptr, coordinatesValue);

        auto row3 = area.removeFromTop(rowHeight);
        layoutCard(row3.removeFromLeft(columnWidth), titles.size() > 5 ? titles[5].get() : nullptr, startValue);
        layoutCard(row3.removeFromLeft(columnWidth), titles.size() > 6 ? titles[6].get() : nullptr, endValue);
        layoutCard(row3,                                 titles.size() > 7 ? titles[7].get() : nullptr, sampleRateValue);

        layoutCard(area,                                titles.size() > 8 ? titles[8].get() : nullptr, noteValue);
    }

private:
    juce::Label& message;
    juce::Label& siteValue;
    juce::Label& deploymentValue;
    juce::Label& platformValue;
    juce::Label& recorderValue;
    juce::Label& coordinatesValue;
    juce::Label& startValue;
    juce::Label& endValue;
    juce::Label& sampleRateValue;
    juce::Label& noteValue;
    std::vector<std::unique_ptr<juce::Label>>& titles;
};

// ======================== MetadataView impl ========================

MetadataView::MetadataView()
{
    addAndMakeVisible(titleLabel);
    titleLabel.setText("Metadata", juce::dontSendNotification);
    titleLabel.setJustificationType(juce::Justification::left);
    titleLabel.setFont(juce::FontOptions(16.0f, juce::Font::bold));

    summaryTab = std::make_unique<SummaryPanel>(messageLabel,
                                                siteValue,
                                                deploymentValue,
                                                platformValue,
                                                recorderValue,
                                                coordinatesValue,
                                                startValue,
                                                endValue,
                                                sampleRateValue,
                                                noteValue,
                                                titleLabels);
    tabs.addTab("Summary", juce::Colours::transparentBlack, summaryTab.get(), false);

    rawEditor.setMultiLine(true, true);
    rawEditor.setReadOnly(true);
    rawEditor.setScrollbarsShown(true, true);
    rawEditor.setFont(juce::FontOptions(13.0f));
    rawEditor.setTextToShowWhenEmpty("Select a set to view metadata.", juce::Colours::grey);
    tabs.addTab("Raw JSON", juce::Colours::transparentBlack, &rawEditor, false);

    addAndMakeVisible(tabs);

    initialiseSummaryCards();
    showMessage("Select a set to view metadata.");
}

// Out-of-line dtor so unique_ptr<SummaryPanel> sees the full type
MetadataView::~MetadataView() = default;

void MetadataView::initialiseSummaryCards()
{
    summaryTab->addAndMakeVisible(messageLabel);
    messageLabel.setJustificationType(juce::Justification::centred);
    messageLabel.setColour(juce::Label::textColourId, juce::Colours::darkgrey);

    auto configureValue = [](juce::Label& label)
    {
        label.setJustificationType(juce::Justification::left);
        label.setFont(juce::FontOptions(14.0f));
        label.setColour(juce::Label::textColourId, juce::Colours::black);
        label.setBorderSize(juce::BorderSize<int>(2));
        label.setMinimumHorizontalScale(0.6f);
    };

    auto addCard = [this, configureValue](const juce::String& title, juce::Label& value)
    {
        auto label = std::make_unique<juce::Label>();
        label->setText(title, juce::dontSendNotification);
        label->setJustificationType(juce::Justification::left);
        label->setColour(juce::Label::textColourId, juce::Colours::darkgrey);
        label->setFont(juce::FontOptions(12.5f, juce::Font::bold));
        summaryTab->addAndMakeVisible(label.get());
        titleLabels.push_back(std::move(label));

        configureValue(value);
        summaryTab->addAndMakeVisible(value);
    };

    addCard("SITE",                 siteValue);
    addCard("DEPLOYMENT",           deploymentValue);
    addCard("PLATFORM",             platformValue);
    addCard("RECORDER",             recorderValue);
    addCard("COORDINATES / DEPTH",  coordinatesValue);
    addCard("START (UTC)",          startValue);
    addCard("END (UTC)",            endValue);
    addCard("SAMPLE RATE",          sampleRateValue);
    addCard("LOCATION NOTE",        noteValue);
}

void MetadataView::setGroupTitle(const juce::String& groupName)
{
    titleLabel.setText(groupName.isNotEmpty() ? groupName : juce::String("Metadata"),
                       juce::dontSendNotification);
}

void MetadataView::setSummary(const MetadataSummary& summary)
{
    messageLabel.setVisible(false);
    for (auto& title : titleLabels) title->setVisible(true);

    auto setValue = [](juce::Label& label, const juce::String& text)
    {
        auto trimmed = text.trim();
        label.setText(trimmed.isNotEmpty() ? trimmed : juce::String("—"),
                      juce::dontSendNotification);
        label.setVisible(true);
    };

    setValue(siteValue,         summary.site);
    setValue(deploymentValue,   summary.deployment);
    setValue(platformValue,     summary.platform);
    setValue(recorderValue,     summary.recorder);
    setValue(coordinatesValue,  summary.coordinates);
    setValue(startValue,        summary.start);
    setValue(endValue,          summary.end);
    setValue(sampleRateValue,   summary.sampleRate);
    setValue(noteValue,         summary.note);

    summaryTab->resized();
}

void MetadataView::setRawJson(const juce::String& rawText)
{
    rawEditor.setText(rawText, juce::dontSendNotification);
}

void MetadataView::showMessage(const juce::String& message)
{
    messageLabel.setText(message, juce::dontSendNotification);
    messageLabel.setVisible(true);

    for (auto& title : titleLabels) title->setVisible(false);

    auto hideValue = [](juce::Label& label)
    {
        label.setText("—", juce::dontSendNotification);
        label.setVisible(false);
    };

    hideValue(siteValue);
    hideValue(deploymentValue);
    hideValue(platformValue);
    hideValue(recorderValue);
    hideValue(coordinatesValue);
    hideValue(startValue);
    hideValue(endValue);
    hideValue(sampleRateValue);
    hideValue(noteValue);

    summaryTab->resized();
}

void MetadataView::resized()
{
    auto bounds = getLocalBounds();
    auto header = bounds.removeFromTop(30);
    titleLabel.setBounds(header);
    tabs.setBounds(bounds);
}

} // namespace sanctsound
