#include "MainComponent.h"

// Ensure complete types are visible in this TU
#include "MetadataView.h"
#include "PreviewModels.h"
#include "SanctSoundClient.h"

#include <juce_gui_extra/juce_gui_extra.h>
#include <utility>
#include <thread>
#include <iostream>

namespace sanctsound
{

namespace
{
juce::String determineMode(const juce::String& name)
{
    auto lower = name.toLowerCase();
    if (lower.endsWith("_1h")) return "HOUR";
    if (lower.endsWith("_1d")) return "DAY";
    return "EVENT";
}

juce::String formatExtCounts(const ProductGroup& group)
{
    juce::StringArray parts;
    for (auto& kv : group.extCounts)
        parts.add(kv.first + ":" + juce::String(kv.second));
    return parts.joinIntoString(", ");
}

// Convert juce::StringArray -> juce::Array<juce::String>
static juce::Array<juce::String> toArray(const juce::StringArray& sa)
{
    juce::Array<juce::String> a;
    for (auto& s : sa) a.add(s);
    return a;
}
} // namespace

// ======================= GroupRow =======================

class MainComponent::GroupRow : public juce::Component,
                                private juce::Button::Listener
{
public:
    explicit GroupRow(MainComponent& ownerRef) : owner(ownerRef)
    {
        addAndMakeVisible(toggle);
        toggle.addListener(this);

        addAndMakeVisible(infoButton);
        infoButton.setButtonText("Info");
        infoButton.addListener(this);

        addAndMakeVisible(nameLabel);
        nameLabel.setJustificationType(juce::Justification::left);

        addAndMakeVisible(metaLabel);
        metaLabel.setJustificationType(juce::Justification::left);
        metaLabel.setColour(juce::Label::textColourId, juce::Colours::dimgrey);
    }

    void setRow(int newIndex) { rowIndex = newIndex; }

    void update(const GroupEntry* entry)
    {
        juce::ScopedValueSetter<bool> svs(guard, true);

        const auto hasEntry = (entry != nullptr);
        toggle.setEnabled(hasEntry);
        infoButton.setEnabled(hasEntry);

        if (hasEntry)
        {
            toggle.setToggleState(entry->selected, juce::dontSendNotification);
            nameLabel.setText(entry->group.name, juce::dontSendNotification);
            metaLabel.setText("[" + entry->mode.toLowerCase() + "]  [" + formatExtCounts(entry->group) + "]",
                              juce::dontSendNotification);
        }
        else
        {
            toggle.setToggleState(false, juce::dontSendNotification);
            nameLabel.setText({}, juce::dontSendNotification);
            metaLabel.setText({}, juce::dontSendNotification);
        }
    }

    void resized() override
    {
        auto bounds = getLocalBounds().reduced(4);
        toggle.setBounds(bounds.removeFromLeft(40));
        infoButton.setBounds(bounds.removeFromLeft(60));
        auto nameArea = bounds.removeFromLeft(bounds.getWidth() / 2);
        nameLabel.setBounds(nameArea);
        metaLabel.setBounds(bounds);
    }

private:
    void buttonClicked(juce::Button* b) override
    {
        if (guard || rowIndex < 0) return;
        if (b == &toggle)         owner.onGroupToggled(rowIndex, toggle.getToggleState());
        else if (b == &infoButton) owner.onGroupInfo(rowIndex);
    }

    MainComponent&   owner;
    int              rowIndex = 0;
    juce::ToggleButton toggle;
    juce::TextButton   infoButton;
    juce::Label        nameLabel;
    juce::Label        metaLabel;
    bool               guard = false;
};

// ======================= FileRow =======================

class MainComponent::FileRow : public juce::Component,
                               private juce::Button::Listener
{
public:
    explicit FileRow(MainComponent& ownerRef) : owner(ownerRef)
    {
        addAndMakeVisible(toggle);
        toggle.addListener(this);

        addAndMakeVisible(nameLabel);
        nameLabel.setJustificationType(juce::Justification::left);

        addAndMakeVisible(timeLabel);
        timeLabel.setColour(juce::Label::textColourId, juce::Colours::dimgrey);

        addAndMakeVisible(urlLabel);
        urlLabel.setColour(juce::Label::textColourId, juce::Colours::dimgrey);
        urlLabel.setJustificationType(juce::Justification::left);
    }

    void setRow(int idx) { rowIndex = idx; }

    void update(const FileEntry* entry)
    {
        juce::ScopedValueSetter<bool> svs(guard, true);

        const auto hasEntry = (entry != nullptr);
        toggle.setEnabled(hasEntry);

        if (hasEntry)
        {
            toggle.setToggleState(entry->selected, juce::dontSendNotification);
            nameLabel.setText(entry->file.name, juce::dontSendNotification);

            juce::String times;
            times << entry->file.start.toISO8601(true) << " -> " << entry->file.end.toISO8601(true);
            timeLabel.setText(times, juce::dontSendNotification);

            urlLabel.setText(entry->file.url, juce::dontSendNotification);
        }
        else
        {
            toggle.setToggleState(false, juce::dontSendNotification);
            nameLabel.setText({}, juce::dontSendNotification);
            timeLabel.setText({}, juce::dontSendNotification);
            urlLabel.setText({}, juce::dontSendNotification);
        }
    }

    void resized() override
    {
        auto bounds = getLocalBounds().reduced(4);
        toggle.setBounds(bounds.removeFromLeft(40));
        nameLabel.setBounds(bounds.removeFromLeft(220));
        timeLabel.setBounds(bounds.removeFromLeft(260));
        urlLabel.setBounds(bounds);
    }

private:
    void buttonClicked(juce::Button* b) override
    {
        if (guard || rowIndex < 0) return;
        if (b == &toggle) owner.onFileToggled(rowIndex, toggle.getToggleState());
    }

    MainComponent&     owner;
    int                rowIndex = 0;
    juce::ToggleButton toggle;
    juce::Label        nameLabel;
    juce::Label        timeLabel;
    juce::Label        urlLabel;
    bool               guard = false;
};

// ======================= List Models =======================

class MainComponent::GroupListModel : public juce::ListBoxModel
{
public:
    explicit GroupListModel(MainComponent& mc) : owner(mc) {}
    int getNumRows() override { return static_cast<int>(owner.groups.size()); }

    juce::Component* refreshComponentForRow(int rowNumber, bool, juce::Component* existing) override
    {
        auto* row = dynamic_cast<GroupRow*>(existing);
        if (row == nullptr) row = new GroupRow(owner);
        if ((size_t)rowNumber < owner.groups.size())
        {
            row->setRow(rowNumber);
            row->update(&owner.groups[(size_t)rowNumber]);
        }
        else
        {
            row->setRow(-1);
            row->update(nullptr);
        }
        return row;
    }

    // Required pure virtual in ListBoxModel
    void paintListBoxItem(int, juce::Graphics&, int, int, bool) override {}

private:
    MainComponent& owner;
};

class MainComponent::FileListModel : public juce::ListBoxModel
{
public:
    explicit FileListModel(MainComponent& mc) : owner(mc) {}
    int getNumRows() override { return static_cast<int>(owner.files.size()); }

    juce::Component* refreshComponentForRow(int rowNumber, bool, juce::Component* existing) override
    {
        auto* row = dynamic_cast<FileRow*>(existing);
        if (row == nullptr) row = new FileRow(owner);
        if ((size_t)rowNumber < owner.files.size())
        {
            row->setRow(rowNumber);
            row->update(&owner.files[(size_t)rowNumber]);
        }
        else
        {
            row->setRow(-1);
            row->update(nullptr);
        }
        return row;
    }

    // Required pure virtual in ListBoxModel
    void paintListBoxItem(int, juce::Graphics&, int, int, bool) override {}

private:
    MainComponent& owner;
};

// ======================= MainComponent =======================

MainComponent::MainComponent()
{
    addAndMakeVisible(siteCombo);
    addAndMakeVisible(tagEditor);
    addAndMakeVisible(refreshButton);
    addAndMakeVisible(listButton);
    addAndMakeVisible(onlyLongRunsToggle);
    addAndMakeVisible(previewButton);
    addAndMakeVisible(downloadButton);
    addAndMakeVisible(clipButton);
    addAndMakeVisible(chooseDestButton);
    addAndMakeVisible(logButton);
    addAndMakeVisible(destinationLabel);
    addAndMakeVisible(statusLabel);
    addAndMakeVisible(metadataView);
    addAndMakeVisible(setsList);
    addAndMakeVisible(filesList);
    addAndMakeVisible(previewSummary);
    addAndMakeVisible(runsEditor);
    addAndMakeVisible(selectionLabel);

    tagEditor.setText("dolphin");
    tagEditor.setTextToShowWhenEmpty("dolphin", juce::Colours::grey);
    onlyLongRunsToggle.setButtonText("Only runs >= 2h");
    refreshButton.setButtonText("Refresh");
    listButton.setButtonText("List sets");
    previewButton.setButtonText("Preview");
    downloadButton.setButtonText("Download");
    clipButton.setButtonText("Clip");
    chooseDestButton.setButtonText("Choose...");
    logButton.setButtonText("Log...");

    previewButton.setEnabled(false);
    downloadButton.setEnabled(false);
    clipButton.setEnabled(false);

    runsEditor.setMultiLine(true);
    runsEditor.setReadOnly(true);
    runsEditor.setColour(juce::TextEditor::backgroundColourId, juce::Colours::lightgrey);

    selectionLabel.setText("0 files selected", juce::dontSendNotification);

    refreshButton.addListener(this);
    listButton.addListener(this);
    previewButton.addListener(this);
    downloadButton.addListener(this);
    clipButton.addListener(this);
    chooseDestButton.addListener(this);
    logButton.addListener(this);
    onlyLongRunsToggle.addListener(this);

    populateSiteCombo();

    destinationLabel.setText(client.getDestinationDirectory().getFullPathName(), juce::dontSendNotification);
    setStatus("Ready");

    groupListModel = std::make_unique<GroupListModel>(*this);
    fileListModel  = std::make_unique<FileListModel>(*this);
    setsList.setModel(groupListModel.get());
    filesList.setModel(fileListModel.get());

    setSize(1280, 880);
}

MainComponent::~MainComponent()
{
    if (logWindow != nullptr)
        logWindow->setVisible(false);
}

void MainComponent::resized()
{
    auto bounds = getLocalBounds().reduced(12);

    auto row1 = bounds.removeFromTop(32);
    siteCombo.setBounds(row1.removeFromLeft(220));
    refreshButton.setBounds(row1.removeFromLeft(80));
    tagEditor.setBounds(row1.removeFromLeft(160));
    listButton.setBounds(row1.removeFromLeft(120));
    onlyLongRunsToggle.setBounds(row1);

    auto row2 = bounds.removeFromTop(32);
    destinationLabel.setBounds(row2.removeFromLeft(360));
    chooseDestButton.setBounds(row2.removeFromLeft(120));
    logButton.setBounds(row2.removeFromLeft(80));

    auto body = bounds.removeFromTop(bounds.getHeight() - 48);
    auto left = body.removeFromLeft(body.getWidth() / 2);
    metadataView.setBounds(left.removeFromBottom(220));
    setsList.setBounds(left);

    auto right = body;
    previewSummary.setBounds(right.removeFromTop(24));
    runsEditor.setBounds(right.removeFromTop(120));
    filesList.setBounds(right.removeFromTop(right.getHeight() - 28));
    selectionLabel.setBounds(right);

    auto bottom = bounds;
    previewButton.setBounds(bottom.removeFromLeft(120));
    downloadButton.setBounds(bottom.removeFromLeft(120));
    clipButton.setBounds(bottom.removeFromLeft(120));
    statusLabel.setBounds(bottom);
}

void MainComponent::buttonClicked(juce::Button* button)
{
    if (button == &listButton)            handleListSets();
    else if (button == &previewButton)    handlePreview();
    else if (button == &downloadButton)   handleDownload();
    else if (button == &clipButton)       handleClip();
    else if (button == &chooseDestButton)
    {
        juce::FileChooser chooser("Choose destination", client.getDestinationDirectory());
        if (chooser.browseForDirectory())
        {
            if (! client.setDestinationDirectory(chooser.getResult()))
            {
                juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::WarningIcon,
                    "Folder error", "Cannot use the selected folder. Check permissions.");
                return;
            }
            destinationLabel.setText(chooser.getResult().getFullPathName(), juce::dontSendNotification);
        }
    }
    else if (button == &logButton)        toggleLogWindow();
}

void MainComponent::comboBoxChanged(juce::ComboBox*) {}

void MainComponent::handleListSets()
{
    auto site = client.codeForLabel(siteCombo.getText());
    auto tag  = tagEditor.getText().trim();

    setStatus("Listing sets...");
    listButton.setEnabled(false);
    previewButton.setEnabled(false);
    downloadButton.setEnabled(false);
    clipButton.setEnabled(false);

    runInBackground([this, site, tag]()
    {
        auto appendLog = [this](const juce::String& s)
        {
            if (logWindow != nullptr)
            {
                logText.moveCaretToEnd();
                logText.insertTextAtCaret(s.endsWithChar('\n') ? s : (s + "\n"));
            }
            DBG(s);
            std::cout << s;
            std::cout.flush();
        };

        try
        {
            auto groupsResult = client.listProductGroups(site, tag,
                [appendLog](const juce::String& msg) { appendLog(msg + "\n"); });

            juce::MessageManager::callAsync([this, groupsResult]() mutable
            {
                groups.clear();
                for (auto& g : groupsResult)
                {
                    GroupEntry entry;
                    entry.group = g;
                    entry.mode = determineMode(g.name);
                    entry.selected = false;
                    groups.push_back(entry);
                }
                setsList.updateContent();
                setsList.repaint();
                metadataView.showMessage("Select a set and click Info.");
                previewButton.setEnabled(!groups.empty());
                listButton.setEnabled(true);
                setStatus("Found " + juce::String(groups.size()) + " sets. Select and preview.");
            });
        }
        catch (const std::exception& e)
        {
            juce::MessageManager::callAsync([this, msg = juce::String(e.what())]()
            {
                juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::WarningIcon, "List failed", msg);
                listButton.setEnabled(true);
                setStatus("List failed");
            });
        }
    });
}

void MainComponent::handlePreview()
{
    auto selected = selectedGroupNames();
    if (selected.isEmpty())
    {
        juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::InfoIcon, "Select at least one group", "");
        return;
    }

    auto site     = client.codeForLabel(siteCombo.getText());
    auto onlyLong = onlyLongRunsToggle.getToggleState();

    std::vector<ProductGroup> groupsToPreview;
    groupsToPreview.reserve((size_t)selected.size());
    for (auto& entry : groups)
        if (entry.selected) groupsToPreview.push_back(entry.group);
    if (groupsToPreview.empty()) return;

    setStatus("Previewing...");
    previewButton.setEnabled(false);
    downloadButton.setEnabled(false);
    clipButton.setEnabled(false);

    runInBackground([this, site, onlyLong, groupsToPreview = std::move(groupsToPreview)]() mutable
    {
        auto appendLog = [this](const juce::String& s)
        {
            if (logWindow != nullptr)
            {
                logText.moveCaretToEnd();
                logText.insertTextAtCaret(s.endsWithChar('\n') ? s : (s + "\n"));
            }
            DBG(s);
            std::cout << s;
            std::cout.flush();
        };

        for (size_t idx = 0; idx < groupsToPreview.size(); ++idx)
        {
            const auto& group = groupsToPreview[idx];
            auto name = group.name;

            try
            {
                appendLog("\n=== Preview " + name + " ===\n");
                auto preview = client.previewGroup(site, group, onlyLong,
                    [appendLog](const juce::String& msg) { appendLog(msg + "\n"); });

                auto isLast = (idx + 1 == groupsToPreview.size());
                juce::MessageManager::callAsync([this, name, preview, isLast]() mutable
                {
                    previewCache[name] = PreviewCache { preview.mode, preview.windows };
                    updateFileList(name, preview);
                    if (isLast) previewButton.setEnabled(true);
                    downloadButton.setEnabled(!files.empty());
                    if (isLast) setStatus("Preview ready");
                });
            }
            catch (const std::exception& e)
            {
                juce::MessageManager::callAsync([this, msg = juce::String(e.what())]()
                {
                    juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::WarningIcon, "Preview failed", msg);
                    previewButton.setEnabled(true);
                    setStatus("Preview failed");
                });
            }
        }
    });
}

void MainComponent::handleDownload()
{
    juce::StringArray urls;
    for (auto& f : files)
        if (f.selected) urls.addIfNotAlreadyThere(f.file.url);

    if (urls.isEmpty())
    {
        juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::InfoIcon, "Select at least one file", "");
        return;
    }

    setStatus("Downloading files...");
    downloadButton.setEnabled(false);

    runInBackground([this, urls]()
    {
        auto appendLog = [this](const juce::String& s)
        {
            if (logWindow != nullptr)
            {
                logText.moveCaretToEnd();
                logText.insertTextAtCaret(s.endsWithChar('\n') ? s : (s + "\n"));
            }
            DBG(s);
            std::cout << s;
            std::cout.flush();
        };

        try
        {
            client.downloadFiles(urls, [appendLog](const juce::String& msg) { appendLog(msg + "\n"); });
            juce::MessageManager::callAsync([this]()
            {
                downloadButton.setEnabled(true);
                clipButton.setEnabled(true);
                setStatus("Download complete");
            });
        }
        catch (const std::exception& e)
        {
            juce::MessageManager::callAsync([this, msg = juce::String(e.what())]()
            {
                juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::WarningIcon, "Download failed", msg);
                downloadButton.setEnabled(true);
                setStatus("Download failed");
            });
        }
    });
}

void MainComponent::handleClip()
{
    auto selected = selectedGroupNames();
    if (selected.isEmpty())
    {
        juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::InfoIcon, "Select a group to clip", "");
        return;
    }

    juce::StringArray basenames;
    for (auto& f : files)
        if (f.selected) basenames.addIfNotAlreadyThere(f.file.name);

    if (basenames.isEmpty())
    {
        juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::InfoIcon, "Select files before clipping", "");
        return;
    }

    setStatus("Clipping...");
    clipButton.setEnabled(false);

    auto selectedArr  = toArray(selected);
    auto basenamesArr = toArray(basenames);

    runInBackground([this, selectedArr, basenamesArr]()
    {
        auto appendLog = [this](const juce::String& s)
        {
            if (logWindow != nullptr)
            {
                logText.moveCaretToEnd();
                logText.insertTextAtCaret(s.endsWithChar('\n') ? s : (s + "\n"));
            }
            DBG(s);
            std::cout << s;
            std::cout.flush();
        };

        try
        {
            auto summary = client.clipGroups(selectedArr, previewCache, basenamesArr,
                                             [appendLog](const juce::String& msg) { appendLog(msg + "\n"); });
            juce::MessageManager::callAsync([this, summary]()
            {
                clipButton.setEnabled(true);

                const bool anyWritten = (summary.written > 0);
                const auto title = anyWritten ? juce::String("Clip complete")
                                              : juce::String("No clips written");
                const auto message = anyWritten
                    ? juce::String("Clips written to: ") + summary.directory.getFullPathName()
                    : juce::String("Clip completed but no audio files were written. See log.");

                juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::InfoIcon,
                                                       title,
                                                       message);
                setStatus(anyWritten ? juce::String("Clip complete")
                                     : juce::String("No clips written"));
            });
        }
        catch (const std::exception& e)
        {
            juce::MessageManager::callAsync([this, msg = juce::String(e.what())]()
            {
                if (msg == "Clip produced no audio files; check source paths and windows.")
                {
                    juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::InfoIcon,
                                                           "No clips written",
                                                           "Clip completed but no audio files were written. See log.");
                    setStatus("No clips written");
                }
                else
                {
                    juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::WarningIcon, "Clip failed", msg);
                    setStatus("Clip failed");
                }
                clipButton.setEnabled(true);
            });
        }
    });
}

class LogWrapper : public juce::Component
{
public:
    explicit LogWrapper(juce::TextEditor& editor) : ed(editor)
    {
        addAndMakeVisible(ed);
    }

    void resized() override
    {
        ed.setBounds(getLocalBounds().reduced(8));
    }

private:
    juce::TextEditor& ed;
};

class LogDocWindow : public juce::DocumentWindow
{
public:
    LogDocWindow(juce::Component* contentToOwn, std::function<void()> onClose)
        : juce::DocumentWindow("Log",
                               juce::Colours::black,
                               juce::DocumentWindow::closeButton),
          onCloseFn(std::move(onClose))
    {
        setUsingNativeTitleBar(true);
        setResizable(true, true);
        setContentOwned(contentToOwn, true);
    }

    void closeButtonPressed() override
    {
        setVisible(false);
        if (onCloseFn) onCloseFn();
    }

private:
    std::function<void()> onCloseFn;
};

void MainComponent::toggleLogWindow()
{
    if (logWindow == nullptr)
    {
        logText.setMultiLine(true);
        logText.setReadOnly(true);
        logText.setScrollbarsShown(true, true);
        logText.setFont(juce::Font(13.0f));

        auto* wrapper = new LogWrapper(logText);

        logWindow.reset(new LogDocWindow(wrapper, [this] {
            logWindow.reset();
        }));

        logWindow->centreWithSize(800, 500);
    }

    logWindow->setVisible(true);
    logWindow->toFront(true);
}

void MainComponent::populateSiteCombo()
{
    auto labels = client.siteLabels();
    siteCombo.clear();
    for (int i = 0; i < labels.size(); ++i)
        siteCombo.addItem(labels[i], i + 1);
    if (labels.size() > 0)
        siteCombo.setSelectedId(1);
}

juce::StringArray MainComponent::selectedGroupNames() const
{
    juce::StringArray names;
    for (auto& entry : groups)
        if (entry.selected) names.add(entry.group.name);
    return names;
}

void MainComponent::setStatus(const juce::String& status)
{
    statusLabel.setText(status, juce::dontSendNotification);
}

void MainComponent::logMessage(const juce::String& message)
{
    std::cout << message << std::flush;

    auto appendLog = [this](const juce::String& s)
    {
        if (logWindow != nullptr)
        {
            logText.moveCaretToEnd();
            logText.insertTextAtCaret(s.endsWithChar('\n') ? s : (s + "\n"));
        }
        DBG(s);
    };

    appendLog(message);
}

void MainComponent::runInBackground(std::function<void()> task)
{
    std::thread(std::move(task)).detach();
}

void MainComponent::onGroupInfo(int index)
{
    if (index < 0 || index >= (int)groups.size()) return;

    auto site      = client.codeForLabel(siteCombo.getText());
    auto groupName = groups[(size_t)index].group.name;
    metadataView.setGroupTitle(groupName);
    metadataView.showMessage("Loading metadata...");

    runInBackground([this, site, groupName]()
    {
        auto appendLog = [this](const juce::String& s)
        {
            if (logWindow != nullptr)
            {
                logText.moveCaretToEnd();
                logText.insertTextAtCaret(s.endsWithChar('\n') ? s : (s + "\n"));
            }
            DBG(s);
            std::cout << s;
            std::cout.flush();
        };

        try
        {
            juce::String raw;
            auto summary = client.fetchMetadataSummary(site, groupName, raw,
                [appendLog](const juce::String& msg) { appendLog(msg + "\n"); });

            juce::MessageManager::callAsync([this, summary, raw]()
            {
                metadataView.setSummary(summary);
                metadataView.setRawJson(raw);
            });
        }
        catch (const std::exception& e)
        {
            juce::MessageManager::callAsync([this, msg = juce::String(e.what())]()
            {
                juce::AlertWindow::showMessageBoxAsync(juce::AlertWindow::WarningIcon, "Metadata error", msg);
                metadataView.showMessage("Metadata failed");
            });
        }
    });
}

void MainComponent::onGroupToggled(int index, bool state)
{
    if (index < 0 || index >= (int)groups.size()) return;
    groups[(size_t)index].selected = state;
}

void MainComponent::onFileToggled(int index, bool state)
{
    if (index < 0 || index >= (int)files.size()) return;
    files[(size_t)index].selected = state;
    updateSelectionLabel();
}

void MainComponent::selectAllFiles(bool state)
{
    for (auto& f : files) f.selected = state;
    filesList.updateContent();
    updateSelectionLabel();
}

void MainComponent::updateFileList(const juce::String& groupName, const PreviewResult& preview)
{
    lastPreviewGroup = groupName;
    previewSummary.setText(preview.summary, juce::dontSendNotification);
    runsEditor.setText(preview.runsText);

    files.clear();
    for (auto& file : preview.files)
    {
        FileEntry entry;
        entry.file = file;
        entry.selected = true;
        files.push_back(entry);
    }
    filesList.updateContent();
    updateSelectionLabel();
}

void MainComponent::updateSelectionLabel()
{
    int count = 0;
    for (auto& f : files)
        if (f.selected) ++count;

    selectionLabel.setText(juce::String(count) + " files selected", juce::dontSendNotification);
}

} // namespace sanctsound
