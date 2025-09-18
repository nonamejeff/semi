#pragma once

#include <juce_gui_extra/juce_gui_extra.h>
#include <map>
#include <vector>

#include "SanctSoundClient.h"
#include "MetadataView.h"

namespace sanctsound
{
class MainComponent : public juce::Component,
                      private juce::Button::Listener,
                      private juce::ComboBox::Listener
{
public:
    MainComponent();
    ~MainComponent() override;

    void resized() override;

private:
    struct GroupEntry
    {
        ProductGroup group;
        juce::String mode;
        bool selected = false;
    };

    struct FileEntry
    {
        ListedFile file;
        bool selected = true;
    };

    class GroupRow;
    class FileRow;
    class GroupListModel;
    class FileListModel;

    void buttonClicked(juce::Button* button) override;
    void comboBoxChanged(juce::ComboBox* comboBoxThatHasChanged) override;

    void handleListSets();
    void handlePreview();
    void handleDownload();
    void handleClip();
    void toggleLogWindow();

    void populateSiteCombo();
    juce::StringArray selectedGroupNames() const;
    void setStatus(const juce::String& status);
    void logMessage(const juce::String& message);

    void runInBackground(std::function<void()> task);

    void onGroupInfo(int index);
    void onGroupToggled(int index, bool state);
    void onFileToggled(int index, bool state);
    void selectAllFiles(bool state);

    void updateFileList(const juce::String& groupName, const PreviewResult& preview);
    void updateSelectionLabel();

    SanctSoundClient client;

    juce::ComboBox siteCombo;
    juce::TextEditor tagEditor;
    juce::TextButton refreshButton;
    juce::TextButton listButton;
    juce::ToggleButton onlyLongRunsToggle;
    juce::TextButton previewButton;
    juce::TextButton downloadButton;
    juce::TextButton clipButton;
    juce::TextButton chooseDestButton;
    juce::TextButton logButton;
    juce::Label destinationLabel;
    juce::Label statusLabel;

    MetadataView metadataView;

    juce::ListBox setsList { "Sets" };
    juce::ListBox filesList { "Files" };
    juce::Label previewSummary;
    juce::TextEditor runsEditor;
    juce::Label selectionLabel;

    std::unique_ptr<juce::DocumentWindow> logWindow;
    std::unique_ptr<juce::TextEditor> logEditor;

    std::vector<GroupEntry> groups;
    std::vector<FileEntry> files;
    std::map<juce::String, PreviewCache> previewCache;
    juce::String lastPreviewGroup;

    std::unique_ptr<GroupListModel> groupListModel;
    std::unique_ptr<FileListModel> fileListModel;

    juce::CriticalSection stateLock;

    JUCE_DECLARE_NON_COPYABLE_WITH_LEAK_DETECTOR(MainComponent)
};

} // namespace sanctsound
