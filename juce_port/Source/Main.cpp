#include <juce_gui_extra/juce_gui_extra.h>
#include "MainComponent.h"

// NOTE: Your MainComponent is inside namespace `sanctsound`.
// Either qualify it here, or `using namespace sanctsound;`.
// We’ll fully qualify to avoid namespace leaks.

class MainWindow : public juce::DocumentWindow
{
public:
    MainWindow()
        : juce::DocumentWindow(
              "SanctSound JUCE",
              juce::Desktop::getInstance().getDefaultLookAndFeel()
                  .findColour(juce::ResizableWindow::backgroundColourId),
              juce::DocumentWindow::allButtons)
    {
        setUsingNativeTitleBar(true);
        setResizable(true, true);

        setContentOwned(new sanctsound::MainComponent(), true);

        centreWithSize(getWidth(), getHeight());
        setVisible(true);
    }

    void closeButtonPressed() override
    {
        juce::JUCEApplicationBase::quit();
    }
};

class SanctSoundApp : public juce::JUCEApplication
{
public:
    const juce::String getApplicationName() override    { return "SanctSound JUCE"; }
    const juce::String getApplicationVersion() override { return "0.1.0"; }

    void initialise(const juce::String&) override
    {
        mainWindow.reset(new MainWindow());
    }

    void shutdown() override
    {
        mainWindow = nullptr;
    }

private:
    std::unique_ptr<MainWindow> mainWindow;
};

START_JUCE_APPLICATION(SanctSoundApp)
