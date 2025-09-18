// Source/Main.cpp
#include <juce_gui_extra/juce_gui_extra.h>
#include "MainComponent.h"

class MainWindow : public juce::DocumentWindow {
public:
    MainWindow() :
        DocumentWindow("SanctSound JUCE",
                       juce::Desktop::getInstance().getDefaultLookAndFeel()
                           .findColour(juce::ResizableWindow::backgroundColourId),
                       DocumentWindow::allButtons)
    {
        setUsingNativeTitleBar(true);
        setResizable(true, true);
        setContentOwned(new MainComponent(), true);
        centreWithSize(getWidth(), getHeight());
        setVisible(true);
    }
    void closeButtonPressed() override { juce::JUCEApplicationBase::quit(); }
};

class SanctSoundApp : public juce::JUCEApplication {
public:
    const juce::String getApplicationName()  override { return "SanctSound JUCE"; }
    const juce::String getApplicationVersion() override { return "0.1.0"; }
    void initialise (const juce::String&) override { window.reset(new MainWindow()); }
    void shutdown() override { window = nullptr; }
private:
    std::unique_ptr<MainWindow> window;
};

START_JUCE_APPLICATION(SanctSoundApp)

