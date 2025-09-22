#include <juce_gui_extra/juce_gui_extra.h>
#include "MainComponent.h"

namespace
{
juce::String chooseUIFont()
{
    juce::StringArray preferred;

#if JUCE_MAC
    preferred.add("SF Pro Text");
    preferred.add(".SF NS Text");
    preferred.add(".AppleSystemUIFont");
    preferred.add("Helvetica Neue");
    preferred.add("Avenir Next");
    preferred.add("Helvetica");
    preferred.add("Arial");
#elif JUCE_WINDOWS
    preferred.add("Segoe UI");
    preferred.add("Calibri");
    preferred.add("Arial");
    preferred.add("Helvetica Neue");
#else
    preferred.add("Noto Sans");
    preferred.add("DejaVu Sans");
    preferred.add("Liberation Sans");
    preferred.add("Helvetica Neue");
    preferred.add("Arial");
#endif

    auto available   = juce::Font::findAllTypefaceNames();
    auto defaultName = juce::Font::getDefaultSansSerifFontName();

    for (auto& name : preferred)
        if (available.contains(name))
            return name;

    if (available.contains(defaultName))
        return defaultName;

    if (! available.isEmpty())
        return available[0];

    return defaultName;
}
} // namespace

// NOTE: Your MainComponent is inside namespace `sanctsound`.
// Either qualify it here, or `using namespace sanctsound;`.
// We'll fully qualify to avoid namespace leaks.

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
        auto uiFont = chooseUIFont();
        juce::LookAndFeel::getDefaultLookAndFeel().setDefaultSansSerifTypefaceName(uiFont);
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
