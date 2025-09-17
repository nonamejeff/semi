from __future__ import annotations
from .shell import which
from .gui.app import App

def main():
    print("Loaded sanctsound_gui — event duration from CSV (else 60s), strict selected-file clipping")
    if which("gsutil") is None:
        print("WARNING: gsutil not found. Install Google Cloud SDK.")
    if which("ffmpeg") is None:
        print("WARNING: ffmpeg not found. Install via Homebrew: brew install ffmpeg")
    app=App(); app.mainloop()

if __name__ == "__main__":
    main()

