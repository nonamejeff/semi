from __future__ import annotations
import os

# ---------------- CONFIG ----------------
PROJECT_ROOT   = "/Users/kailashpermaul/Documents/Dolphin-Audio apps/code name semi"
DEFAULT_DEST   = os.path.join(PROJECT_ROOT, "data", "raw")

AUDIO_PREFIX    = "gs://noaa-passive-bioacoustic/sanctsound/audio"
PRODUCTS_PREFIX = "gs://noaa-passive-bioacoustic/sanctsound/products/detections"

# Preferred order when multiple product artifact types exist
PREFER_PRODUCT_ORDER = (".csv", ".nc", ".json")

# Preview padding kept at 0 now that we bring a left-boundary file
WINDOW_PAD_HOURS   = 0

# Event fallback when CSV lacks duration (seconds)
EVENT_FALLBACK_SEC = 60.0    # 1 minute default

# Tiny outputs are treated as failed and removed
MIN_CLIP_BYTES     = 10_000

# Output audio format
CLIP_SR_HZ         = 48_000
CLIP_MONO          = True
CLIP_SAMPLE_FMT    = "s16"

