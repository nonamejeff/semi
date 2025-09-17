from __future__ import annotations
import os, re
from datetime import datetime, timezone

FNAME_TS_RE = re.compile(r"_(?:(\d{8}T\d{6}Z)|(\d{12}))\.(?:flac|wav)$", re.I)
SET_TO_FOLDER_RE = re.compile(r"(sanctsound_[a-z]{2}\d{2}_\d{2})", re.I)

def parse_audio_start_from_name(name: str):
    m = FNAME_TS_RE.search(name)
    if not m: return None
    if m.group(1):  # YYYYMMDDThhmmssZ
        dt = datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ")
        return dt.replace(tzinfo=timezone.utc)
    s = m.group(2)  # YYMMDDhhmmss
    yy = int(s[0:2]); year = 2000 + yy if yy <= 69 else 1900 + yy
    return datetime(year, int(s[2:4]), int(s[4:6]),
                    int(s[6:8]), int(s[8:10]), int(s[10:12]),
                    tzinfo=timezone.utc)

def parse_audio_start_from_url(url: str):
    return parse_audio_start_from_name(os.path.basename(url))

def folder_from_set(set_name: str) -> str | None:
    m = SET_TO_FOLDER_RE.search(set_name)
    return m.group(1).lower() if m else None

