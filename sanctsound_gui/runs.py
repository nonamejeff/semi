# sanctsound_gui/runs.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import bisect

# ---------------------------------------------------------------------------
# Utilities for building run windows and computing a minimal audio-file union
# ---------------------------------------------------------------------------

def group_consecutive(points: List[datetime], step: timedelta) -> List[Tuple[datetime, datetime]]:
    """
    Given sorted datetimes at a fixed cadence 'step', return merged [start,end)
    runs for each consecutive block.
    """
    if not points:
        return []
    runs: List[Tuple[datetime, datetime]] = []
    start = points[0]
    prev = points[0]
    for p in points[1:]:
        if p - prev == step:
            prev = p
            continue
        runs.append((start, prev + step))
        start = p
        prev = p
    runs.append((start, prev + step))
    return runs


def hours_from_runs(runs: List[Tuple[datetime, datetime]]) -> List[datetime]:
    """Expand [start,end) hour runs back into hour-start timestamps."""
    out: List[datetime] = []
    step = timedelta(hours=1)
    for s, e in runs:
        h = s
        while h < e:
            out.append(h)
            h += step
    return out


def days_from_runs(runs: List[Tuple[datetime, datetime]]) -> List[datetime]:
    """Expand [start,end) day runs back into day-start timestamps."""
    out: List[datetime] = []
    step = timedelta(days=1)
    for s, e in runs:
        d = s
        while d < e:
            out.append(d)
            d += step
    return out


def minimal_union_for_windows(
    files: List[Dict[str, Any]],
    windows: List[Tuple[datetime, datetime]],
) -> Tuple[List[str], List[str], List[List[str]]]:
    """
    Pick the minimal set of raw audio files to cover each requested [s,e) window.
    Assumes `files` are sorted by start time and each has:
        - 'start_dt': datetime (UTC)
        - 'end_dt'  : datetime (UTC)
        - 'url'     : gs://...
        - 'fname'   : basename
        - 'folder'  : deployment folder (optional)
    Returns:
        urls_sorted, names_sorted, mapping_rows
    where mapping_rows are CSV-friendly rows:
        [start_iso, end_iso, "fname1;fname2", "url1;url2"]
    """
    if not files or not windows:
        return [], [], []

    # Ensure sorted by start time
    files = sorted(files, key=lambda f: f["start_dt"])
    starts = [f["start_dt"] for f in files]

    def pick_window(s: datetime, e: datetime):
        # choose the file starting at or immediately before s
        i = bisect.bisect_right(starts, s) - 1
        if i < 0:
            return []
        first = files[i]
        # if first fully covers window, one file is enough
        if first["end_dt"] >= e:
            return [first]
        # otherwise we’ll need the next adjacent file as well (hourly layout)
        if i + 1 < len(files):
            return [first, files[i + 1]]
        return [first]

    urls, names, rows = set(), set(), []
    for s, e in windows:
        chosen = pick_window(s, e)
        fnames = [c["fname"] for c in chosen]
        furls = [c["url"] for c in chosen]
        rows.append([s.isoformat(), e.isoformat(), ";".join(fnames), ";".join(furls)])
        for c in chosen:
            urls.add(c["url"])
            names.add(c["fname"])

    urls_sorted = sorted(urls)
    names_sorted = sorted(names)
    return urls_sorted, names_sorted, rows
