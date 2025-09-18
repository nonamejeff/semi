from __future__ import annotations
import os, bisect
from datetime import timedelta
from typing import List, Tuple
from .shell import run_cmd
from .config import AUDIO_PREFIX, WINDOW_PAD_HOURS
from .timeparse import parse_audio_start_from_name, parse_audio_start_from_url

def list_site_deployments(site: str) -> list[str]:
    base = f"{AUDIO_PREFIX}/{site}/"
    folders = []
    for line in run_cmd(["gsutil", "ls", base], ok_returncodes=(0, 1)):
        if line.strip().endswith("/"):
            name = line.strip().split("/")[-2]
            if name.startswith("sanctsound_"):
                folders.append(name)
    return sorted(folders)

def build_hours_from_rows(rows):
    hours=[]; n=len(rows)
    for i,(url,fname,st,folder) in enumerate(rows):
        if i+1<n and rows[i+1][3]==folder:
            et=rows[i+1][2]
            if et<=st: et=st+timedelta(seconds=1)
        else:
            et=st+timedelta(hours=1)
        hours.append(dict(url=url,fname=fname,start_dt=st,end_dt=et,folder=folder))
    return hours

def list_audio_files_in_folder(site: str, folder: str, tmin, tmax):
    pat = f"{AUDIO_PREFIX}/{site}/{folder}/audio/*.flac"
    rows = []
    left_candidate = None
    for line in run_cmd(["gsutil", "ls", "-r", pat], ok_returncodes=(0, 1)):
        if not (line.startswith("gs://") and line.lower().endswith(".flac")):
            continue
        url = line.strip()
        fname = os.path.basename(url)
        st = parse_audio_start_from_name(fname)
        if not st: continue
        if tmin and st < tmin:
            if (left_candidate is None) or (st > left_candidate[2]):
                left_candidate = (url, fname, st, folder)
            continue
        if tmax and st > tmax:
            continue
        rows.append((url, fname, st, folder))
    if left_candidate:
        if not tmin or (tmin - left_candidate[2]) <= timedelta(hours=6):
            rows.append(left_candidate)
    rows.sort(key=lambda r: r[2])
    return build_hours_from_rows(rows)

def list_audio_files_across(site: str, pref_first_folder: str | None, tmin, tmax, log_cb=None):
    all_folders = list_site_deployments(site)
    ordered = ([pref_first_folder] if pref_first_folder else []) + [f for f in all_folders if f != pref_first_folder]
    all_files = []
    for folder in ordered:
        files = list_audio_files_in_folder(site, folder, tmin, tmax)
        all_files.extend(files)
    all_files.sort(key=lambda x: (x["start_dt"], x["folder"]))
    return all_files

def minimal_union_for_windows(files, windows):
    starts=[f["start_dt"] for f in files]
    def pick(s,e):
        i = bisect.bisect_right(starts, s) - 1
        if i<0: return []
        first = files[i]
        if first["end_dt"] >= e: return [first]
        if i+1 < len(files): return [first, files[i+1]]
        return [first]
    urls=set(); names=set(); rows=[]
    for s,e in windows:
        chosen=pick(s,e)
        fn=[c["fname"] for c in chosen]
        fu=[c["url"] for c in chosen]
        rows.append([s.isoformat(), e.isoformat(), ";".join(fn), ";".join(fu)])
        for c in chosen: urls.add(c["url"]); names.add(c["fname"])
    return sorted(urls), sorted(names), rows

