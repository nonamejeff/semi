from __future__ import annotations
import os, re, bisect, csv, tempfile
from datetime import timedelta
from typing import Iterable
from .shell import ffprobe_duration_seconds, ffmpeg_cut, ffmpeg_concat_wavs
from .timeparse import parse_audio_start_from_name
from .config import MIN_CLIP_BYTES, CLIP_SR_HZ, CLIP_MONO, CLIP_SAMPLE_FMT

def build_local_index(dest_dir: str, selected_basenames: set[str]):
    local=[]
    for n in os.listdir(dest_dir):
        if not n.lower().endswith(".flac"): continue
        if n not in selected_basenames:
            continue
        st=parse_audio_start_from_name(n)
        if not st: continue
        m = re.search(r"(SanctSound_[A-Z]{2}\d{2}_\d{2})_", n, re.I)
        fld=m.group(1).lower() if m else ""
        local.append(dict(path=os.path.join(dest_dir,n), fname=n, start_dt=st, folder=fld))
    local.sort(key=lambda x:x["start_dt"])
    # infer end_dt
    for i,h in enumerate(local):
        if i+1<len(local):
            et=local[i+1]["start_dt"]
            if et<=h["start_dt"]: et=h["start_dt"]+timedelta(seconds=1)
        else:
            dur=ffprobe_duration_seconds(h["path"])
            et=h["start_dt"]+timedelta(seconds=dur if dur and dur>1 else 3600)
        h["end_dt"]=et
    return local

def cover_and_next(local_index, ts):
    starts=[h["start_dt"] for h in local_index]
    i = bisect.bisect_right(starts, ts) - 1
    if i<0: return None, None
    cur = local_index[i]
    nxt = local_index[i+1] if (i+1)<len(local_index) else None
    return cur, nxt

def clip_windows(dest_dir: str, grp: str, mode: str, windows, selected_basenames: set[str], log_cb=print):
    # Build local index from user-selected files only
    local=build_local_index(dest_dir, selected_basenames)

    clips_dir=os.path.join(dest_dir,"clips",grp)
    os.makedirs(clips_dir, exist_ok=True)
    manifest_csv=os.path.join(clips_dir,"clips_manifest.csv")
    rows=[]; skipped=0

    for k,(s,e) in enumerate(windows,1):
        h, nxt = cover_and_next(local, s)
        if not h: skipped+=1; continue

        need_two = e > h["end_dt"]
        if not need_two:
            if h["fname"] not in selected_basenames:
                skipped+=1; continue
        else:
            if not nxt or (h["fname"] not in selected_basenames) or (nxt["fname"] not in selected_basenames):
                skipped+=1; continue

        if h["end_dt"]>=e:
            t0=(s-h["start_dt"]).total_seconds()
            dur=(e-s).total_seconds()
            if dur<=0: skipped+=1; continue
            out=f"{os.path.splitext(h['fname'])[0]}__{s.strftime('%Y%m%dT%H%M%S')}_{e.strftime('%Y%m%dT%H%M%S')}.wav"
            outp=os.path.join(clips_dir,out)
            try:
                ffmpeg_cut(h["path"], t0, dur, outp, CLIP_SR_HZ, CLIP_MONO, CLIP_SAMPLE_FMT)
                if os.path.getsize(outp) < MIN_CLIP_BYTES:
                    os.remove(outp); skipped+=1; continue
                rows.append([out, h["fname"], s.isoformat(), e.isoformat(), round(dur,3), mode])
            except Exception as ex:
                skipped+=1; log_cb(f"[WARN] ffmpeg failed: {ex}\n")
        else:
            if not nxt: skipped+=1; continue
            t0a=(s-h["start_dt"]).total_seconds()
            dura=(h["end_dt"]-s).total_seconds()
            durb=(e - nxt["start_dt"]).total_seconds()
            if dura<=0 or durb<=0: skipped+=1; continue

            tmpa=tempfile.mkstemp(suffix=".wav")[1]
            tmpb=tempfile.mkstemp(suffix=".wav")[1]
            out=f"{os.path.splitext(h['fname'])[0]}__{s.strftime('%Y%m%dT%H%M%S')}_{e.strftime('%Y%m%dT%H%M%S')}.wav"
            outp=os.path.join(clips_dir,out)
            try:
                ffmpeg_cut(h["path"], t0a, dura, tmpa, CLIP_SR_HZ, CLIP_MONO, CLIP_SAMPLE_FMT)
                ffmpeg_cut(nxt["path"], 0.0, durb, tmpb, CLIP_SR_HZ, CLIP_MONO, CLIP_SAMPLE_FMT)
                ffmpeg_concat_wavs(tmpa, tmpb, outp)
                if os.path.getsize(outp) < MIN_CLIP_BYTES:
                    os.remove(outp); skipped+=1; continue
                rows.append([out, f"{h['fname']} + {nxt['fname']}", s.isoformat(), e.isoformat(), round(dura+durb,3), mode])
            except Exception as ex:
                skipped+=1; log_cb(f"[WARN] stitching failed: {ex}\n")
            finally:
                for p in (tmpa,tmpb):
                    try: os.remove(p)
                    except: pass

        if k%100==0: log_cb(f"  cut {k}/{len(windows)} windows …\n")

    with open(manifest_csv,"w",newline="") as f:
        w=csv.writer(f); w.writerow(["clip_wav","source_flac(s)","start_utc","end_utc","duration_sec","mode"]); w.writerows(rows)
    with open(os.path.join(clips_dir,"clips_summary.txt"),"w") as f:
        f.write(f"Windows: {len(windows)} | Clips: {len(rows)} | Skipped: {skipped} | Mode: {mode}\nDir: {clips_dir}\n")

    log_cb(f"Clips → {clips_dir} | written {len(rows)}, skipped {skipped}\n")

