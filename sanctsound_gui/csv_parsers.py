from __future__ import annotations
from datetime import timezone, timedelta
from typing import List, Tuple
import pandas as pd
import os
from .config import EVENT_FALLBACK_SEC

def parse_presence_hours_from_csv(local_csv: str):
    df = pd.read_csv(local_csv)
    hour_col=None
    for c in df.columns:
        try:
            s = pd.to_datetime(df[c], utc=True, errors="raise")
            if s.notna().sum() >= max(10, int(0.1*len(df))):
                hour_col=c; break
        except: pass
    present_col=None
    for c in df.columns:
        if c==hour_col: continue
        try:
            vals = pd.to_numeric(df[c], errors="coerce").dropna()
        except: continue
        if not len(vals): continue
        vc = vals.round().astype(int).value_counts().to_dict()
        if set(vc).issubset({0,1}) and 1 in vc:
            present_col=c; break
    if hour_col is None or present_col is None:
        raise RuntimeError(f"Could not detect hour/presence columns in {os.path.basename(local_csv)}")

    hours=[]
    s_hour = pd.to_datetime(df[hour_col], utc=True, errors="coerce")
    for h,flag in zip(s_hour, df[present_col]):
        try: pres=int(round(float(flag)))
        except: continue
        if pd.isna(h) or pres!=1: continue
        h0 = h.to_pydatetime().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        hours.append(h0)
    return sorted(set(hours))

def parse_presence_days_from_csv(local_csv: str):
    df = pd.read_csv(local_csv)
    dt_col=None
    for c in df.columns:
        try:
            s = pd.to_datetime(df[c], utc=True, errors="raise")
            if s.notna().sum() >= max(5, int(0.05*len(df))):
                dt_col=c; break
        except: pass
    if dt_col is None:
        raise RuntimeError(f"Could not detect date/datetime column in {os.path.basename(local_csv)}")

    present_col=None
    for c in df.columns:
        if c==dt_col: continue
        try:
            vals = pd.to_numeric(df[c], errors="coerce").dropna()
        except: continue
        if not len(vals): continue
        vc = vals.round().astype(int).value_counts().to_dict()
        if set(vc).issubset({0,1}) and 1 in vc:
            present_col=c; break
    if present_col is None:
        raise RuntimeError(f"Could not detect presence (0/1) column in {os.path.basename(local_csv)}")

    days=set()
    s_dt = pd.to_datetime(df[dt_col], utc=True, errors="coerce")
    for d,flag in zip(s_dt, df[present_col]):
        try: pres=int(round(float(flag)))
        except: continue
        if pd.isna(d) or pres!=1: continue
        d0 = d.to_pydatetime().astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        days.add(d0)
    return sorted(days)

def parse_events_from_csv(local_csv: str):
    """Return list of (start_dt, end_dt) events.
    If CSV has explicit end or duration, use it; else fallback to 60s.
    """
    df = pd.read_csv(local_csv)
    dt_cols = []
    for c in df.columns:
        try:
            s = pd.to_datetime(df[c], utc=True, errors="raise")
            if s.notna().sum() >= max(5, int(0.05 * len(df))):
                dt_cols.append(c)
        except:
            pass
    if not dt_cols:
        raise RuntimeError(f"No usable datetime column in {os.path.basename(local_csv)}")
    start_col = dt_cols[0]
    end_col = next((c for c in dt_cols[1:] if "end" in c.lower()), None)
    dur_col = next((c for c in df.columns if any(k in c.lower() for k in ["duration","dur","length"])), None)

    starts = pd.to_datetime(df[start_col], utc=True, errors="coerce")
    events = []

    if end_col:
        ends = pd.to_datetime(df[end_col], utc=True, errors="coerce")
        for st, et in zip(starts, ends):
            if pd.isna(st) or pd.isna(et): continue
            s = st.to_pydatetime().replace(tzinfo=timezone.utc)
            e = et.to_pydatetime().replace(tzinfo=timezone.utc)
            if e <= s: e = s + timedelta(seconds=EVENT_FALLBACK_SEC)
            events.append((s, e))
    else:
        durs = None
        if dur_col is not None:
            try:
                durs = pd.to_numeric(df[dur_col], errors="coerce")
            except:
                durs = None
        for i, st in enumerate(starts):
            if st is pd.NaT: continue
            s = st.to_pydatetime().replace(tzinfo=timezone.utc)
            if durs is not None and i < len(durs) and not pd.isna(durs.iloc[i]) and float(durs.iloc[i]) > 0:
                sec = float(durs.iloc[i])
            else:
                sec = EVENT_FALLBACK_SEC
            e = s + timedelta(seconds=sec)
            events.append((s, e))

    events = sorted(set(events), key=lambda x: x[0])
    return events

