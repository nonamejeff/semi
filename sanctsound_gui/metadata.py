# sanctsound_gui/metadata.py
from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict

from .config import PRODUCTS_PREFIX, PROJECT_ROOT
from .shell import run_cmd

# Where the GUI and scripts read/write the cached index
DEFAULT_INDEX_PATH = os.path.join(PROJECT_ROOT, "data", "metadata_index.json")

# Canonical sanctuary / region names by 2-letter site prefix
SITE_LONG_NAMES: Dict[str, str] = {
    "ci": "Channel Islands",
    "fk": "Florida Keys",
    "gr": "Gray's Reef",
    "hi": "Hawaiian Islands Humpback Whale",
    "mb": "Monterey Bay",
    "oc": "Olympic Coast",
    "pm": "Papahānaumokuākea",
    "sb": "Stellwagen Bank",
}

# Layout we are targeting
# gs://.../sanctsound/products/detections/<site>/<group>/metadata/*.json
# gs://.../sanctsound/products/detections/<site>/<group>/data/*.csv
SITE_RE   = re.compile(r"/detections/([a-z]{2}\d{2})(?:/|$)", re.I)
GROUP_RE  = re.compile(r"/detections/([a-z]{2}\d{2})/([^/]+)/", re.I)
DEP_FROM_GROUP_RE = re.compile(r"sanctsound_[a-z]{2}\d{2}_(\d{2})", re.I)

# ---------------- low-level GCS helpers ----------------

def _gcs_read_text(url: str, timeout_s: int, verbose: bool = False) -> Optional[str]:
    """Read a small GCS object as text with a timeout; return text or None."""
    try:
        res = subprocess.run(
            ["gsutil", "cat", url],
            capture_output=True, text=True, timeout=timeout_s, check=True
        )
        return res.stdout
    except subprocess.TimeoutExpired:
        if verbose:
            print(f"    … timeout (> {timeout_s}s) {url}", flush=True)
    except Exception as e:
        if verbose:
            print(f"    … read failed {url}: {e}", flush=True)
    return None

# ---------------- JSON field extraction ----------------

KEY_ALIASES: Dict[str, List[str]] = {
    "site": ["site", "site_code", "station_code", "location_id"],
    "deployment": ["deployment", "deployment_id", "mooring", "station", "station_id", "recorder_id"],
    "location": ["location", "station_name", "site_name", "region", "subregion", "deployment_zone"],
    "lat": ["lat", "latitude", "Latitude", "LAT"],
    "lon": ["lon", "longitude", "Longitude", "LON"],
    "depth_m": ["depth", "Depth", "depth_m", "water_depth_m", "bottom_depth_m"],
    "start_utc": ["start", "start_time", "StartTime", "recording_start_utc", "deployment_start_utc", "utc_start"],
    "end_utc": ["end", "end_time", "EndTime", "recording_end_utc", "deployment_end_utc", "utc_end"],
    "sample_rate_hz": ["sample_rate", "fs", "sample_rate_hz", "Fs", "sampling_rate_hz"],
    "platform": ["platform", "moorings", "platform_name", "platform_id", "platform_type"],
    "recorder": ["recorder", "instrument", "device", "recorder_model", "hydrophone_model"],
}

def _pick(obj: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in obj:
            return obj[k]
    # shallow nested dicts
    for v in obj.values():
        if isinstance(v, dict):
            for k in keys:
                if k in v:
                    return v[k]
    return None

def _to_float(x: Any) -> Optional[float]:
    try:
        if isinstance(x, str):
            x = x.strip()
            if not x:
                return None
        return float(x)
    except Exception:
        return None

def _extract_fields_json(doc: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for canon, aliases in KEY_ALIASES.items():
        out[canon] = _pick(doc, aliases)

    # Normalize
    if out.get("site"):
        out["site"] = str(out["site"]).strip().lower()
    if out.get("deployment"):
        dep = str(out["deployment"]).strip()
        m = re.search(r"(\d{2})", dep)
        out["deployment"] = m.group(1) if m else dep
    if out.get("location"):
        out["location"] = str(out["location"]).strip()

    out["lat"] = _to_float(out.get("lat"))
    out["lon"] = _to_float(out.get("lon"))
    out["depth_m"] = _to_float(out.get("depth_m"))
    out["sample_rate_hz"] = _to_float(out.get("sample_rate_hz"))
    return out

# ---------------- discovery using object listing ----------------

def _discover_sites(verbose: bool) -> List[str]:
    """
    Find site codes either via top-level 'ls' under products or by parsing any
    object under that tree (fallback).
    """
    base = f"{PRODUCTS_PREFIX}/"
    sites: set[str] = set()

    if verbose:
        print(f"[sites] ls {base}", flush=True)
    for line in run_cmd(["gsutil", "ls", base]):
        s = line.strip()
        m = SITE_RE.search(s)
        if m:
            sites.add(m.group(1).lower())

    if not sites:
        pat = f"{PRODUCTS_PREFIX}/**"
        if verbose:
            print(f"[sites-fallback] ls -r {pat}", flush=True)
        for line in run_cmd(["gsutil", "ls", "-r", pat]):
            s = line.strip()
            if not s.startswith("gs://"):
                continue
            m = SITE_RE.search(s)
            if m:
                sites.add(m.group(1).lower())

    out = sorted(sites)
    if verbose:
        print(f"[sites] found {len(out)} → {', '.join(out)}", flush=True)
    return out

def _scan_site_objects(site: str, verbose: bool) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
    """
    For a site, list all objects recursively once and collect:
      - metadata JSON urls per <group>
      - csv counts per <group>
    """
    meta_by_group: Dict[str, List[str]] = defaultdict(list)  # type: ignore
    csv_count_by_group: Dict[str, int] = defaultdict(int)     # type: ignore

    pat = f"{PRODUCTS_PREFIX}/{site}/**"
    if verbose:
        print(f"[{site}] ls -r {pat}", flush=True)
    for line in run_cmd(["gsutil", "ls", "-r", pat]):
        s = line.strip()
        if not s.startswith("gs://"):
            continue
        m = GROUP_RE.search(s)
        if not m:
            continue
        g = m.group(2)
        if "/metadata/" in s and s.lower().endswith(".json"):
            meta_by_group[g].append(s)
        elif "/data/" in s and s.lower().endswith(".csv"):
            csv_count_by_group[g] += 1

    for g in meta_by_group:
        meta_by_group[g].sort()

    if verbose:
        total_csv = sum(csv_count_by_group.values())
        print(
            f"[{site}] groups={len(set(meta_by_group)|set(csv_count_by_group))} "
            f"(meta_json_groups={len(meta_by_group)} csv_groups={len(csv_count_by_group)} "
            f"csv_total={total_csv})",
            flush=True,
        )

    return dict(meta_by_group), dict(csv_count_by_group)

# ---------------- main index build ----------------

def build_index(
    sites: Optional[List[str]] = None,
    max_per_site: Optional[int] = None,
    verbose: bool = False,
    dry_run: bool = False,
    per_file_timeout_s: int = 10,
    max_json_per_group: Optional[int] = 3,
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """
    Build a lightweight index using a few metadata JSONs per group and CSV counts.
    Returns (index, stats).
    """
    index: Dict[str, Any] = {"by_site": {}, "sites_sorted": []}
    stats = {"sites": 0, "groups": 0, "deployments": 0, "json_scanned": 0, "csv_listed": 0}

    # Sites
    if not sites:
        sites = _discover_sites(verbose=verbose)
    else:
        sites = sorted(set(sites))
        if verbose:
            print(f"[sites] using provided list ({len(sites)}): {', '.join(sites)}", flush=True)

    if verbose:
        print(f"== Scanning {len(sites)} site(s) ==", flush=True)

    for site in sites:
        if verbose:
            print(f"\n-- Scanning site: {site} --", flush=True)
        per_site: Dict[str, Any] = {"deployments": {}, "label": site.upper()}
        index["by_site"][site] = per_site
        stats["sites"] += 1

        meta_by_group, csv_by_group = _scan_site_objects(site, verbose=verbose)

        groups = sorted(set(meta_by_group) | set(csv_by_group))
        stats["groups"] += len(groups)
        if verbose:
            print(f"[{site}] discovered groups: {len(groups)}", flush=True)

        for gi, group in enumerate(groups, 1):
            if verbose:
                print(f"[{site}][{gi}/{len(groups)}] group {group}", flush=True)

            dep = (DEP_FROM_GROUP_RE.search(group).group(1) if DEP_FROM_GROUP_RE.search(group) else "??")
            d = per_site["deployments"].setdefault(
                dep,
                {
                    "label": None,
                    "lat": None,
                    "lon": None,
                    "depth_m": None,
                    "start_utc": None,
                    "end_utc": None,
                    "sample_rate_hz": None,
                    "platform": None,
                    "recorder": None,
                    "json_urls": [],
                },
            )

            meta_urls = meta_by_group.get(group, [])
            if verbose:
                print(f"  [metadata] {len(meta_urls)} file(s)", flush=True)
            to_read = meta_urls if (max_json_per_group is None) else meta_urls[:max_json_per_group]

            for j, url in enumerate(to_read, 1):
                if dry_run:
                    print(f"    ({j}/{len(to_read)}) {url}  (dry-run)", flush=True)
                    continue
                if verbose:
                    print(f"    ({j}/{len(to_read)}) {url}", flush=True)

                text = _gcs_read_text(url, timeout_s=per_file_timeout_s, verbose=verbose)
                if not text:
                    continue
                try:
                    doc = json.loads(text)
                    f = _extract_fields_json(doc)
                except Exception as e:
                    if verbose:
                        print(f"    … JSON parse error: {e}", flush=True)
                    continue

                # Fill fields (first non-null wins)
                for k in ("lat", "lon", "depth_m", "start_utc", "end_utc",
                          "sample_rate_hz", "platform", "recorder"):
                    if d[k] is None and f.get(k) is not None:
                        d[k] = f[k]
                if not d["label"]:
                    d["label"] = f.get("location") or f"{site.upper()} — {dep}"

                d["json_urls"].append(url)
                stats["json_scanned"] += 1

                if max_per_site and stats["json_scanned"] >= max_per_site:
                    if verbose:
                        print(f"  [limit] global max_per_site reached ({max_per_site})", flush=True)
                    break

            n_csv = csv_by_group.get(group, 0)
            stats["csv_listed"] += n_csv
            if verbose:
                print(f"  [data] {n_csv} csv file(s)", flush=True)

        # Force friendly site label “Location — CODE”
        prefix = site[:2].lower()
        long_name = SITE_LONG_NAMES.get(prefix, site.upper())
        per_site["label"] = f"{long_name} — {site.upper()}"

    index["sites_sorted"] = sorted(index["by_site"].keys())
    stats["deployments"] = sum(len(v["deployments"]) for v in index["by_site"].values())

    if verbose and not dry_run:
        print(
            f"\n[done] sites={stats['sites']} groups={stats['groups']} "
            f"deployments={stats['deployments']} json_scanned={stats['json_scanned']} "
            f"csv_listed={stats['csv_listed']}",
            flush=True,
        )
    return index, stats

# ---------------- GUI helpers ----------------

def site_display_label(site_code: str, idx: Optional[Dict[str, Any]] = None) -> str:
    """
    Always show 'Location Name — CODE' using the canonical SITE_LONG_NAMES map.
    We intentionally ignore labels from the index to avoid 'CI01 — CI01'.
    """
    code = site_code.strip().lower()
    long_name = SITE_LONG_NAMES.get(code[:2], code.upper())
    return f"{long_name} — {code.upper()}"

# ---------------- persistence ----------------

def save_index(index: Dict[str, Any], path: str = DEFAULT_INDEX_PATH) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(index, f, indent=2)
    return path

def load_index(path: str = DEFAULT_INDEX_PATH) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None
