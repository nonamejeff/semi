# sanctsound_gui/products.py
from __future__ import annotations

import os
import re
from typing import Callable, Dict, List, Optional

from .config import PRODUCTS_PREFIX
from .shell import run_cmd

# preferred product file type order for preview/build steps
PREFER_PRODUCT_ORDER = (".csv", ".nc", ".json")

_GROUP_RE_TMPL = r"/detections/{site}/([^/]+)/"

def _log(cb: Optional[Callable[[str], None]], msg: str) -> None:
    if cb:
        cb(msg if msg.endswith("\n") else msg + "\n")


def list_product_groups(site: str, tag: str, log_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Dict]:
    """
    Scan products for a given site and tag and return a mapping:
        {
          "<group>": {
              "paths": [gs://...csv|nc|json, ...],
              "exts": {".csv": N, ".nc": M, ".json": K}
          },
          ...
        }
    We look under:
        gs://.../sanctsound/products/detections/<site>/**/*{tag}*.{csv,nc,json}
    """
    globs = [
        f"{PRODUCTS_PREFIX}/{site}/**/*{tag}*.csv",
        f"{PRODUCTS_PREFIX}/{site}/**/*{tag}*.nc",
        f"{PRODUCTS_PREFIX}/{site}/**/*{tag}*.json",
    ]
    groups: Dict[str, Dict[str, object]] = {}
    grp_re = re.compile(_GROUP_RE_TMPL.format(site=re.escape(site)), re.I)

    for g in globs:
        _log(log_cb, f"[ls] {g}")
        for line in run_cmd(["gsutil", "ls", "-r", g]):
            url = line.strip()
            if not (url.startswith("gs://") and not url.endswith("/")):
                continue

            # group name is the directory directly under <site>/
            m = grp_re.search(url)
            if m:
                grp = m.group(1)
            else:
                # fallback: filename stem
                grp = re.sub(r"\..+$", "", os.path.basename(url))

            d = groups.setdefault(grp, {"paths": [], "exts": {}})

            # record path
            d["paths"].append(url)  # type: ignore[index]

            # extension counts
            ext = os.path.splitext(url)[1].lower()
            exts: Dict[str, int] = d["exts"]  # type: ignore[assignment]
            exts[ext] = exts.get(ext, 0) + 1  # type: ignore[index]

    # sort groups by name for stable UI
    return dict(sorted(groups.items(), key=lambda kv: kv[0]))


def choose_best_file(paths: List[str]) -> List[str]:
    """
    From a list of product file urls (mixed csv/nc/json), keep only the
    preferred type in priority order PREFER_PRODUCT_ORDER. If none match,
    return the original list.
    """
    buckets = {ext: [] for ext in PREFER_PRODUCT_ORDER}
    for p in paths:
        ext = os.path.splitext(p)[1].lower()
        if ext in buckets:
            buckets[ext].append(p)
    for ext in PREFER_PRODUCT_ORDER:
        if buckets[ext]:
            return buckets[ext]
    return paths
