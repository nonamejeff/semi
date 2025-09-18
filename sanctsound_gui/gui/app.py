# sanctsound_gui/gui/app.py
from __future__ import annotations

import os, re, csv, bisect, tempfile, subprocess, threading, json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import tkinter.font as tkfont

# ----------------- project imports -----------------
from ..config import DEFAULT_DEST, PRODUCTS_PREFIX
from ..shell import run_cmd
from ..products import list_product_groups, choose_best_file
from ..csv_parsers import (
    parse_presence_hours_from_csv,
    parse_presence_days_from_csv,
    parse_events_from_csv,
)
from ..audio_listing import list_audio_files_across
from ..runs import (
    group_consecutive,
    hours_from_runs,
    days_from_runs,
    minimal_union_for_windows,
)
from ..timeparse import folder_from_set, parse_audio_start_from_name
from .. import sites as mod_sites

# ----------------- constants -----------------
EVENT_FALLBACK_SEC = 60.0
MIN_CLIP_BYTES     = 10_000

# ----------------- FF utilities (local so clip works) -----------------
def ffprobe_duration_seconds(path):
    try:
        out = subprocess.check_output(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1", path],
            text=True).strip()
        return float(out)
    except Exception:
        return None

def ffmpeg_cut(src, start_sec, dur_sec, out_wav, sr_out=48000):
    dur = max(0.01, float(dur_sec))
    argv = ["ffmpeg","-y","-loglevel","error",
            "-ss",f"{start_sec:.3f}","-t",f"{dur:.3f}",
            "-i",src,"-ac","1","-ar",str(sr_out),"-sample_fmt","s16",out_wav]
    subprocess.check_call(argv)

def ffmpeg_concat_wavs(wav1, wav2, out_wav):
    listfile = tempfile.mkstemp(suffix=".txt")[1]
    with open(listfile,"w") as f:
        f.write(f"file '{wav1}'\n")
        f.write(f"file '{wav2}'\n")
    argv = ["ffmpeg","-y","-loglevel","error","-f","concat","-safe","0",
            "-i",listfile,"-c","copy",out_wav]
    subprocess.check_call(argv)
    try: os.remove(listfile)
    except: pass

# ----------------- UI palette -----------------
PALETTE = {
    "bg":     "#f7f1e6",
    "panel":  "#f2ebdf",
    "ink":    "#2c2c2c",
    "muted":  "#6a6a6a",
    "accent": "#a93c2e",
    "steel":  "#b9b9b9",
}

def _pick_font(root, candidates, size, weight="normal"):
    fams=set(tkfont.families(root))
    for f in candidates:
        if f in fams: return (f,size,weight) if weight!="normal" else (f,size)
    return ("Helvetica",size,weight) if weight!="normal" else ("Helvetica",size)

def build_fonts(root):
    return {
        "ui":   _pick_font(root, ["SF Pro Text","Helvetica Neue","Avenir Next"], 13),
        "mono": ("Menlo", 11),
        "h1":   _pick_font(root, ["SF Pro Display","Helvetica Neue","Avenir Next"], 15, "bold"),
        "card": _pick_font(root, ["SF Pro Text","Helvetica Neue","Avenir Next"], 12, "bold"),
        "small":_pick_font(root, ["SF Pro Text","Helvetica Neue"], 11),
    }

def apply_styles(root, F):
    style = ttk.Style(root)
    try: style.theme_use("clam")
    except Exception: pass
    root.configure(bg=PALETTE["bg"])

    style.configure(".", background=PALETTE["bg"], foreground=PALETTE["ink"], font=F["ui"])
    style.configure("N.TFrame", background=PALETTE["bg"])
    style.configure("N.Panel.TFrame", background=PALETTE["panel"])
    style.configure("N.TLabelframe", background=PALETTE["panel"])
    style.configure("N.TLabelframe.Label", background=PALETTE["panel"], foreground=PALETTE["muted"], font=F["card"])
    style.configure("N.TLabel", background=PALETTE["bg"], foreground=PALETTE["ink"])
    style.configure("N.Muted.TLabel", background=PALETTE["bg"], foreground=PALETTE["muted"], font=F["small"])
    style.configure("N.TButton", background=PALETTE["panel"], foreground=PALETTE["ink"], borderwidth=1)
    style.map("N.TButton", background=[("active","#e9e1d2")])
    style.configure("N.TCheckbutton", background=PALETTE["panel"], foreground=PALETTE["ink"])
    style.configure("N.TNotebook", background=PALETTE["panel"])
    style.configure("N.TNotebook.Tab", background=PALETTE["panel"], padding=(10,6))
    style.map("N.TNotebook.Tab", background=[("selected","#e9e1d2")])

# ----------------- site labels/codes -----------------
SITE_LABELS: List[str] = []
SITE_BY_LABEL: Dict[str,str] = {}

def get_site_labels()->List[str]:
    global SITE_LABELS, SITE_BY_LABEL
    try:
        labels = mod_sites.site_labels_for_dropdown()
        SITE_BY_LABEL = {lab: mod_sites.code_for_label(lab) for lab in labels}
        SITE_LABELS = labels
        return labels
    except Exception:
        # worst-case fallback
        codes = [
            "ci01","ci02","ci03","ci04","ci05","fk01","fk02","fk03","fk04",
            "gr01","gr02","gr03","hi01","hi03","hi04","hi05","hi06",
            "mb01","mb02","mb03","oc01","oc02","oc03","oc04",
            "pm01","pm02","pm05","sb01","sb02","sb03",
        ]
        labels = [f"{c.upper()} — {c.upper()}" for c in codes]
        SITE_BY_LABEL = dict(zip(labels,codes))
        SITE_LABELS = labels
        return labels

def label_to_code(label: str) -> str:
    if label in SITE_BY_LABEL: return SITE_BY_LABEL[label]
    try: return mod_sites.code_for_label(label)
    except Exception:
        if "—" in label: return label.split("—")[-1].strip().lower()
        if "-" in label: return label.split("-")[-1].strip().lower()
        return label.strip().lower()

# ----------------- metadata helpers -----------------
def _gcs_ls_metadata_jsons(site: str, group: str, log_cb) -> List[str]:
    pat = f"{PRODUCTS_PREFIX}/{site}/{group}/metadata/*.json"
    out=[]
    for line in run_cmd(["gsutil","ls","-r",pat], ok_returncodes=(0, 1)):
        s=line.strip()
        if s.startswith("gs://") and s.lower().endswith(".json"):
            out.append(s)
    out.sort()
    log_cb(f"[meta] {len(out)} metadata json\n")
    return out

def _gcs_cat(url: str) -> Optional[str]:
    try:
        buf=[]
        for line in run_cmd(["gsutil","cat",url]): buf.append(line+"\n")
        return "".join(buf)
    except Exception:
        return None

def _safe_json(text: str) -> Optional[Dict[str, Any]]:
    try: return json.loads(text)
    except Exception: return None

def _find_first(d: Any, keys: List[str]) -> Optional[Any]:
    keyset = {k.lower() for k in keys}
    if isinstance(d, dict):
        for k,v in d.items():
            if k.lower() in keyset: return v
        for v in d.values():
            hit=_find_first(v, keys)
            if hit is not None: return hit
    elif isinstance(d, list):
        for v in d:
            hit=_find_first(v, keys)
            if hit is not None: return hit
    return None

def build_summary_from_json(meta: Dict[str,Any]) -> Dict[str,str]:
    def pick(*names):
        v=_find_first(meta, list(names))
        if isinstance(v,(dict,list)): return ""
        return "" if v is None else str(v)

    site=pick("SITE_NAME","SITE"); dep=pick("DEPLOYMENT_NAME","DEPLOYMENT")
    plat=pick("PLATFORM_NAME","PLATFORM"); rec=pick("RECORDER","MODEL","INSTRUMENT_MODEL")
    lat=pick("LATITUDE","LAT"); lon=pick("LONGITUDE","LON"); depth=pick("DEPTH","WATER_DEPTH","SENSOR_DEPTH")
    coord=", ".join(p for p in (lat,lon,f"{depth} m" if depth else "") if p)
    st=pick("START_TIME","START"); en=pick("END_TIME","END")
    sr=pick("SAMPLE_RATE","SAMPLE_RATE_HZ","SAMPLING_RATE"); note=pick("LOCATION_NOTE","COMMENTS")
    return {"site":site,"deployment":dep,"platform":plat,"recorder":rec,"coord":coord,"start":st,"end":en,"samplerate":sr,"locnote":note}

# ----------------- cache -----------------
@dataclass
class PreviewCache:
    mode: str
    windows: List[Tuple[datetime, datetime]]

# ----------------- the app -----------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SanctSound — Minimal-files preview → Select → Download → Clip")
        self.geometry("1420x980")

        self.F = build_fonts(self)
        apply_styles(self, self.F)

        # state
        self.dest_dir = DEFAULT_DEST
        self.only_long_runs = tk.IntVar(value=0)
        self.site_label = tk.StringVar()
        self.tag = tk.StringVar(value="dolphin")

        self.groups: Dict[str, Dict[str, Any]] = {}
        self.group_vars: Dict[str, tk.IntVar] = {}
        self.mode: Dict[str, str] = {}
        self.preview_cache: Dict[str, PreviewCache] = {}
        self.file_vars: List[Tuple[tk.StringVar, tk.IntVar]] = []

        # ui elements
        self._log_window: Optional[tk.Toplevel] = None
        self.txt_log: Optional[tk.Text] = None

        # one-at-a-time metadata panel under sets
        self._open_meta_grp: Optional[str] = None
        self._open_meta_frame: Optional[tk.Frame] = None
        self._open_meta_raw: Optional[tk.Text] = None
        self._open_meta_cards: Dict[str, tk.StringVar] = {}

        self._build()

        labels = get_site_labels()
        self.cmb_site["values"] = labels
        if labels: self.site_label.set(labels[0])

    # --------- UI ----------
    def _build(self):
        pad={"padx":10,"pady":8}
        top=ttk.Frame(self, style="N.TFrame"); top.pack(fill="x", **pad)

        ttk.Label(top, text="Site:", style="N.TLabel").grid(row=0, column=0, sticky="w")
        self.cmb_site = ttk.Combobox(top, textvariable=self.site_label, width=36, state="readonly")
        self.cmb_site.grid(row=0, column=1, sticky="w")
        ttk.Button(top, text="↻", width=3, style="N.TButton", command=self.on_refresh_sites)\
            .grid(row=0, column=2, sticky="w", padx=(6,16))

        ttk.Label(top, text="Tag:", style="N.TLabel").grid(row=0, column=3, sticky="e")
        ttk.Entry(top, textvariable=self.tag, width=18).grid(row=0, column=4, sticky="w")

        ttk.Button(top, text="List sets", style="N.TButton", command=self.on_list)\
            .grid(row=0, column=5, sticky="w", padx=(16,0))
        ttk.Checkbutton(top, text="Only runs ≥ 2h (hour/day modes)", variable=self.only_long_runs, style="N.TCheckbutton")\
            .grid(row=0, column=6, sticky="w", padx=(16,0))

        ttk.Label(top, text="Destination:", style="N.TLabel").grid(row=1, column=0, sticky="w")
        self.var_dest=tk.StringVar(value=self.dest_dir)
        ttk.Label(top, textvariable=self.var_dest, style="N.Muted.TLabel").grid(row=1, column=1, columnspan=4, sticky="we", padx=(6,10))
        ttk.Button(top, text="Choose…", style="N.TButton", command=self.on_choose_dest).grid(row=1, column=5, sticky="e")
        ttk.Button(top, text="Log…", style="N.TButton", command=self.toggle_log_window).grid(row=1, column=6, sticky="w", padx=(16,0))

        # Columns: LEFT (sets + metadata under) | MIDDLE (preview/select files)
        body = ttk.Panedwindow(self, orient="horizontal", style="N.TFrame"); body.pack(fill="both", expand=True, **pad)

        # LEFT – sets + embedded metadata
        self.frm_sets = ttk.Labelframe(body, text="SETS — CHECK TO PROCESS   (click ⓘ to view metadata below the set)", style="N.TLabelframe")
        body.add(self.frm_sets, weight=2)

        self.canvas_sets = tk.Canvas(self.frm_sets, bg=PALETTE["panel"], highlightthickness=0)
        self.scroll_sets = ttk.Scrollbar(self.frm_sets, orient="vertical", command=self.canvas_sets.yview)
        self.frame_sets = ttk.Frame(self.canvas_sets, style="N.Panel.TFrame")
        self.frame_sets.bind("<Configure>", lambda e: self.canvas_sets.configure(scrollregion=self.canvas_sets.bbox("all")))
        self.canvas_sets.create_window((0,0), window=self.frame_sets, anchor="nw")
        self.canvas_sets.configure(yscrollcommand=self.scroll_sets.set)
        self.canvas_sets.pack(side="left", fill="both", expand=True)
        self.scroll_sets.pack(side="right", fill="y")

        # MIDDLE – preview/select files (unchanged position)
        self.frm_preview = ttk.Labelframe(body, text="PREVIEW → SELECT FILES TO DOWNLOAD", style="N.TLabelframe")
        body.add(self.frm_preview, weight=3)

        self.lbl_summary = ttk.Label(self.frm_preview, text="", style="N.Muted.TLabel")
        self.lbl_summary.pack(anchor="w", padx=8, pady=(8,0))

        self.txt_runs = tk.Text(self.frm_preview, height=12, font=self.F["mono"], bg=PALETTE["panel"],
                                fg=PALETTE["ink"], highlightthickness=0, relief="flat")
        self.txt_runs.pack(fill="x", expand=False, padx=8, pady=8)

        self.canvas_files = tk.Canvas(self.frm_preview, height=320, bg=PALETTE["panel"], highlightthickness=0)
        self.hscroll_files = ttk.Scrollbar(self.frm_preview, orient="horizontal", command=self.canvas_files.xview)
        self.vscroll_files = ttk.Scrollbar(self.frm_preview, orient="vertical", command=self.canvas_files.yview)
        self.frame_files = ttk.Frame(self.canvas_files, style="N.Panel.TFrame")
        self.frame_files.bind("<Configure>", lambda e: self.canvas_files.configure(scrollregion=self.canvas_files.bbox("all")))
        self.canvas_files.create_window((0,0), window=self.frame_files, anchor="nw")
        self.canvas_files.configure(yscrollcommand=self.vscroll_files.set, xscrollcommand=self.hscroll_files.set)
        self.canvas_files.pack(side="left", fill="both", expand=True, padx=(8,0), pady=(0,8))
        self.vscroll_files.pack(side="right", fill="y"); self.hscroll_files.pack(fill="x")

        selbar=ttk.Frame(self.frm_preview, style="N.TFrame"); selbar.pack(fill="x", padx=8, pady=8)
        ttk.Button(selbar, text="Select all", style="N.TButton", command=lambda:self._select_files(True)).pack(side="left")
        ttk.Button(selbar, text="Select none", style="N.TButton", command=lambda:self._select_files(False)).pack(side="left", padx=6)
        self.var_filecount=tk.StringVar(value="0 files selected")
        ttk.Label(selbar, textvariable=self.var_filecount, style="N.Muted.TLabel").pack(side="right")

        # Bottom bar
        bottom=ttk.Frame(self, style="N.TFrame"); bottom.pack(fill="x", **pad)
        self.btn_preview=ttk.Button(bottom, text="Preview runs & files", style="N.TButton", command=self.on_preview, state="disabled")
        self.btn_preview.pack(side="left")
        self.btn_download=ttk.Button(bottom, text="Download checked files", style="N.TButton", command=self.on_download, state="disabled")
        self.btn_download.pack(side="left", padx=8)
        self.btn_clip=ttk.Button(bottom, text="Clip selected", style="N.TButton", command=self.on_clip, state="disabled")
        self.btn_clip.pack(side="left", padx=8)
        self.progress=ttk.Progressbar(bottom, mode="indeterminate", length=380)
        self.progress.pack(side="left", padx=8)
        self.var_status=tk.StringVar(value="Ready")
        ttk.Label(self, textvariable=self.var_status, anchor="w", style="N.Muted.TLabel").pack(fill="x", side="bottom")

    # ---------- logging ----------
    def toggle_log_window(self):
        if self._log_window and tk.Toplevel.winfo_exists(self._log_window):
            self._log_window.destroy(); self._log_window=None; self.txt_log=None; return
        self._log_window=tk.Toplevel(self); self._log_window.title("Log / Progress")
        self._log_window.geometry("560x640"); self._log_window.configure(bg=PALETTE["panel"])
        self.txt_log=tk.Text(self._log_window, height=30, font=self.F["mono"], bg=PALETTE["panel"], fg=PALETTE["ink"],
                             highlightthickness=0, relief="flat")
        self.txt_log.pack(fill="both", expand=True, padx=8, pady=8)

    def log(self, s:str):
        print(s, end="" if s.endswith("\n") else "\n")
        if self.txt_log and self._log_window and tk.Toplevel.winfo_exists(self._log_window):
            self.txt_log.insert("end", s); self.txt_log.see("end")

    def set_status(self, s:str): self.var_status.set(s)

    # ---------- actions ----------
    def on_refresh_sites(self):
        labels=get_site_labels(); self.cmb_site["values"]=labels
        if labels and self.site_label.get() not in labels: self.site_label.set(labels[0])

    def on_choose_dest(self):
        d=filedialog.askdirectory(initialdir=self.dest_dir, title="Choose destination folder")
        if d: self.dest_dir=d; self.var_dest.set(d)

    def on_list(self):
        # clear sets & metadata panel
        for w in list(self.frame_sets.children.values()): w.destroy()
        self.groups.clear(); self.group_vars={}; self.mode={}
        self._close_embedded_meta()

        self.btn_preview.config(state="disabled"); self.btn_download.config(state="disabled"); self.btn_clip.config(state="disabled")
        site_code=label_to_code(self.site_label.get()); tag=self.tag.get().strip()
        self.progress.start(10); self.set_status("Listing sets…")

        def worker():
            try:
                groups=list_product_groups(site_code, tag, self.log); self.groups=groups
                def build():
                    r=0
                    for grp,info in groups.items():
                        v=tk.IntVar(value=0); self.group_vars[grp]=v
                        gl=grp.lower(); m="EVENT"
                        if gl.endswith("_1h"): m="HOUR"
                        elif gl.endswith("_1d"): m="DAY"
                        self.mode[grp]=m
                        exts=", ".join([f"{ext}:{cnt}" for ext,cnt in info["exts"].items()])

                        row=ttk.Frame(self.frame_sets, style="N.Panel.TFrame"); row.grid(row=r, column=0, sticky="we", padx=6, pady=3); r+=1
                        row.grid_columnconfigure(3, weight=1)
                        tk.Checkbutton(row, variable=v, bg=PALETTE["panel"], activebackground=PALETTE["panel"]).grid(row=0,column=0, sticky="w")
                        ttk.Button(row, text="ⓘ Info", style="N.TButton", width=6,
                                   command=lambda g=grp, anchor_row=row: self._toggle_embedded_meta(site_code, g, anchor_row)).grid(row=0,column=1, padx=(6,10))
                        ttk.Label(row, text=grp, style="N.TLabel").grid(row=0,column=2, sticky="w")
                        ttk.Label(row, text=f"  [{m.lower()}]  [{exts}]", style="N.Muted.TLabel").grid(row=0,column=3, sticky="w")

                    self.btn_preview.config(state=("normal" if groups else "disabled"))
                    self.set_status(f"Found {len(groups)} sets. Select → Preview.")
                    self.progress.stop()
                self.after(0, build)
            except Exception as e:
                self.progress.stop(); messagebox.showerror("List failed", str(e))
        threading.Thread(target=worker, daemon=True).start()

    def _selected_groups(self)->List[str]:
        self.update_idletasks()
        return [g for g,v in self.group_vars.items() if v.get()==1]

    # ---------- embedded metadata under a set row ----------
    def _close_embedded_meta(self):
        self._open_meta_grp=None
        if self._open_meta_frame and self._open_meta_frame.winfo_exists():
            self._open_meta_frame.destroy()
        self._open_meta_frame=None; self._open_meta_raw=None; self._open_meta_cards={}

    def _toggle_embedded_meta(self, site_code:str, group:str, anchor_row:tk.Widget):
        if self._open_meta_grp == group:
            self._close_embedded_meta()
            return
        # close any open and open a new one beneath anchor_row
        self._close_embedded_meta()
        self._open_meta_grp = group

        meta_frame = tk.Frame(self.frame_sets, bg=PALETTE["panel"], highlightbackground=PALETTE["steel"], highlightthickness=1)
        # place right after the anchor row
        meta_frame.grid(row=anchor_row.grid_info()["row"]+1, column=0, sticky="we", padx=6, pady=(0,8))
        self._open_meta_frame = meta_frame

        # tabs inside
        tabs = ttk.Notebook(meta_frame, style="N.TNotebook"); tabs.pack(fill="both", expand=True, padx=6, pady=6)
        tab_summary = ttk.Frame(tabs, style="N.Panel.TFrame")
        tab_raw = ttk.Frame(tabs, style="N.Panel.TFrame")
        tabs.add(tab_summary, text="Summary"); tabs.add(tab_raw, text="Raw JSON")

        # summary cards
        title = ttk.Label(tab_summary, text=group, font=self.F["h1"], background=PALETTE["panel"], foreground=PALETTE["ink"])
        title.grid(row=0, column=0, columnspan=3, sticky="w", padx=8, pady=(8,8))

        def mk_card(parent, title, row, col, span=1):
            f=tk.Frame(parent,bg=PALETTE["panel"],highlightbackground=PALETTE["steel"],highlightthickness=1,bd=0)
            f.grid(row=row,column=col,columnspan=span,sticky="nsew",padx=8,pady=6)
            parent.grid_columnconfigure(col, weight=1)
            tk.Label(f,text=title,bg=PALETTE["panel"],fg=PALETTE["ink"],font=self.F["card"]).pack(anchor="w")
            v=tk.StringVar(value="—")
            tk.Label(f,textvariable=v,bg=PALETTE["panel"],fg=PALETTE["ink"],font=self.F["ui"]).pack(anchor="w",pady=(4,2))
            return v
        c_site = mk_card(tab_summary,"SITE",1,0)
        c_dep  = mk_card(tab_summary,"DEPLOYMENT",1,1)
        c_plat = mk_card(tab_summary,"PLATFORM",1,2)
        c_rec  = mk_card(tab_summary,"RECORDER",2,0)
        c_crd  = mk_card(tab_summary,"COORDINATES / DEPTH",2,1,span=2)
        c_st   = mk_card(tab_summary,"START (UTC)",3,0)
        c_en   = mk_card(tab_summary,"END (UTC)",3,1)
        c_sr   = mk_card(tab_summary,"SAMPLE RATE",4,0)
        c_note = mk_card(tab_summary,"LOCATION NOTE",4,1,span=2)

        self._open_meta_cards = {"site":c_site,"deployment":c_dep,"platform":c_plat,"recorder":c_rec,
                                 "coord":c_crd,"start":c_st,"end":c_en,"samplerate":c_sr,"locnote":c_note}

        raw = tk.Text(tab_raw, font=self.F["mono"], bg=PALETTE["panel"], fg=PALETTE["ink"], highlightthickness=0, relief="flat")
        raw.pack(fill="both", expand=True, padx=6, pady=6)
        self._open_meta_raw = raw

        # fetch & fill
        def worker():
            try:
                urls=_gcs_ls_metadata_jsons(site_code, group, self.log)
                if not urls:
                    self.after(0, lambda: raw.insert("end","(no metadata/*.json files found)\n")); return
                texts=[]
                for i,u in enumerate(urls[:2],1):
                    t=_gcs_cat(u) or ""
                    if t: texts.append(f"// [{i}/{len(urls)}] {u}\n{t}\n\n")
                rtxt="".join(texts) if texts else "(empty)"
                meta=None
                for t in texts:
                    jstart=t.find("\n"); jtxt=t[jstart+1:] if jstart>=0 else t
                    meta=_safe_json(jtxt.strip())
                    if meta: break
                summ=build_summary_from_json(meta) if meta else {k:"" for k in self._open_meta_cards}
                def ui():
                    if not self._open_meta_raw or not self._open_meta_raw.winfo_exists(): return
                    self._open_meta_raw.delete("1.0","end")
                    self._open_meta_raw.insert("end", rtxt)
                    for k,v in self._open_meta_cards.items():
                        v.set(summ.get(k,"—") or "—")
                self.after(0, ui)
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Metadata error", str(e)))
        threading.Thread(target=worker, daemon=True).start()

    # ---------- preview ----------
    def on_preview(self):
        chosen=self._selected_groups()
        if not chosen: return messagebox.showinfo("Check at least one set.","")
        site_code=label_to_code(self.site_label.get())
        self.btn_preview.config(state="disabled"); self.btn_download.config(state="disabled"); self.btn_clip.config(state="disabled")
        self.progress.start(10); self.set_status("Previewing…")
        self.file_vars.clear()
        for w in list(self.frame_files.children.values()): w.destroy()
        self.txt_runs.delete("1.0","end"); self.lbl_summary.config(text="")
        threading.Thread(target=self._preview_worker,args=(site_code,chosen),daemon=True).start()

    def _preview_worker(self, site:str, groups:List[str]):
        try:
            os.makedirs(self.dest_dir, exist_ok=True)
            for idx, grp in enumerate(groups,1):
                self.log(f"\n=== Preview {idx}/{len(groups)}: {grp} ===\n")
                pref_folder = folder_from_set(grp)
                info=self.groups[grp]; chosen=choose_best_file(info["paths"])

                # pull CSVs locally
                local_csvs=[]
                for u in chosen:
                    for line in run_cmd(["gsutil","cp",u,f"{self.dest_dir}/"]): self.log(line+"\n")
                    if u.lower().endswith(".csv"): local_csvs.append(os.path.join(self.dest_dir, os.path.basename(u)))
                if not local_csvs:
                    self.log("[WARN] No CSV found; preview expects CSV.\n"); continue

                mode = self.mode[grp]
                if mode=="HOUR":
                    all_hours=[]
                    for lc in local_csvs: all_hours += parse_presence_hours_from_csv(lc)
                    all_hours=sorted(set(all_hours))
                    runs=group_consecutive(all_hours, timedelta(hours=1))
                    if self.only_long_runs.get()==1:
                        runs=[(s,e) for (s,e) in runs if (e-s)>=timedelta(hours=2)]
                    hours=hours_from_runs(runs)
                    tmin=(hours[0]) if hours else None; tmax=(hours[-1]+timedelta(hours=1)) if hours else None
                    files=list_audio_files_across(site, pref_folder, tmin, tmax, self.log)
                    urls, names, _ = minimal_union_for_windows(files, [(h, h+timedelta(hours=1)) for h in hours])
                elif mode=="DAY":
                    all_days=[]
                    for lc in local_csvs: all_days += parse_presence_days_from_csv(lc)
                    all_days=sorted(set(all_days))
                    runs=group_consecutive(all_days, timedelta(days=1))
                    days=days_from_runs(runs)
                    tmin=days[0] if days else None; tmax=(days[-1]+timedelta(days=1)) if days else None
                    files=list_audio_files_across(site, pref_folder, tmin, tmax, self.log)
                    urls, names, _ = minimal_union_for_windows(files, [(d, d+timedelta(days=1)) for d in days])
                else:
                    events=[]
                    for lc in local_csvs: events += parse_events_from_csv(lc)
                    events=sorted(set(events))
                    tmin=events[0][0] if events else None; tmax=events[-1][1] if events else None
                    files=list_audio_files_across(site, pref_folder, tmin, tmax, self.log)
                    urls, names, _ = minimal_union_for_windows(files, events)

                def build_ui():
                    if mode=="HOUR":
                        self.lbl_summary.config(text=f"{grp} | mode: hour | unique files: {len(names)}")
                        self.txt_runs.delete("1.0","end")
                        self.txt_runs.insert("end", f"Runs ({len(runs)}):\n")
                        for i,(rs,re) in enumerate(runs,1):
                            self.txt_runs.insert("end", f"{i:02d}. {rs} → {re}\n")
                    elif mode=="DAY":
                        self.lbl_summary.config(text=f"{grp} | mode: day | unique files: {len(names)}")
                        self.txt_runs.delete("1.0","end"); self.txt_runs.insert("end", f"Days: {len(days)}\n")
                    else:
                        self.lbl_summary.config(text=f"{grp} | mode: event | unique files: {len(names)}")
                        self.txt_runs.delete("1.0","end"); self.txt_runs.insert("end", f"Events: {len(events)}\n")

                    self.file_vars.clear()
                    for name, url in zip(names, urls):
                        st = parse_audio_start_from_name(name)
                        row=ttk.Frame(self.frame_files, style="N.Panel.TFrame"); row.pack(fill="x", padx=4, pady=2)
                        v=tk.IntVar(value=1)
                        tk.Checkbutton(row,variable=v,bg=PALETTE["panel"],activebackground=PALETTE["panel"],
                                       command=self._update_filecount).pack(side="left")
                        ttk.Label(row,text=name,style="N.TLabel").pack(side="left", padx=(6,12))
                        ttk.Label(row,text=(st.isoformat() if st else ""), width=24, style="N.Muted.TLabel").pack(side="left")
                        ttk.Label(row,text=url,style="N.Muted.TLabel").pack(side="left")
                        self.file_vars.append((tk.StringVar(value=url), v))
                    self._update_filecount()
                    self.btn_download.config(state=("normal" if urls else "disabled"))
                self.after(0, build_ui)

                if mode=="HOUR": windows=[(h, h+timedelta(hours=1)) for h in hours]
                elif mode=="DAY": windows=[(d, d+timedelta(days=1)) for d in days]
                else: windows=events
                self.preview_cache[grp]=PreviewCache(mode=mode, windows=windows)

            self.after(0, lambda: (self.set_status("Preview done"), self.progress.stop(), self.btn_preview.config(state="normal")))
        except Exception as e:
            self.progress.stop(); messagebox.showerror("Preview failed", str(e))

    def _select_files(self, state:bool):
        for _,v in self.file_vars: v.set(1 if state else 0)
        self._update_filecount()

    def _update_filecount(self):
        n=sum(1 for _,v in self.file_vars if v.get()==1)
        self.var_filecount.set(f"{n} files selected")

    # ---------- download ----------
    def on_download(self):
        checked=[sv.get() for sv,v in self.file_vars if v.get()==1]
        if not checked:
            return messagebox.showinfo("None selected","Check at least one file.")
        self.btn_download.config(state="disabled"); self.progress.start(10); self.set_status("Downloading files…")
        threading.Thread(target=self._download_worker, args=(checked,), daemon=True).start()

    def _download_worker(self, urls:List[str]):
        try:
            for j,u in enumerate(urls,1):
                self.log(f"$ gsutil cp {u} {self.dest_dir}/\n")
                for line in run_cmd(["gsutil","cp",u,f"{self.dest_dir}/"]): self.log(line+"\n")
                self.log(f"Downloaded [{j}/{len(urls)}]\n")
            self.after(0, lambda: (self.set_status("Download complete"),
                                   self.progress.stop(),
                                   self.btn_download.config(state="normal"),
                                   self.btn_clip.config(state="normal")))
        except Exception as e:
            self.progress.stop(); messagebox.showerror("Download failed", str(e))

    # ---------- clipping (restored, strict selected-file coverage) ----------
    def on_clip(self):
        chosen=self._selected_groups()
        if not chosen: return messagebox.showinfo("Nothing selected","Select the set you previewed.")
        selected_basenames = { os.path.basename(sv.get()) for sv,v in self.file_vars if v.get()==1 }
        if not selected_basenames:
            return messagebox.showinfo("Nothing selected","Select at least one file in the list.")
        self.btn_clip.config(state="disabled"); self.progress.start(10); self.set_status("Clipping…")
        threading.Thread(target=self._clip_worker, args=(chosen, selected_basenames), daemon=True).start()

    def _clip_worker(self, groups:List[str], selected_basenames:set):
        try:
            # Build local index from user-selected files only
            local=[]
            for n in os.listdir(self.dest_dir):
                if not n.lower().endswith(".flac"): continue
                if n not in selected_basenames:  # strict
                    continue
                st=parse_audio_start_from_name(n)
                if not st: continue
                m = re.search(r"(SanctSound_[A-Z]{2}\d{2}_\d{2})_", n, re.I)
                fld=m.group(1).lower() if m else ""
                local.append(dict(path=os.path.join(self.dest_dir,n), fname=n, start_dt=st, folder=fld))
            local.sort(key=lambda x:x["start_dt"])
            for i,h in enumerate(local):
                if i+1<len(local):
                    et=local[i+1]["start_dt"]
                    if et<=h["start_dt"]: et=h["start_dt"]+timedelta(seconds=1)
                else:
                    dur=ffprobe_duration_seconds(h["path"])
                    et=h["start_dt"]+timedelta(seconds=dur if dur and dur>1 else 3600)
                h["end_dt"]=et

            starts=[h["start_dt"] for h in local]
            def cover_and_next(ts):
                i = bisect.bisect_right(starts, ts) - 1
                if i<0: return None, None
                cur = local[i]
                nxt = local[i+1] if (i+1)<len(local) else None
                return cur, nxt

            for grp in groups:
                cache=self.preview_cache.get(grp)
                if not cache:
                    self.log(f"[WARN] No preview cache for {grp}; run Preview first.\n")
                    continue
                windows=list(cache.windows)
                mode=cache.mode
                clips_dir=os.path.join(self.dest_dir,"clips",grp)
                os.makedirs(clips_dir, exist_ok=True)
                manifest_csv=os.path.join(clips_dir,"clips_manifest.csv")
                rows=[]; skipped=0

                for k,(s,e) in enumerate(windows,1):
                    h, nxt = cover_and_next(s)
                    if not h: skipped+=1; continue

                    # Require selected basenames (strict)
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
                            ffmpeg_cut(h["path"], t0, dur, outp, 48000)
                            if os.path.getsize(outp) < MIN_CLIP_BYTES:
                                os.remove(outp); skipped+=1; continue
                            rows.append([out, h["fname"], s.isoformat(), e.isoformat(), round(dur,3), mode])
                        except Exception as ex:
                            skipped+=1; self.log(f"[WARN] ffmpeg failed: {ex}\n")
                    else:
                        if not nxt: skipped+=1; continue
                        t0a=(s-h["start_dt"]).total_seconds()
                        dura=(h["end_dt"]-s).total_seconds()
                        t0b=0.0
                        durb=(e - nxt["start_dt"]).total_seconds()
                        if dura<=0 or durb<=0: skipped+=1; continue
                        tmpa=tempfile.mkstemp(suffix=".wav")[1]
                        tmpb=tempfile.mkstemp(suffix=".wav")[1]
                        out=f"{os.path.splitext(h['fname'])[0]}__{s.strftime('%Y%m%dT%H%M%S')}_{e.strftime('%Y%m%dT%H%M%S')}.wav"
                        outp=os.path.join(clips_dir,out)
                        try:
                            ffmpeg_cut(h["path"], t0a, dura, tmpa, 48000)
                            ffmpeg_cut(nxt["path"], 0.0, durb, tmpb, 48000)
                            ffmpeg_concat_wavs(tmpa, tmpb, outp)
                            if os.path.getsize(outp) < MIN_CLIP_BYTES:
                                os.remove(outp); skipped+=1; continue
                            rows.append([out, f"{h['fname']} + {nxt['fname']}", s.isoformat(), e.isoformat(), round(dura+durb,3), mode])
                        except Exception as ex:
                            skipped+=1; self.log(f"[WARN] stitching failed: {ex}\n")
                        finally:
                            for p in (tmpa,tmpb):
                                try: os.remove(p)
                                except: pass

                    if k%100==0: self.log(f"  cut {k}/{len(windows)} windows …\n")

                with open(manifest_csv,"w",newline="") as f:
                    w=csv.writer(f); w.writerow(["clip_wav","source_flac(s)","start_utc","end_utc","duration_sec","mode"]); w.writerows(rows)
                with open(os.path.join(clips_dir,"clips_summary.txt"),"w") as f:
                    f.write(f"Windows: {len(windows)} | Clips: {len(rows)} | Skipped: {skipped} | Mode: {mode}\nDir: {clips_dir}\n")

                self.log(f"Clips → {clips_dir} | written {len(rows)}, skipped {skipped}\n")

            self.after(0, lambda: (self.set_status("Clipping done"), self.progress.stop(), self.btn_clip.config(state="normal")))
        except Exception as e:
            self.progress.stop(); messagebox.showerror("Clip failed", str(e))

# -------------- main --------------
def AppMain():
    app=App(); app.mainloop()

if __name__=="__main__":
    print("Loaded sanctsound_gui — restored layout, embedded metadata, full clipping")
    AppMain()
