from __future__ import annotations
import subprocess
from shutil import which as _which
from typing import Iterable

def which(binname: str) -> str | None:
    return _which(binname)

def run_cmd(argv: list[str]) -> Iterable[str]:
    """Yield lines from a subprocess, raising on non-zero exit."""
    p = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in iter(p.stdout.readline, ''):
        yield line.rstrip("\n")
    p.stdout.close()
    p.wait()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, argv)

def ffprobe_duration_seconds(path: str) -> float | None:
    try:
        out = subprocess.check_output(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1", path],
            text=True).strip()
        return float(out)
    except Exception:
        return None

def ffmpeg_cut(src: str, start_sec: float, dur_sec: float, out_wav: str,
               sr_out: int, mono: bool, sample_fmt: str):
    """Cut a segment, resample/mono as configured."""
    from math import fsum
    dur = max(0.01, float(dur_sec))
    argv = ["ffmpeg","-y","-loglevel","error",
            "-ss",f"{start_sec:.3f}","-t",f"{dur:.3f}",
            "-i",src]
    if mono:
        argv += ["-ac","1"]
    argv += ["-ar", str(sr_out), "-sample_fmt", sample_fmt, out_wav]
    subprocess.check_call(argv)

def ffmpeg_concat_wavs(wav1: str, wav2: str, out_wav: str):
    import tempfile, os
    fd, listfile = tempfile.mkstemp(suffix=".txt")
    os.close(fd)
    try:
        with open(listfile, "w") as f:
            f.write(f"file '{wav1}'\n")
            f.write(f"file '{wav2}'\n")
        argv = ["ffmpeg","-y","-loglevel","error","-f","concat","-safe","0",
                "-i",listfile,"-c","copy",out_wav]
        subprocess.check_call(argv)
    finally:
        try: os.remove(listfile)
        except: pass

