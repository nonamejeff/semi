# sanctsound_gui/sites.py
# Human-friendly site labeling and helpers for the SanctSound GUI.

from __future__ import annotations

# Map 2-letter site prefixes → sanctuary names
SITE_PREFIX_NAME: dict[str, str] = {
    "ci": "Channel Islands",
    "fk": "Florida Keys",
    "gr": "Gray's Reef",
    "hi": "Hawaiian Islands",
    "mb": "Monterey Bay",
    "oc": "Olympic Coast",
    "pm": "Papahānaumokuākea",
    "sb": "Stellwagen Bank",
}

# If you later build this list from your bucket scan or metadata index,
# just update KNOWN_CODES at import time or return a computed list in
# `site_labels_for_dropdown()`.
KNOWN_CODES: list[str] = [
    "ci01","ci02","ci03","ci04","ci05",
    "fk01","fk02","fk03","fk04",
    "gr01","gr02","gr03",
    "hi01","hi03","hi04","hi05","hi06",
    "mb01","mb02","mb03",
    "oc01","oc02","oc03","oc04",
    "pm01","pm02","pm05",
    "sb01","sb02","sb03",
]


def site_label_for_code(code: str) -> str:
    """
    Convert a code like 'ci01' → 'Channel Islands — CI01'.
    """
    c = (code or "").strip().lower()
    prefix = c[:2]
    friendly = SITE_PREFIX_NAME.get(prefix, prefix.upper())
    return f"{friendly} — {c.upper()}"


def code_for_label(label: str) -> str:
    """
    Extract 'ci01' from a label like 'Channel Islands — CI01'.
    Falls back to returning the last token lowered.
    """
    if "—" in label:
        return label.split("—")[-1].strip().lower()
    if "-" in label:
        return label.split("-")[-1].strip().lower()
    return label.strip().lower()


def site_labels_for_dropdown() -> list[str]:
    """
    Return the list of user-facing labels for the dropdown, sorted by code.
    """
    return [site_label_for_code(c) for c in sorted(KNOWN_CODES)]
