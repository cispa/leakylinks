"""
OCR utility functions for script detection and language selection.
"""

import pytesseract
from pytesseract import Output
from typing import Optional

SCRIPT_TO_LANG = {
    "Han": "chi_sim",         # Simplified Chinese
    "Hani": "chi_sim",        # Some builds report 'Hani'
    "Hangul": "kor",
    "Katakana": "jpn",
    "Hiragana": "jpn",
    "Japanese": "jpn",
    "Arabic": "ara",
    "Cyrillic": "rus",
    "Hebrew": "heb",
    "Thai": "tha",
    "Devanagari": "hin",
    # Fallbacks
    "Latin": "eng",
}

# Multi-language fallback for when script detection fails
MULTI_LANG_FALLBACK = "eng+deu+fra+spa+ita+chi_sim+jpn+kor+ara+rus+hin+heb+tha"


def detect_script(img) -> str:
    """
    Return a best-guess script label from Tesseract OSD.
    
    Args:
        img: PIL Image object
        
    Returns:
        Script name as string, or empty string if detection fails
    """
    try:
        osd = pytesseract.image_to_osd(img, output_type=Output.DICT)  # requires --psm 0 internally
        # keys often include 'script' and 'script_conf'
        script = osd.get("script")
        return script or ""
    except Exception:
        return ""


def pick_lang_for_image(img, default: str = MULTI_LANG_FALLBACK) -> str:
    """
    Use OSD to pick a minimal language set. Never return a huge combo.
    Prefer single-target models (chi_sim, jpn, kor, etc.).
    
    Args:
        img: PIL Image object
        default: Default language string to use if detection fails
        
    Returns:
        Language string for pytesseract (e.g., "chi_sim", "eng+deu", etc.)
    """
    script = detect_script(img)
    if script in SCRIPT_TO_LANG:
        return SCRIPT_TO_LANG[script]
    # If OSD didn't help, use the provided default (usually multi-lang fallback)
    return default

