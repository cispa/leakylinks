"""
Token detection utility functions for URL token checking.
No database operations - pure functions only.
"""
import math
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote


def _shannon_entropy(s: str) -> float:
    """
    Shannon entropy in bits per character.
    Empty strings return 0.0.
    """
    if not s:
        return 0.0
    # Frequency of each character
    freq = {}
    for ch in s:
        freq[ch] = freq.get(ch, 0) + 1
    n = len(s)
    entropy = 0.0
    for c in freq.values():
        p = c / n
        entropy -= p * math.log2(p)
    return entropy


def _is_likely_filename(segment: str) -> bool:
    """
    Detect if a segment is likely a filename (has an extension).
    Looks for pattern: something.ext where ext has 1-10 alphanumeric chars.
    No allowlist - just pattern matching.
    """
    if not segment:
        return False
    
    # Must have a dot
    if '.' not in segment:
        return False
    
    # Split by last dot
    parts = segment.rsplit('.', 1)
    if len(parts) != 2:
        return False
    
    name_part, ext_part = parts
    
    # Extension should be 1-10 alphanumeric characters
    if not ext_part or len(ext_part) > 10:
        return False
    
    # Extension should be mostly alphanumeric (allow some special chars like _-)
    if not all(c.isalnum() or c in ('_', '-') for c in ext_part):
        return False
    
    # Name part should exist and not be empty
    if not name_part:
        return False
    
    return True


def _is_token_like(seg: str, min_len: int, min_entropy: float, skip_entropy: bool = False) -> bool:
    """
    Heuristic: consider a segment a 'token' if it meets length & entropy threshold.
    If skip_entropy is True, only check length.
    """
    if not seg:
        return False
    seg = seg.strip()
    # Ignore trivial or placeholder segments
    if seg in ("", ".", "..", "-", "_"):
        return False
    if len(seg) < min_len:
        return False
    if skip_entropy:
        return False  # Actually skip entirely
    return _shannon_entropy(seg) >= min_entropy


def strict_has_token_smart(url: Optional[str], min_len: int, min_entropy: float) -> bool:
    """
    Smart URL token detector that:
    - Uses min_len and min_entropy thresholds
    - Detects if last path segment is a filename
    - Skips checking filenames entirely (don't treat filenames as tokens)
    - Checks path segments, query values, and fragment segments
    """
    if not url or not isinstance(url, str):
        return False
    s = url.strip()
    if not s:
        return False
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    p = urlparse(s)
    if (not p.path or p.path == "/") and not p.query and not p.fragment:
        return False

    # Path segments
    path_segments = p.path.strip("/").split("/")
    if path_segments and path_segments[0]:  # Non-empty path
        for i, seg in enumerate(path_segments):
            seg = unquote(seg)
            if not seg:
                continue
            
            # Check if this is the last segment and if it's a filename
            is_last = (i == len(path_segments) - 1)
            is_file = _is_likely_filename(seg) if is_last else False
            
            # Skip checking entirely if it's a filename (don't treat filenames as tokens)
            if is_file:
                continue
            
            # Check non-filename segments for tokens
            if _is_token_like(seg, min_len, min_entropy, skip_entropy=False):
                return True

    # Query keys and values - always check entropy for query params
    for key, vals in parse_qs(p.query, keep_blank_values=True).items():
        # Check query parameter name (key)
        key_unquoted = unquote(key)
        if _is_token_like(key_unquoted, min_len, min_entropy, skip_entropy=False):
            return True
        # Check query values
        for v in vals:
            v = unquote(v)
            if _is_token_like(v, min_len, min_entropy, skip_entropy=False):
                return True

    # Fragment segments - always check entropy for fragments
    if p.fragment:
        for seg in p.fragment.strip("/").split("/"):
            seg = unquote(seg)
            if _is_token_like(seg, min_len, min_entropy, skip_entropy=False):
                return True

    return False


def is_valid_http_url(raw_url: Optional[str]) -> bool:
    """Validate if string is a valid HTTP/HTTPS URL."""
    if not raw_url:
        return False
    s = raw_url.strip()
    try:
        p = urlparse(s)
        if p.scheme in ("http", "https") and p.netloc and "." in p.netloc:
            return True
        if not s.startswith(("http://", "https://")):
            p2 = urlparse("https://" + s)
            if p2.scheme in ("http", "https") and p2.netloc and "." in p2.netloc:
                return True
        return False
    except Exception:
        return False

