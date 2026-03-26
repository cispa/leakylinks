from collections import Counter
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import pathlib
import hashlib

def _bs(html: str) -> BeautifulSoup:
    # Prefer fast lxml; fall back to built-in if unavailable
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def _domain_of(url: str) -> str | None:
    try:
        host = urlparse(url).hostname
        if not host:
            return None
        return host.lower().lstrip("www.")
    except Exception:
        return None

def _extract_title_from_soup(soup: BeautifulSoup) -> str:
    t = soup.find("title")
    return (t.get_text(strip=True) if t else "")[:512]

def _extract_script_domain_counts_from_soup(soup: BeautifulSoup, base_url: str | None = None) -> dict[str, int]:
    counts = Counter()
    for tag in soup.find_all("script"):
        src = tag.get("src")
        if not src:
            continue
        if base_url:
            src = urljoin(base_url, src)
        dom = _domain_of(src)
        if dom:
            counts[dom] += 1
    return dict(counts)

def extract_title(html: str) -> str:
    soup = _bs(html)
    return _extract_title_from_soup(soup)

def extract_script_domain_counts(html: str, base_url: str | None = None) -> dict[str, int]:
    soup = _bs(html)
    return _extract_script_domain_counts_from_soup(soup, base_url=base_url)

def build_page_stats_from_html(html_bytes: bytes, base_url: str | None = None) -> dict:
    html_str = html_bytes.decode("utf-8", errors="replace")
    soup = _bs(html_str)  # single parse
    return {
        "bytes_size": len(html_bytes),
        "script_stats": _extract_script_domain_counts_from_soup(soup, base_url=base_url),
        "title": _extract_title_from_soup(soup),
    }

def build_page_stats_from_file(path: str | pathlib.Path, base_url: str | None = None) -> dict:
    p = pathlib.Path(path)
    html_bytes = p.read_bytes()
    return build_page_stats_from_html(html_bytes, base_url=base_url)

def file_hash(path: str | pathlib.Path) -> str:
    p = pathlib.Path(path)
    return hashlib.sha256(p.read_bytes()).hexdigest()
