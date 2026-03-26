from difflib import SequenceMatcher

SIMILARITY_CACHE = {}

def jaccard_similarity(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 1.0
    inter = len(set_a & set_b)
    union = len(set_a) + len(set_b) - inter
    return inter / union if union else 1.0

def _stats_signature(d: dict) -> tuple:
    """
    Hashable, order-independent signature of exactly what this function reads.
    This replaces the slow json.dumps(a)+json.dumps(b) key.
    """
    return (
        d.get('bytes_size', 0),
        d.get('title', ''),
        frozenset(d.get('script_stats', {}).items()),  # (domain, count) pairs
    )

def compute_similarity_score(a: dict, b: dict, verbose: bool = False) -> float | dict:
    """
    Compute similarity between two page stats dicts (bytes_size, script_stats, title).
    Same outputs as before; only the cache key generation is changed for speed.
    """
    # Fast, canonical cache key (no JSON serialization)
    cache_key = (_stats_signature(a), _stats_signature(b))
    if not verbose:
        cached = SIMILARITY_CACHE.get(cache_key)
        if cached is not None:
            return cached

    script_stats_a = a['script_stats']
    script_stats_b = b['script_stats']

    script_domains_a = set(script_stats_a.keys())
    script_domains_b = set(script_stats_b.keys())

    # Domains Jaccard
    script_domains_similarity = jaccard_similarity(script_domains_a, script_domains_b)

    # Per-domain count ratio on intersection
    common_domains = script_domains_a & script_domains_b
    if common_domains:
        s = 0.0
        n = 0
        for dname in common_domains:
            na = script_stats_a[dname]
            nb = script_stats_b[dname]
            lo, hi = (na, nb) if na <= nb else (nb, na)
            s += lo / hi
            n += 1
        script_number_similarity = s / n
    else:
        script_number_similarity = 1.0

    # Size similarity
    a_bytes = a['bytes_size']
    b_bytes = b['bytes_size']
    m = a_bytes if a_bytes >= b_bytes else b_bytes
    size_similarity = 1.0 if m == 0 else (a_bytes if a_bytes <= b_bytes else b_bytes) / m

    # Title similarity
    title_similarity = SequenceMatcher(None, a['title'], b['title']).ratio()

    min_similarity = min(script_number_similarity, script_domains_similarity, size_similarity, title_similarity)
    if min_similarity < 0.75:
        final_score = 0.0
    else:
        final_score = (script_number_similarity + script_domains_similarity + size_similarity + title_similarity) / 4.0

    if not verbose:
        SIMILARITY_CACHE[cache_key] = final_score

    if verbose:
        return {
            'score': final_score,
            'metrics': {
                'script_domains_similarity': script_domains_similarity,
                'script_number_similarity': script_number_similarity,
                'size_similarity': size_similarity,
                'title_similarity': title_similarity,
            },
            'details': {
                'script_domains_a': list(script_domains_a),
                'script_domains_b': list(script_domains_b),
                'common_domains': list(common_domains),
                'bytes_size_a': a['bytes_size'],
                'bytes_size_b': b['bytes_size'],
                'title_a': a['title'],
                'title_b': b['title'],
            }
        }

    return final_score
