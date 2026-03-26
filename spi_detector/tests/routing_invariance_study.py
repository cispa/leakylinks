#!/usr/bin/env python3
"""
Routing Invariance Study

This script studies how routing decisions change when varying token detection parameters
(length and entropy thresholds). It compares routing outcomes across different parameter
combinations to measure routing invariance.

The routing logic (using exact SQL queries):
- Token branch (branch down): final_url_before_has_token = true
- Roth branch (branch up): final_url_before_has_token = false 
  AND levenshtein >= 1 AND rothetal_sim < 0.8

We vary only token detection parameters (length and entropy).
Levenshtein and roth sim thresholds remain fixed.
"""

import sys
import math
import json
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urlparse, parse_qs, unquote
from collections import defaultdict
from pathlib import Path

# Add the project root to Python path
import os
from config.settings import PROJECT_PATH
sys.path.append(PROJECT_PATH)

from pipeline.db import DB  # type: ignore


def _shannon_entropy(s: str) -> float:
    """Shannon entropy in bits per character."""
    if not s:
        return 0.0
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
    """Detect if a segment is likely a filename (has an extension)."""
    if not segment or '.' not in segment:
        return False
    parts = segment.rsplit('.', 1)
    if len(parts) != 2:
        return False
    name_part, ext_part = parts
    if not ext_part or len(ext_part) > 10:
        return False
    if not all(c.isalnum() or c in ('_', '-') for c in ext_part):
        return False
    if not name_part:
        return False
    return True


def _is_token_like(seg: str, min_len: int, min_entropy: float) -> bool:
    """Check if segment meets token criteria."""
    if not seg:
        return False
    seg = seg.strip()
    if seg in ("", ".", "..", "-", "_"):
        return False
    if len(seg) < min_len:
        return False
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
            if _is_token_like(seg, min_len, min_entropy):
                return True

    # Query keys and values - always check entropy for query params
    for key, vals in parse_qs(p.query, keep_blank_values=True).items():
        # Check query parameter name (key)
        key_unquoted = unquote(key)
        if _is_token_like(key_unquoted, min_len, min_entropy):
            return True
        # Check query values
        for v in vals:
            v = unquote(v)
            if _is_token_like(v, min_len, min_entropy):
                return True

    # Fragment segments - always check entropy for fragments
    if p.fragment:
        for seg in p.fragment.strip("/").split("/"):
            seg = unquote(seg)
            if _is_token_like(seg, min_len, min_entropy):
                return True

    return False


def load_filtered_analysis_data(db: DB) -> List[Dict]:
    """
    Load all relevant data that could potentially be routed.
    We load ALL cases (not pre-filtered by token detection) so we can recompute
    routing with different parameters.
    
    This loads all cases where:
    - final_url_before_valid = true
    - vision_sensitive = TRUE
    
    We don't filter by token detection here - that will be done in compute_routing_for_params
    with the specific parameters being tested.
    """
    results = []
    
    # Load ALL eligible cases (not pre-filtered by token detection)
    # This allows us to recompute routing with different token detection parameters
    query_all = """
        SELECT 
            a.source_table,
            a.source_id,
            a.page_url,
            a.session_analysis -> 'json' ->> 'finalUrlBefore' as finalurlbefore,
            a.rothetal_sim
        FROM filtered_analysis_output a 
        WHERE a.final_url_before_valid = true 
        AND EXISTS (
            SELECT 1
            FROM screenshot_test_qwen v
            WHERE v.source_id = a.source_id
                AND v.source_table = a.source_table
                AND v.vision_sensitive = TRUE
        )
    """
    db.cursor.execute(query_all)
    rows = db.cursor.fetchall()
    
    for row in rows:
        source_table, source_id, page_url, final_url_before, rothetal_sim = row
        # Only include cases that have a valid final_url_before
        if final_url_before:
            results.append({
                'source_table': source_table,
                'source_id': source_id,
                'page_url': page_url,
                'final_url_before': final_url_before,
                'rothetal_sim': rothetal_sim,
                # Note: We don't set original_branch here since we're loading all cases
                # The original branch classification will be computed separately if needed
            })
    
    return results


def compute_routing_for_params(data: List[Dict], min_len: int, min_entropy: float) -> Dict[str, Set[Tuple[str, str]]]:
    """
    Compute routing for given parameters.
    Uses the same logic as the SQL queries but with different token detection parameters.
    
    For roth branch: has_token = false AND levenshtein >= 1 AND rothetal_sim < 0.8
    For token branch: has_token = true
    
    Returns dict with 'token_branch' and 'roth_branch' sets of (source_table, source_id) tuples.
    """
    try:
        from Levenshtein import distance as levenshtein_distance
    except ImportError:
        # Fallback to a simple implementation if python-Levenshtein is not available
        def levenshtein_distance(s1, s2):
            """Simple Levenshtein distance implementation."""
            if len(s1) < len(s2):
                return levenshtein_distance(s2, s1)
            if len(s2) == 0:
                return len(s1)
            previous_row = range(len(s2) + 1)
            for i, c1 in enumerate(s1):
                current_row = [i + 1]
                for j, c2 in enumerate(s2):
                    insertions = previous_row[j + 1] + 1
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)
                    current_row.append(min(insertions, deletions, substitutions))
                previous_row = current_row
            return previous_row[-1]
    
    token_branch = set()
    roth_branch = set()
    unclassified = set()  # Cases that don't meet criteria for either branch
    
    for item in data:
        source_table = item['source_table']
        source_id = item['source_id']
        key = (source_table, source_id)
        final_url_before = item['final_url_before']
        page_url = item['page_url']
        rothetal_sim = item.get('rothetal_sim')
        
        if not final_url_before:
            unclassified.add(key)
            continue
        
        # Compute token detection with new parameters
        has_token = strict_has_token_smart(final_url_before, min_len, min_entropy)
        
        if has_token:
            # Token branch: has_token = true
            token_branch.add(key)
        else:
            # Roth branch: has_token = false AND levenshtein >= 1 AND rothetal_sim < 0.8
            # Check levenshtein condition
            final_url_right = final_url_before[-255:] if len(final_url_before) > 255 else final_url_before
            page_url_right = page_url[-255:] if page_url and len(page_url) > 255 else (page_url or "")
            
            levenshtein_dist = levenshtein_distance(final_url_right, page_url_right)
            levenshtein_ok = levenshtein_dist >= 1
            
            # Check roth sim condition
            roth_sim_ok = (rothetal_sim is not None and rothetal_sim < 0.8)
            
            if levenshtein_ok and roth_sim_ok:
                roth_branch.add(key)
            else:
                # If conditions not met, case is unclassified (not in either branch)
                unclassified.add(key)
    
    return {
        'token_branch': token_branch,
        'roth_branch': roth_branch,
        'unclassified': unclassified,
        'total_processed': len(data)
    }


def compare_routings(base_routing: Dict[str, Set], compare_routing: Dict[str, Set]) -> Dict:
    """
    Compare two routing results and compute differences.
    Returns statistics about routing changes.
    """
    base_token = base_routing['token_branch']
    base_roth = base_routing['roth_branch']
    compare_token = compare_routing['token_branch']
    compare_roth = compare_routing['roth_branch']
    
    # Cases that changed from token to roth
    token_to_roth = base_token - compare_token
    
    # Cases that changed from roth to token
    roth_to_token = base_roth - compare_roth
    
    # Cases that stayed in token branch
    stayed_token = base_token & compare_token
    
    # Cases that stayed in roth branch
    stayed_roth = base_roth & compare_roth
    
    total_changed = len(token_to_roth) + len(roth_to_token)
    total_stable = len(stayed_token) + len(stayed_roth)
    total_cases = len(base_token) + len(base_roth)
    
    return {
        'token_to_roth': token_to_roth,
        'roth_to_token': roth_to_token,
        'stayed_token': stayed_token,
        'stayed_roth': stayed_roth,
        'num_token_to_roth': len(token_to_roth),
        'num_roth_to_token': len(roth_to_token),
        'num_stayed_token': len(stayed_token),
        'num_stayed_roth': len(stayed_roth),
        'num_total_changed': total_changed,
        'num_total_stable': total_stable,
        'num_total_cases': total_cases,
        'pct_changed': (total_changed / total_cases * 100) if total_cases > 0 else 0.0,
        'pct_stable': (total_stable / total_cases * 100) if total_cases > 0 else 0.0
    }


def generate_report(
    all_routings: Dict[Tuple[int, float], Dict[str, Set]],
    base_params: Tuple[int, float],
    output_file: Optional[str] = None
) -> str:
    """Generate a comprehensive report comparing routing across parameter combinations."""
    base_routing = all_routings[base_params]
    
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("ROUTING INVARIANCE STUDY")
    report_lines.append("=" * 80)
    report_lines.append("")
    report_lines.append(f"Base parameters: len={base_params[0]}, entropy={base_params[1]}")
    base_token = len(base_routing['token_branch'])
    base_roth = len(base_routing['roth_branch'])
    base_unclassified = len(base_routing.get('unclassified', set()))
    base_total = base_routing.get('total_processed', base_token + base_roth + base_unclassified)
    base_routed = base_token + base_roth
    report_lines.append(f"Base routing: {base_token} token, {base_roth} roth, {base_unclassified} unclassified")
    if base_total > 0:
        report_lines.append(f"Total processed: {base_total}, Routed: {base_routed} ({base_routed}/{base_total} = {base_routed/base_total*100:.2f}%)")
    else:
        report_lines.append(f"Total processed: {base_total}, Routed: {base_routed}")
    report_lines.append("")
    
    # Sort parameter combinations for consistent output
    sorted_params = sorted(all_routings.keys(), key=lambda x: (x[0], x[1]))
    
    report_lines.append("=" * 80)
    report_lines.append("PARAMETER COMBINATIONS COMPARISON")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Table header
    report_lines.append(f"{'Len':<6} {'Entropy':<8} {'Token':<8} {'Roth':<8} {'Unclassified':<12} {'Total':<8} {'Changed':<10} {'% Changed':<12} {'Stable':<10} {'% Stable':<12}")
    report_lines.append("-" * 100)
    
    for params in sorted_params:
        if params == base_params:
            continue
        
        compare_routing = all_routings[params]
        comparison = compare_routings(base_routing, compare_routing)
        
        min_len, min_entropy = params
        token_count = len(compare_routing['token_branch'])
        roth_count = len(compare_routing['roth_branch'])
        unclassified_count = len(compare_routing.get('unclassified', set()))
        total_routed = token_count + roth_count
        changed = comparison['num_total_changed']
        pct_changed = comparison['pct_changed']
        stable = comparison['num_total_stable']
        pct_stable = comparison['pct_stable']
        
        report_lines.append(
            f"{min_len:<6} {min_entropy:<8.2f} {token_count:<8} {roth_count:<8} {unclassified_count:<12} {total_routed:<8} "
            f"{changed:<10} {pct_changed:<12.2f} {stable:<10} {pct_stable:<12.2f}"
        )
    
    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("DETAILED CHANGES")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Detailed changes for each parameter combination
    for params in sorted_params:
        if params == base_params:
            continue
        
        min_len, min_entropy = params
        compare_routing = all_routings[params]
        comparison = compare_routings(base_routing, compare_routing)
        
        report_lines.append(f"Parameters: len={min_len}, entropy={min_entropy}")
        report_lines.append(f"  Token → Roth: {comparison['num_token_to_roth']} cases")
        report_lines.append(f"  Roth → Token: {comparison['num_roth_to_token']} cases")
        report_lines.append(f"  Total changed: {comparison['num_total_changed']} ({comparison['pct_changed']:.2f}%)")
        report_lines.append(f"  Total stable: {comparison['num_total_stable']} ({comparison['pct_stable']:.2f}%)")
        report_lines.append("")
    
    # Summary statistics
    report_lines.append("=" * 80)
    report_lines.append("SUMMARY STATISTICS")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Find most stable parameter combination
    stability_scores = {}
    for params in sorted_params:
        if params == base_params:
            continue
        comparison = compare_routings(base_routing, all_routings[params])
        stability_scores[params] = comparison['pct_stable']
    
    if stability_scores:
        most_stable = max(stability_scores.items(), key=lambda x: x[1])
        least_stable = min(stability_scores.items(), key=lambda x: x[1])
        
        report_lines.append(f"Most stable (vs base): len={most_stable[0][0]}, entropy={most_stable[0][1]:.2f} ({most_stable[1]:.2f}% stable)")
        report_lines.append(f"Least stable (vs base): len={least_stable[0][0]}, entropy={least_stable[0][1]:.2f} ({least_stable[1]:.2f}% stable)")
        report_lines.append("")
    
    # Distribution across all parameter combinations
    report_lines.append("Routing distribution across all parameter combinations:")
    report_lines.append("")
    report_lines.append(f"{'Len':<6} {'Entropy':<8} {'Token Branch':<15} {'Roth Branch':<15} {'Unclassified':<15} {'Total Routed':<15}")
    report_lines.append("-" * 75)
    
    for params in sorted_params:
        min_len, min_entropy = params
        routing = all_routings[params]
        token_count = len(routing['token_branch'])
        roth_count = len(routing['roth_branch'])
        unclassified_count = len(routing.get('unclassified', set()))
        total_routed = token_count + roth_count
        report_lines.append(
            f"{min_len:<6} {min_entropy:<8.2f} {token_count:<15} {roth_count:<15} {unclassified_count:<15} {total_routed:<15}"
        )
    
    report = "\n".join(report_lines)
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"Report written to {output_file}")
    
    return report


def main():
    """Main function to run the routing invariance study."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Study routing invariance across different token detection parameters"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use live DB config instead of test"
    )
    parser.add_argument(
        "--min-lens",
        type=int,
        nargs="+",
        default=[6, 8, 10, 12, 16, 20, 24, 28, 32],
        help="Token length thresholds to test (default: 6 8 10 12 16 20 24 28 32)"
    )
    parser.add_argument(
        "--entropies",
        type=float,
        nargs="+",
        default=[1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
        help="Entropy thresholds to test (default: 1.5 2.0 2.5 3.0 3.5 4.0 4.5 5.0)"
    )
    parser.add_argument(
        "--base-len",
        type=int,
        default=8,
        help="Base token length for comparison (default: 8)"
    )
    parser.add_argument(
        "--base-entropy",
        type=float,
        default=2.0,
        help="Base entropy for comparison (default: 2.0)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="routing_invariance_report.txt",
        help="Output file for report (default: routing_invariance_report.txt)"
    )
    parser.add_argument(
        "--json-output",
        type=str,
        default=None,
        help="Optional JSON output file for detailed results"
    )
    
    args = parser.parse_args()
    
    print("Loading data from database...")
    db = DB(test_mode=False)
    
    try:
        # Load all eligible cases (not pre-filtered by token detection)
        print("Loading all eligible cases from database...")
        data = load_filtered_analysis_data(db)
        print(f"Loaded {len(data)} cases")
        
        # Compute routing for all parameter combinations
        print("\nComputing routing for parameter combinations...")
        all_routings = {}
        
        param_combinations = [(len_thresh, ent_thresh) 
                             for len_thresh in args.min_lens 
                             for ent_thresh in args.entropies]
        
        for min_len, min_entropy in param_combinations:
            print(f"  Computing: len={min_len}, entropy={min_entropy:.2f}")
            routing = compute_routing_for_params(data, min_len, min_entropy)
            all_routings[(min_len, min_entropy)] = routing
            token_count = len(routing['token_branch'])
            roth_count = len(routing['roth_branch'])
            unclassified_count = len(routing.get('unclassified', set()))
            total_routed = token_count + roth_count
            total_processed = routing.get('total_processed', len(data))
            print(f"    Result: {token_count} token branch, {roth_count} roth branch, {unclassified_count} unclassified")
            if total_processed > 0:
                print(f"    Total: {total_routed} routed out of {total_processed} processed ({total_routed/total_processed*100:.2f}% routed)")
            else:
                print(f"    Total: {total_routed} routed out of {total_processed} processed")
        
        # Generate report
        print("\nGenerating report...")
        base_params = (args.base_len, args.base_entropy)
        if base_params not in all_routings:
            print(f"Warning: Base parameters {base_params} not in tested combinations. Using first combination.")
            base_params = param_combinations[0]
        
        report = generate_report(all_routings, base_params, args.output)
        print("\n" + report)
        
        # Save JSON output if requested
        if args.json_output:
            json_data = {
                'base_parameters': {'len': base_params[0], 'entropy': base_params[1]},
                'parameter_combinations': [
                    {
                        'len': min_len,
                        'entropy': min_entropy,
                        'token_branch_count': len(routing['token_branch']),
                        'roth_branch_count': len(routing['roth_branch']),
                        'token_branch_keys': [
                            {'source_table': st, 'source_id': si}
                            for st, si in routing['token_branch']
                        ],
                        'roth_branch_keys': [
                            {'source_table': st, 'source_id': si}
                            for st, si in routing['roth_branch']
                        ]
                    }
                    for (min_len, min_entropy), routing in sorted(all_routings.items())
                ]
            }
            
            with open(args.json_output, 'w') as f:
                json.dump(json_data, f, indent=2)
            print(f"\nJSON output written to {args.json_output}")
        
    finally:
        db.close()
    
    print("\nDone!")


if __name__ == "__main__":
    main()

