#!/usr/bin/env python3
"""
Test reproducibility of vision detection by running each image 10 times.
Simple version - uses port 52000.

READ-ONLY: This script does NOT write to the database.
- Only reads from manual_labels_qwen_filtered.json
- Only calls LLM analysis functions (which are read-only)
- Only optionally saves test results to a JSON file (if --output specified)
"""

import json
import sys
import time
from pathlib import Path
from typing import Dict, List
from collections import Counter
import statistics

# Add the project root to Python path
import os
from config.settings import PROJECT_PATH
sys.path.append(PROJECT_PATH)

import spi_detector.analyze_screenshot as analyze_screenshot

MANUAL_LABELS_FILE = os.path.join(PROJECT_PATH, "screenshot_analyzer", "manual_labels_qwen_filtered.json")
NUM_RUNS = 10


def load_manual_labels() -> Dict:
    """Load manual labels from JSON file."""
    try:
        if Path(MANUAL_LABELS_FILE).exists():
            with open(MANUAL_LABELS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Failed to load manual labels: {e}")
        return {}


def test_image_reproducibility(image_path: str, image_key: str, num_runs: int = NUM_RUNS, show_port: bool = True) -> Dict:
    """
    Run detection on a single image multiple times.
    """
    results = []
    
    print(f"  Testing {image_key} ({num_runs} runs)...")
    print(f"    Image: {image_path}")
    if show_port:
        print(f"    Using LLM at: {analyze_screenshot._get_llm_host()}")
    
    for run_num in range(1, num_runs + 1):
        # Show which port is being used for this run (if round-robin)
        if show_port and analyze_screenshot._LLM_MODE == "round_robin":
            current_host = analyze_screenshot._get_llm_host()
            print(f"    Run {run_num}: Using {current_host}")
        # Add small delay between runs
        if run_num > 1:
            time.sleep(0.5)
        
        try:
            vision_start = time.time()
            vision_response = analyze_screenshot.call_llm_with_image(
                image_path,
                analyze_screenshot.VISION_SYSTEM_PROMPT,
                analyze_screenshot.VISION_USER_PROMPT
            )
            vision_time = time.time() - vision_start
            
            # Parse vision response
            vision_json_str = analyze_screenshot.extract_json_from_response(vision_response) if vision_response else None
            
            if vision_json_str:
                try:
                    vision_llm_obj = json.loads(vision_json_str)
                    vision_llm_obj = analyze_screenshot.normalize_llm_numbers(vision_llm_obj)
                except json.JSONDecodeError:
                    vision_llm_obj = None
            else:
                vision_llm_obj = None
            
            # Build result
            if not vision_llm_obj:
                result = {
                    "sensitive": False,
                    "confidence": 0.0,
                    "error": "Vision LLM call failed or returned invalid JSON",
                    "page_type": None,
                    "processing_time": vision_time
                }
            else:
                result = {
                    "sensitive": vision_llm_obj.get("sensitive", False),
                    "confidence": vision_llm_obj.get("confidence", 0.0),
                    "risk_score": vision_llm_obj.get("risk_score", 0.0),
                    "primary_intent": vision_llm_obj.get("primary_intent"),
                    "page_type": vision_llm_obj.get("page_type"),
                    "pii_types": vision_llm_obj.get("pii_types", []),
                    "quoted_evidence": vision_llm_obj.get("quoted_evidence", []),
                    "reasons": vision_llm_obj.get("reasons", []),
                    "processing_time": vision_time,
                    "llm_raw": vision_llm_obj,
                    "error": None
                }
            
            results.append(result)
            
            if result.get('error'):
                print(f"    Run {run_num}/{num_runs}: FAILED {result.get('error')}")
            else:
                print(f"    Run {run_num}/{num_runs}: PASS sensitive={result.get('sensitive')}, "
                      f"confidence={result.get('confidence', 0):.2f}, time={vision_time:.1f}s")
                
        except Exception as e:
            error_msg = str(e)
            results.append({
                'sensitive': None,
                'confidence': None,
                'error': error_msg,
                'page_type': None
            })
            print(f"    Run {run_num}/{num_runs}: ✗ {error_msg[:80]}")
    
    # Analyze results
    sensitive_values = [r.get('sensitive') for r in results if r.get('sensitive') is not None]
    sensitive_counts = Counter(sensitive_values)
    
    # Check consistency
    unique_sensitive = set(sensitive_values)
    consistent = len(unique_sensitive) == 1
    
    # Confidence statistics
    confidences = [r.get('confidence', 0) for r in results if r.get('confidence') is not None]
    confidence_stats = {}
    if confidences:
        confidence_stats = {
            'mean': statistics.mean(confidences),
            'std': statistics.stdev(confidences) if len(confidences) > 1 else 0.0,
            'min': min(confidences),
            'max': max(confidences),
            'median': statistics.median(confidences)
        }
    
    # Page type counts
    page_types = [r.get('page_type') for r in results if r.get('page_type') is not None]
    page_type_counts = Counter(page_types)
    
    return {
        'image_key': image_key,
        'image_path': image_path,
        'results': results,
        'sensitive_counts': dict(sensitive_counts),
        'consistent': consistent,
        'confidence_stats': confidence_stats,
        'page_type_counts': dict(page_type_counts)
    }


def run_single_image_once(image_path: str, image_key: str, run_num: int, show_port: bool = True) -> Dict:
    """
    Run detection on a single image once.
    Returns a single result dict.
    """
    try:
        vision_start = time.time()
        vision_response = analyze_screenshot.call_llm_with_image(
            image_path,
            analyze_screenshot.VISION_SYSTEM_PROMPT,
            analyze_screenshot.VISION_USER_PROMPT
        )
        vision_time = time.time() - vision_start
        
        # Parse vision response
        vision_json_str = analyze_screenshot.extract_json_from_response(vision_response) if vision_response else None
        
        if vision_json_str:
            try:
                vision_llm_obj = json.loads(vision_json_str)
                vision_llm_obj = analyze_screenshot.normalize_llm_numbers(vision_llm_obj)
            except json.JSONDecodeError:
                vision_llm_obj = None
        else:
            vision_llm_obj = None
        
        # Build result
        if not vision_llm_obj:
            result = {
                "sensitive": None,  # None for failed runs, not False
                "confidence": None,
                "error": "Vision LLM call failed or returned invalid JSON",
                "page_type": None,
                "processing_time": vision_time
            }
        else:
            result = {
                "sensitive": vision_llm_obj.get("sensitive", False),
                "confidence": vision_llm_obj.get("confidence", 0.0),
                "risk_score": vision_llm_obj.get("risk_score", 0.0),
                "primary_intent": vision_llm_obj.get("primary_intent"),
                "page_type": vision_llm_obj.get("page_type"),
                "pii_types": vision_llm_obj.get("pii_types", []),
                "quoted_evidence": vision_llm_obj.get("quoted_evidence", []),
                "reasons": vision_llm_obj.get("reasons", []),
                "processing_time": vision_time,
                "llm_raw": vision_llm_obj,
                "error": None
            }
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        return {
            'sensitive': None,
            'confidence': None,
            'error': error_msg,
            'page_type': None,
            'processing_time': 0.0
        }


def run_reproducibility_test(num_images: int = None, num_runs: int = NUM_RUNS, show_port: bool = True) -> Dict:
    """
    Run reproducibility test: all images once per run.
    Pattern: Run 1 (all images), Run 2 (all images), ..., Run N (all images)
    """
    print(f"Loading manual labels from {MANUAL_LABELS_FILE}...")
    manual_labels = load_manual_labels()
    
    if not manual_labels:
        print("No manual labels found!")
        return {}
    
    print(f"Found {len(manual_labels)} labeled images")
    
    # Filter to only images that exist
    valid_images = []
    for image_key, label_data in manual_labels.items():
        image_path = label_data.get('image_path')
        if image_path and Path(image_path).exists():
            valid_images.append((image_key, image_path, label_data))
        else:
            print(f"  Skipping {image_key}: image not found at {image_path}")
    
    if num_images:
        valid_images = valid_images[:num_images]
        print(f"Testing first {len(valid_images)} images...")
    else:
        print(f"Testing all {len(valid_images)} images...")
    
    print(f"\nRunning {num_runs} complete passes through all images...")
    print("=" * 80)
    
    # Store results: image_key -> list of results (one per run)
    image_results = {img_key: [] for img_key, _, _ in valid_images}
    run_stats = []  # Stats after each run
    start_time = time.time()
    
    # Run all images once per round
    for run_num in range(1, num_runs + 1):
        print(f"\n{'=' * 80}")
        print(f"RUN {run_num}/{num_runs} - Processing all {len(valid_images)} images")
        print("=" * 80)
        
        run_start_time = time.time()
        run_results = []
        run_successful = 0
        run_failed = 0
        
        for idx, (image_key, image_path, label_data) in enumerate(valid_images, 1):
            result = run_single_image_once(image_path, image_key, run_num, show_port)
            image_results[image_key].append(result)
            run_results.append(result)
            
            if result.get('error'):
                run_failed += 1
            else:
                run_successful += 1
            
            # Small delay between images
            if idx < len(valid_images):
                time.sleep(0.2)
            
            # Show progress every 50 images
            if idx % 50 == 0 or idx == len(valid_images):
                print(f"  Progress: {idx}/{len(valid_images)} images processed...")
        
        run_time = time.time() - run_start_time
        
        # Calculate stats for this run
        sensitive_values = [r.get('sensitive') for r in run_results if r.get('sensitive') is not None]
        sensitive_true = sum(1 for v in sensitive_values if v is True)
        sensitive_false = sum(1 for v in sensitive_values if v is False)
        
        run_stat = {
            'run_num': run_num,
            'total_images': len(valid_images),
            'successful': run_successful,
            'failed': run_failed,
            'sensitive_count': sensitive_true,
            'not_sensitive_count': sensitive_false,
            'run_time_seconds': run_time,
            'avg_time_per_image': run_time / len(valid_images) if valid_images else 0
        }
        run_stats.append(run_stat)
        
        print(f"\nRun {run_num} Summary:")
        print(f"  Successful: {run_successful}/{len(valid_images)}")
        print(f"  Failed: {run_failed}/{len(valid_images)}")
        print(f"  Sensitive: {sensitive_true}, Not Sensitive: {sensitive_false}")
        print(f"  Time: {run_time:.1f}s (avg: {run_time/len(valid_images):.1f}s per image)")
    
    total_time = time.time() - start_time
    
    # Analyze results: check consistency across runs for each image
    all_results = []
    inconsistent_details = []
    misclassifications = []  # Images where LLM disagrees with manual labels
    
    for image_key, results in image_results.items():
        # Get image path and manual label
        image_path = None
        manual_label = None
        for img_key, img_path, label_data in valid_images:
            if img_key == image_key:
                image_path = img_path
                manual_label = label_data.get('label')  # 'sensitive' or 'not_sensitive'
                break
        
        # Analyze consistency
        sensitive_values = [r.get('sensitive') for r in results if r.get('sensitive') is not None]
        unique_sensitive = set(sensitive_values)
        consistent = len(unique_sensitive) == 1
        
        # Count sensitive values
        sensitive_counts = Counter(sensitive_values)
        
        # Confidence statistics
        confidences = [r.get('confidence', 0) for r in results if r.get('confidence') is not None]
        confidence_stats = {}
        if confidences:
            confidence_stats = {
                'mean': statistics.mean(confidences),
                'std': statistics.stdev(confidences) if len(confidences) > 1 else 0.0,
                'min': min(confidences),
                'max': max(confidences),
                'median': statistics.median(confidences)
            }
        
        # Page type counts
        page_types = [r.get('page_type') for r in results if r.get('page_type') is not None]
        page_type_counts = Counter(page_types)
        
        # Check for misclassification (compare with manual label)
        most_common_sensitive = Counter(sensitive_values).most_common(1)[0][0] if sensitive_values else None
        manual_sensitive = None
        if manual_label == 'sensitive':
            manual_sensitive = True
        elif manual_label == 'not_sensitive':
            manual_sensitive = False
        
        if manual_sensitive is not None and most_common_sensitive is not None:
            if most_common_sensitive != manual_sensitive:
                misclassifications.append({
                    'image_key': image_key,
                    'manual_label': manual_label,
                    'llm_result': 'sensitive' if most_common_sensitive else 'not_sensitive',
                    'llm_distribution': dict(sensitive_counts)
                })
        
        result_summary = {
            'image_key': image_key,
            'image_path': image_path,
            'results': results,
            'sensitive_counts': dict(sensitive_counts),
            'consistent': consistent,
            'confidence_stats': confidence_stats,
            'page_type_counts': dict(page_type_counts)
        }
        all_results.append(result_summary)
        
        # Track inconsistencies
        if not consistent:
            value_counts = {}
            for val in unique_sensitive:
                value_counts[val] = sensitive_values.count(val)
            
            inconsistent_details.append({
                'image_key': image_key,
                'sensitive_counts': dict(sensitive_counts),
                'value_distribution': value_counts,
                'confidence_stats': confidence_stats,
                'page_type_counts': dict(page_type_counts),
                'num_successful_runs': len([r for r in results if r.get('sensitive') is not None]),
                'num_failed_runs': len([r for r in results if r.get('error')])
            })
    
    # Aggregate statistics
    total_images = len(all_results)
    consistent_images = sum(1 for r in all_results if r['consistent'])
    inconsistent_images = total_images - consistent_images
    
    summary = {
        'test_config': {
            'num_images_tested': total_images,
            'num_runs': num_runs,
            'total_time_seconds': total_time,
            'avg_time_per_image': total_time / (total_images * num_runs) if total_images > 0 else 0
        },
        'run_stats': run_stats,  # Stats after each run
        'reproducibility_stats': {
            'total_images': total_images,
            'consistent_images': consistent_images,
        'inconsistent_images': inconsistent_images,
        'consistency_rate': (consistent_images / total_images * 100) if total_images > 0 else 0,
        'total_runs': total_images * num_runs,
        'successful_runs': sum(len([r for r in res['results'] if r.get('sensitive') is not None]) for res in all_results),
        'failed_runs': sum(len([r for r in res['results'] if r.get('error')]) for res in all_results)
    },
    'inconsistent_images': inconsistent_details,
    'misclassifications': misclassifications,
    'all_results': all_results
}
    
    return summary


def print_summary(summary: Dict):
    """Print a formatted summary with detailed inconsistency analysis."""
    print("\n" + "=" * 80)
    print("REPRODUCIBILITY TEST SUMMARY")
    print("=" * 80)
    
    config = summary['test_config']
    stats = summary['reproducibility_stats']
    run_stats = summary.get('run_stats', [])
    
    print(f"\nTest Configuration:")
    print(f"  Images tested: {config['num_images_tested']}")
    print(f"  Number of runs: {config['num_runs']}")
    print(f"  Total time: {config['total_time_seconds']:.1f}s ({config['total_time_seconds']/60:.1f} minutes)")
    print(f"  Avg time per image per run: {config['avg_time_per_image']:.1f}s")
    
    # Show stats for each run
    if run_stats:
        print(f"\n{'=' * 80}")
        print("RUN-BY-RUN STATISTICS")
        print("=" * 80)
        for run_stat in run_stats:
            print(f"\n  Run {run_stat['run_num']}:")
            print(f"    Successful: {run_stat['successful']}/{run_stat['total_images']}")
            print(f"    Failed: {run_stat['failed']}/{run_stat['total_images']}")
            print(f"    Sensitive: {run_stat['sensitive_count']}, Not Sensitive: {run_stat['not_sensitive_count']}")
            print(f"    Time: {run_stat['run_time_seconds']:.1f}s (avg: {run_stat['avg_time_per_image']:.1f}s per image)")
    
    print(f"\n{'=' * 80}")
    print("REPRODUCIBILITY RESULTS")
    print("=" * 80)
    print(f"  Consistent images: {stats['consistent_images']}/{stats['total_images']} ({stats['consistency_rate']:.1f}%)")
    print(f"  Inconsistent images: {stats['inconsistent_images']}/{stats['total_images']} ({100 - stats['consistency_rate']:.1f}%)")
    print(f"\n  Total runs: {stats['total_runs']}")
    print(f"  Successful runs: {stats['successful_runs']} ({stats['successful_runs']/stats['total_runs']*100:.1f}%)")
    print(f"  Failed runs: {stats['failed_runs']} ({stats['failed_runs']/stats['total_runs']*100:.1f}%)")
    
    # Show inconsistent images
    inconsistent = summary.get('inconsistent_images', [])
    if inconsistent:
        print(f"\n{'=' * 80}")
        print(f"INCONSISTENT IMAGES ({len(inconsistent)} total)")
        print("=" * 80)
        print("These images gave DIFFERENT results across multiple runs:")
        print()
        
        # Show first 20 inconsistent images
        for idx, img in enumerate(inconsistent[:20], 1):
            print(f"\n  {idx}. {img['image_key']}")
            print(f"     Sensitive value distribution:")
            for val, count in sorted(img['value_distribution'].items(), key=lambda x: -x[1]):
                val_str = "True" if val else "False" if val is False else "None"
                print(f"       {val_str}: {count} times")
            
            if img['confidence_stats']:
                cs = img['confidence_stats']
                print(f"     Confidence: mean={cs.get('mean', 0):.2f}, std={cs.get('std', 0):.3f}, "
                      f"range=[{cs.get('min', 0):.2f}, {cs.get('max', 0):.2f}]")
            
            if img['page_type_counts']:
                print(f"     Page types: {img['page_type_counts']}")
            
            print(f"     Successful runs: {img['num_successful_runs']}/{config['num_runs_per_image']}")
            if img['num_failed_runs'] > 0:
                print(f"     Failed runs: {img['num_failed_runs']}")
        
        if len(inconsistent) > 20:
            print(f"\n  ... and {len(inconsistent) - 20} more inconsistent images")
            print(f"  (See full results in --output JSON file for details)")
    else:
        print(f"\n{'=' * 80}")
        print("PERFECT REPRODUCIBILITY")
        print("=" * 80)
        print("All images gave consistent results across all runs.")
    
    # Show misclassifications (LLM vs manual labels)
    misclassifications = summary.get('misclassifications', [])
    if misclassifications:
        print(f"\n{'=' * 80}")
        print(f"MISCLASSIFICATIONS ({len(misclassifications)} total)")
        print("=" * 80)
        print("Images where LLM disagrees with manual labels:")
        print()
        for idx, mis in enumerate(misclassifications, 1):
            print(f"  {idx}. {mis['image_key']}")
            print(f"     Manual: {mis['manual_label']}, LLM: {mis['llm_result']}")
            print(f"     Distribution: {mis['llm_distribution']}")
    
    print(f"\n{'=' * 80}")
    print("KEY METRICS")
    print("=" * 80)
    print(f"  Reproducibility Rate: {stats['consistency_rate']:.1f}%")
    print(f"  Success Rate: {stats['successful_runs']/stats['total_runs']*100:.1f}%")
    if inconsistent:
        print(f"  WARNING: {len(inconsistent)} images showed inconsistency (different results across runs)")
        print(f"     This suggests the model is non-deterministic or there are port/model differences")
    else:
        print(f"  All results were consistent (perfect reproducibility)")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Test reproducibility of vision detection')
    parser.add_argument('--num-images', type=int, default=None,
                       help='Limit number of images to test (default: all)')
    parser.add_argument('--num-runs', type=int, default=NUM_RUNS,
                       help=f'Number of runs per image (default: {NUM_RUNS})')
    parser.add_argument('--port', type=int, default=None,
                       help='Single port to use for sequential mode (e.g., 52000)')
    parser.add_argument('--round-robin', action='store_true',
                       help='Use round-robin mode across multiple ports')
    parser.add_argument('--ports', type=str, default="52000,53000,54000,55000",
                       help='Comma-separated ports for round-robin mode (default: 52000,53000,54000,55000)')
    parser.add_argument('--output', type=str, default=None,
                       help='Save results to JSON file')
    
    args = parser.parse_args()
    
    # Configure LLM mode and ports
    if args.round_robin:
        # Parse ports
        try:
            port_list = [int(p.strip()) for p in args.ports.split(',')]
            if not port_list:
                print("Error: --ports must specify at least one port")
                sys.exit(1)
            analyze_screenshot.set_llm_mode("round_robin")
            analyze_screenshot.set_round_robin_ports(port_list)
            ports_info = port_list
            print(f"Using round-robin mode with ports: {ports_info}")
        except ValueError as e:
            print(f"Error: Invalid port format. Use comma-separated integers (e.g., '52000,53000,54000,55000'): {e}")
            sys.exit(1)
    else:
        # Sequential mode
        if args.port:
            port = args.port
        else:
            port = 52000  # Default
        analyze_screenshot.set_llm_mode("sequential")
        analyze_screenshot._SEQUENTIAL_PORT = port
        ports_info = [port]
        print(f"Using sequential mode with port: {port}")
    
    # Verify configuration
    actual_mode = analyze_screenshot._LLM_MODE
    test_host = analyze_screenshot._get_llm_host()
    
    print("=" * 80)
    print("VISION DETECTION REPRODUCIBILITY TEST")
    print("=" * 80)
    print(f"LLM Mode: {actual_mode}")
    if len(ports_info) == 1:
        print(f"Port: {ports_info[0]}")
    else:
        print(f"Ports (round-robin): {ports_info}")
    print(f"Model: {analyze_screenshot._LLM_MODEL}")
    print(f"Temperature: {analyze_screenshot.LLM_TEMPERATURE}")
    print(f"Seed: {analyze_screenshot.LLM_SEED}")
    print(f"Number of runs: {args.num_runs}")
    if args.num_images:
        print(f"Limited to first {args.num_images} images")
    print()
    
    # Verify port matches (for sequential mode)
    if len(ports_info) == 1:
        if f":{ports_info[0]}" not in test_host:
            print(f"WARNING: Port mismatch! Expected port {ports_info[0]} but _get_llm_host() returns {test_host}")
            print()
    
    summary = run_reproducibility_test(
        num_images=args.num_images,
        num_runs=args.num_runs,
        show_port=(len(ports_info) > 1)  # Show port for each run if round-robin
    )
    
    print_summary(summary)
    
    if args.output:
        output_path = Path(args.output)
        print(f"\nSaving results to {output_path}...")
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"Results saved!")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()

