#!/usr/bin/env python3
"""
Check IPs against AlienVault OTX for threat intelligence
Improved version with retry logic, resume capability, and better error handling
"""
import httpx
import csv
import json
import os
import time
import random
from dotenv import load_dotenv

load_dotenv()

# AlienVault OTX API key
otx_api_key = os.getenv("OTX_API_KEY")
if not otx_api_key:
    print("Warning: OTX_API_KEY not found in .env file")

data_dir = "../data"
ip_path = os.path.join(data_dir, "ips_all.csv")
otx_output_path = os.path.join(data_dir, "alienvault_results.jsonl")
progress_path = os.path.join(data_dir, "alienvault_progress.txt")

def check_ip_alienvault(ip, max_retries=3):
    """Check a single IP against AlienVault OTX with retry logic"""
    headers = {}
    if otx_api_key:
        headers['X-OTX-API-KEY'] = otx_api_key
    
    for attempt in range(max_retries):
        try:
            # Get general information about the IP (only this call to reduce load)
            url = f"https://otx.alienvault.com/api/v1/indicators/IPv4/{ip}/general"
            
            # Longer timeout and more generous settings
            timeout = httpx.Timeout(60.0, connect=30.0)
            response = httpx.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json()
                return data
            elif response.status_code == 404:
                return {"ip": ip, "error": f"HTTP 404 - IP not found", "status": "not_found"}
            elif response.status_code == 429:
                # Rate limited - wait longer
                wait_time = 60 + random.randint(10, 30)
                print(f"  Rate limited, waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                return {"ip": ip, "error": f"HTTP {response.status_code}", "status": "error"}
                
        except httpx.TimeoutException:
            if attempt < max_retries - 1:
                wait_time = 30 + random.randint(5, 15)
                print(f"  Timeout on attempt {attempt + 1}, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            else:
                return {"ip": ip, "error": "All retries timed out", "status": "error"}
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 10 + random.randint(5, 15)
                print(f"  Error on attempt {attempt + 1}: {e}, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            else:
                return {"ip": ip, "error": str(e), "status": "error"}
    
    return {"ip": ip, "error": "Max retries exceeded", "status": "error"}

def load_progress():
    """Load progress from file to resume where we left off"""
    if os.path.exists(progress_path):
        with open(progress_path, 'r') as f:
            return int(f.read().strip())
    return 0

def save_progress(index):
    """Save current progress"""
    with open(progress_path, 'w') as f:
        f.write(str(index))

def load_completed_ips():
    """Load already completed IPs from output file"""
    completed = set()
    if os.path.exists(otx_output_path):
        with open(otx_output_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    if 'ip' in data:
                        completed.add(data['ip'])
                except:
                    continue
    return completed

def main():
    print(f"=== IMPROVED ALIENVAULT OTX CHECKER ===")
    print(f"Input file: {ip_path}")
    print(f"Output file: {otx_output_path}")
    print(f"Progress file: {progress_path}")
    
    if not os.path.exists(ip_path):
        print(f"Error: {ip_path} not found!")
        return
    
    # Read IPs from CSV
    ips_to_check = []
    with open(ip_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        for row in reader:
            if len(row) > 1:  # CSV has index column, IP is in second column
                ip = row[1].strip()  # IP is in second column (index 1)
                ips_to_check.append(ip)
    
    print(f"Found {len(ips_to_check)} IPs to check")
    
    # Load progress and completed IPs
    start_index = load_progress()
    completed_ips = load_completed_ips()
    
    print(f"Resuming from index {start_index}")
    print(f"Already completed: {len(completed_ips)} IPs")
    
    # Skip already completed IPs
    ips_to_process = [ip for i, ip in enumerate(ips_to_check) if i >= start_index and ip not in completed_ips]
    print(f"Remaining IPs to process: {len(ips_to_process)}")
    
    # Check each IP
    success_count = 0
    error_count = 0
    
    for i, ip in enumerate(ips_to_process):
        current_index = start_index + i
        print(f"Checking {current_index + 1}/{len(ips_to_check)}: {ip}")
        
        result = check_ip_alienvault(ip)
        
        # Save result immediately
        with open(otx_output_path, "a") as out_file:
            json.dump(result, out_file)
            out_file.write("\n")
        
        # Update progress
        save_progress(current_index + 1)
        
        # Count results
        if 'error' in result or result.get('status') == 'error':
            error_count += 1
        else:
            success_count += 1
        
        # Print progress every 10 IPs
        if (i + 1) % 10 == 0:
            print(f"  Progress: {i + 1}/{len(ips_to_process)} processed")
            print(f"  Success: {success_count}, Errors: {error_count}")
        
        # Rate limiting - be nice to the API with random delays
        delay = 2 + random.uniform(1, 3)  # 2-5 seconds
        time.sleep(delay)
    
    # Final summary
    print(f"\n=== ALIENVAULT CHECK COMPLETE ===")
    print(f"Total IPs processed: {len(ips_to_process)}")
    print(f"Successful lookups: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Success rate: {(success_count / len(ips_to_process)) * 100:.1f}%")
    print(f"Results saved to: {otx_output_path}")
    
    # Quick summary of threat IPs
    threat_ips = []
    with open(otx_output_path, 'r') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                if 'pulse_info' in data and data['pulse_info'].get('count', 0) > 0:
                    threat_ips.append(data)
            except:
                continue
    
    print(f"\nIPs with threat indicators: {len(threat_ips)}")
    
    if threat_ips:
        print("\nTop threat IPs:")
        for result in sorted(threat_ips, key=lambda x: x.get('pulse_info', {}).get('count', 0), reverse=True)[:5]:
            ip = result.get('indicator', 'unknown')
            count = result.get('pulse_info', {}).get('count', 0)
            print(f"  {ip}: {count} threat indicators")

if __name__ == "__main__":
    main() 