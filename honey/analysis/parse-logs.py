import pandas as pd
import os

log_file = os.getenv("ACCESS_LOG_FILE", "../logs/access.log")
df = pd.read_json(log_file, lines=True)


print(f"Original shape: {len(df)} rows, {len(df.columns)} columns")
print(f"Timestamp column dtype: {df['ts'].dtype}")
print(f"Sample timestamps: {df['ts'].head()}")

# Convert Unix timestamps to datetime
df['ts'] = pd.to_datetime(df['ts'], unit='s')
print(f"After conversion - Sample timestamps: {df['ts'].head()}")

start_datetime = pd.to_datetime('2025-07-25 00:00:00')
end_datetime = pd.to_datetime('2025-08-05 23:59:59')
df = df[(df['ts'] >= start_datetime) & (df['ts'] <= end_datetime)]
print(f"After date filtering: {len(df)} rows, {len(df.columns)} columns")

headers = pd.json_normalize(df["request"])
print(f"Headers shape after normalization: {len(headers)} rows, {len(headers.columns)} columns")
print(f"Header columns: {list(headers.columns)}")
print(f"Sample URI: {headers['uri'].head()}")
print(f"Sample host: {headers['host'].head()}")

# Analyze subdomain distribution
print(f"\nSubdomain analysis:")
subdomain_counts = headers['host'].value_counts()
print(f"Subdomain distribution:")
for subdomain, count in subdomain_counts.items():
    print(f"  {subdomain}: {count} requests")


# Filter by target subdomains (3,4,5,6,7)
target_subdomains = ['3.sheep-savvy.com', '4.sheep-savvy.com', '5.sheep-savvy.com', '6.sheep-savvy.com', '7.sheep-savvy.com']
filtered_headers = headers[headers['host'].isin(target_subdomains)]
print(f"After subdomain filtering: {len(filtered_headers)} rows (only 3,4,5,6,7)")

public_tokens = [
    'apuw',  # AnyRun
    'hpua',  # HybridAnalysis 
    'jpua',  # JoeSandbox 
    'rpua',  # Radar
    'upua',  # URLScan  
]
public_requests = filtered_headers[filtered_headers['uri'].str.contains('|'.join(public_tokens), na=False)]
print(f"After public token filtering: {len(public_requests)} rows (only public submissions)")


all_ips = public_requests["headers.X-Forwarded-For"].explode().str.split(",").explode().str.strip().unique().tolist()
pd.DataFrame(all_ips, columns=["IP"]).to_csv("../data/ips_all.csv")

# Debug: Check what paths we're actually matching (using filtered data)
print(f"\nPath analysis (filtered data):")
print(f"Total URIs in public_requests: {len(public_requests)}")
print(f"URIs containing '/news/': {len(public_requests[public_requests['uri'].str.contains('/news/')])}")
print(f"URIs containing '/secret/login/': {len(public_requests[public_requests['uri'].str.contains('/secret/login/')])}")
print(f"URIs containing UUID: {len(public_requests[public_requests['uri'].str.contains('562210be-067c-4a62-b8a8-df27f3893a80')])}")
print(f"URIs containing '/dashboard/': {len(public_requests[public_requests['uri'].str.contains('/dashboard/')])}")

# Show sample URIs for each category (from filtered data)
print(f"\nSample URIs for each category (filtered):")
print(f"News URIs: {public_requests[public_requests['uri'].str.contains('/news/')]['uri'].head().tolist()}")
print(f"Secret/login URIs: {public_requests[public_requests['uri'].str.contains('/secret/login/')]['uri'].head().tolist()}")
print(f"Dashboard URIs: {public_requests[public_requests['uri'].str.contains('/dashboard/')]['uri'].head().tolist()}")


boring_ips = public_requests.loc[public_requests["uri"].str.contains("/news/")]["headers.X-Forwarded-For"].explode().str.split(",").explode().str.strip().unique().tolist()
pd.DataFrame(boring_ips, columns=["IP"]).to_csv("../data/ips_boring.csv")

# Sensitive-login honeypages (but not entropy-path)
sensitive_ips = public_requests.loc[public_requests["uri"].str.contains("/secret/login/") & ~public_requests["uri"].str.contains("562210be-067c-4a62-b8a8-df27f3893a80")]["headers.X-Forwarded-For"].explode().str.split(",").explode().str.strip().unique().tolist()
pd.DataFrame(sensitive_ips, columns=["IP"]).to_csv("../data/ips_sensitive_login.csv")

# Entropy-path honeypages (with UUID)
entropy_path_ips = public_requests.loc[public_requests["uri"].str.contains("562210be-067c-4a62-b8a8-df27f3893a80")]["headers.X-Forwarded-For"].explode().str.split(",").explode().str.strip().unique().tolist()
pd.DataFrame(entropy_path_ips, columns=["IP"]).to_csv("../data/ips_entropy_path.csv")

# Entropy-query honeypages (dashboard with auth params)
entropy_query_ips = public_requests.loc[public_requests["uri"].str.contains("/dashboard/")]["headers.X-Forwarded-For"].explode().str.split(",").explode().str.strip().unique().tolist()
pd.DataFrame(entropy_query_ips, columns=["IP"]).to_csv("../data/ips_entropy_query.csv")

print(f"  All honeypages: {len(all_ips)} unique IPs")
print(f"  Boring: {len(boring_ips)} unique IPs")
print(f"  Sensitive-login: {len(sensitive_ips)} unique IPs")
print(f"  Entropy-path: {len(entropy_path_ips)} unique IPs")
print(f"  Entropy-query: {len(entropy_query_ips)} unique IPs")


print(f"\nScanner identification:")
print(f"From headers: {headers['headers.From'].apply(str).unique()}")
print(f"X-Scanned-By headers: {headers['headers.X-Scanned-By'].apply(str).unique()}")

# Add service detection columns based on service codes in URL paths
# Service codes: u=URLScan, r=Radar, q=URLQuery, a=AnyRun, j=JoeSandbox, h=HybridAnalysis
df["urlscan"] = df["request.uri"].str.contains(r"/[0-9]+/u[pr][uw]", regex=True)
df["radar"] = df["request.uri"].str.contains(r"/[0-9]+/r[pr][uw]", regex=True)
df["urlquery"] = df["request.uri"].str.contains(r"/[0-9]+/q[pr][uw]", regex=True)
df["anyrun"] = df["request.uri"].str.contains(r"/[0-9]+/a[pr][uw]", regex=True)
df["joesandbox"] = df["request.uri"].str.contains(r"/[0-9]+/j[pr][uw]", regex=True)
df["hybridanalysis"] = df["request.uri"].str.contains(r"/[0-9]+/h[pr][uw]", regex=True)

# Add submission type detection (api vs website)
df["api_submission"] = df["request.uri"].str.contains(r"/[0-9]+/[a-z][pr]a", regex=True)
df["website_submission"] = df["request.uri"].str.contains(r"/[0-9]+/[a-z][pr]w", regex=True)

# Add visibility type detection (public, private, unlisted)
df["public_visibility"] = df["request.uri"].str.contains(r"/[0-9]+/[a-z]pu", regex=True)
df["private_visibility"] = df["request.uri"].str.contains(r"/[0-9]+/[a-z]pr", regex=True)
df["unlisted_visibility"] = df["request.uri"].str.contains(r"/[0-9]+/[a-z]u[uw]", regex=True)

# Create a combined service column for easier analysis
def get_service_from_uri(uri):
    """Extract service name from URI based on service code"""
    import re
    match = re.search(r"/[0-9]+/([a-z])[pr][uw]", uri)
    if match:
        service_code = match.group(1)
        service_map = {
            'u': 'URLScan',
            'r': 'Radar', 
            'q': 'URLQuery',
            'a': 'AnyRun',
            'j': 'JoeSandbox',
            'h': 'HybridAnalysis'
        }
        return service_map.get(service_code, 'Unknown')
    return 'Unknown'

df["detected_service"] = df["request.uri"].apply(get_service_from_uri)

# Create a combined submission type column
def get_submission_type_from_uri(uri):
    """Extract submission type from URI"""
    if re.search(r"/[0-9]+/[a-z][pr]a", uri):
        return 'API'
    elif re.search(r"/[0-9]+/[a-z][pr]w", uri):
        return 'Website'
    return 'Unknown'

df["detected_submission_type"] = df["request.uri"].apply(get_submission_type_from_uri)

# Create a combined visibility type column
def get_visibility_type_from_uri(uri):
    """Extract visibility type from URI"""
    if re.search(r"/[0-9]+/[a-z]pu", uri):
        return 'Public'
    elif re.search(r"/[0-9]+/[a-z]pr", uri):
        return 'Private'
    elif re.search(r"/[0-9]+/[a-z]u[uw]", uri):
        return 'Unlisted'
    return 'Unknown'

df["detected_visibility_type"] = df["request.uri"].apply(get_visibility_type_from_uri)

# Print service detection analysis
print(f"\nService detection analysis:")
print(f"URLScan requests: {df['urlscan'].sum()}")
print(f"Radar requests: {df['radar'].sum()}")
print(f"URLQuery requests: {df['urlquery'].sum()}")
print(f"AnyRun requests: {df['anyrun'].sum()}")
print(f"JoeSandbox requests: {df['joesandbox'].sum()}")
print(f"HybridAnalysis requests: {df['hybridanalysis'].sum()}")

print(f"\nDetected services distribution:")
service_counts = df['detected_service'].value_counts()
for service, count in service_counts.items():
    if service != 'Unknown':
        print(f"  {service}: {count} requests")

print(f"\nDetected submission types distribution:")
submission_counts = df['detected_submission_type'].value_counts()
for sub_type, count in submission_counts.items():
    if sub_type != 'Unknown':
        print(f"  {sub_type}: {count} requests")

print(f"\nDetected visibility types distribution:")
visibility_counts = df['detected_visibility_type'].value_counts()
for vis_type, count in visibility_counts.items():
    if vis_type != 'Unknown':
        print(f"  {vis_type}: {count} requests")

