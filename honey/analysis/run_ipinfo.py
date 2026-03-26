import httpx
import csv
import json
import os
from dotenv import load_dotenv

load_dotenv()

ipinfo_api = os.getenv("IPINFO_APIKEY")

data_dir = "../data"
ip_path = os.path.join(data_dir, "ips_all.csv")
ip_new_path = os.path.join(data_dir, "ips.new.txt")
hostname_path = os.path.join(data_dir, "hostnames.new.txt")
ipinfo_output_path = os.path.join(data_dir, "ipinfo.jsonl")


with open(ip_path) as csvfile:
    ip_reader = csv.reader(csvfile)
    next(ip_reader, None)
    for row in ip_reader:
        ip = row[1].strip()
        print(ip)
        print(row[0])
        res = httpx.get(f"https://ipinfo.io/{ip}?token={ipinfo_api}")
        with open(ipinfo_output_path, "a") as out_file:
            print(res.text)
            res_json = res.json()
            json.dump(res_json, out_file)
            out_file.write("\n")

            if "ip" in res_json:
                with open(ip_new_path, 'a') as f:
                    f.write("http://" + res_json["ip"] + "\n")
            if "hostname" in res_json:
                with open(hostname_path, 'a') as f:
                    f.write("http://" + res_json["hostname"] + "\n")