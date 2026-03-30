import requests, re

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.cricbuzz.com",
}

r = requests.get("https://www.cricbuzz.com/live-cricket-scores/149618/", headers=headers, timeout=20)
print("Status:", r.status_code)
print("URL after redirect:", r.url)
print("HTML length:", len(r.text))

matches = re.findall(r'[A-Za-z ]{5,50}won by[^<"]{5,60}', r.text)
print("Total won-by matches found:", len(matches))
for m in matches:
    print(repr(m))

# Also check for RCB anywhere in the page
rcb_count = r.text.lower().count("royal challengers")
mi_count = r.text.lower().count("mumbai indians")
print(f"'royal challengers' appears {rcb_count} times")
print(f"'mumbai indians' appears {mi_count} times")