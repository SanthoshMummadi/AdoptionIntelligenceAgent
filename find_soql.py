import re

with open("domain/salesforce/org62_client.py", "r") as f:
    content = f.read()

matches = re.findall(r"sf\.query\([^)]{20,}", content)
for i, m in enumerate(matches):
    print(f"\n{i + 1}. {m[:200]}")

if not matches:
    print("(No sf.query(...) matches in domain/salesforce/org62_client.py)")
