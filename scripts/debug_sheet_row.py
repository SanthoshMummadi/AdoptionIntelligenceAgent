from domain.integrations.gsheet_exporter import HEADERS_22

print("Current 22 columns:")
for i, h in enumerate(HEADERS_22, 1):
    print(f"  {i:2}. {h}")
