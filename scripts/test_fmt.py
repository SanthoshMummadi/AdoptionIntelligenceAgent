import sys

sys.path.insert(0, ".")

from domain.analytics.snowflake_client import fmt_amount

tests = [
    695492,
    1608311,
    14894126,
    500000,
    298893,
    142651,
    50000,
    5000000,
    0,
    810000,
]

for v in tests:
    print(f"  ${v:,.0f}  →  {fmt_amount(v)}")
