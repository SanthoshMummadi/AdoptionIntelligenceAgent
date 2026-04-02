import traceback
from datetime import date

from domain.integrations.gsheet_exporter import export_to_gsheet

reviews = [
    {
        "account_name": "Adidas AG",
        "account_id": "00130000002xFEIAA2",
        "opp": {
            "Name": "Adidas AG Renewal 31003 Commerce Cloud",
            "CloseDate": "2027-01-31",
            "StageName": "01 Initiate",
            "Forecasted_Attrition__c": 695492,
            "Swing__c": 0,
            "License_At_Risk_Reason__c": "Financial & Contractual",
            "ACV_Reason_Detail__c": "",
            "NextStep": "",
            "Specialist_Sales_Notes__c": "Test notes",
            "Manager_Forecast_Judgement__c": "Best Case",
            "Description": "",
        },
        "snowflake_display": {
            "ari_category": "High",
            "ari_probability": "77.3%",
            "ari_reason": "Financial & Contractual",
            "health_score": 63,
            "health_literal": "Yellow",
            "health_display": "Yellow (63)",
            "cc_aov": "$1.6M",
            "utilization_rate": "87.2%",
            "util_emoji": ":large_green_circle:",
        },
        "enrichment": {
            "renewal_aov": {
                "renewal_aov": 1608311,
                "renewal_atr": 810000,
                "csg_geo": "EMEA",
            }
        },
        "red_account": {
            "Stage__c": "Precautionary",
            "Days_Red__c": 17,
            "Latest_Updates__c": "Mar-10: Test update",
        },
        "all_products_attrition": [
            {
                "APM_LVL_1": "Commerce",
                "APM_LVL_2": "B2C Commerce",
                "APM_LVL_3": "B2C Commerce (B2Ce)",
            },
            {"APM_LVL_1": "Sales", "APM_LVL_2": "Sales Cloud"},
        ],
        "risk_notes": "- Risk note 1\n- Risk note 2",
        "recommendation": "- Recommendation 1\n- Recommendation 2",
        "adoption_pov": "Test adoption POV",
    }
]

print("Testing full export...")
try:
    sheet_name = date.today().strftime("GM Review %Y-%m-%d")
    url = export_to_gsheet(reviews, sheet_name=sheet_name)
    if url:
        print("✓ Export successful!")
        print(f"  URL: {url}")
    else:
        print("❌ Export returned empty URL")
except Exception:
    print("❌ Export error:")
    traceback.print_exc()
