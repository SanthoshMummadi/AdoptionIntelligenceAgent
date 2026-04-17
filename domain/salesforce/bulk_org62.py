"""
Bulk org62 Salesforce queries.
Single SOQL returns data for ALL accounts at once.
"""
from domain.salesforce.org62_client import get_sf_client
from domain.salesforce.org62_client import sf_query


def get_red_accounts_bulk(account_ids: list[str]) -> dict:
    """
    Single SOQL query for all red accounts.
    Returns {account_id: red_account_data}
    """
    if not account_ids:
        return {}

    sf = get_sf_client()

    # Convert to 15-char for SOQL
    ids_15 = [aid[:15] for aid in account_ids if aid]
    ids_soql = "','".join(ids_15)

    query = f"""
        SELECT Id, Red_Account__c, Stage__c, ACV_at_Risk__c,
               Latest_Updates__c, Action_Plan__c, Issue_Product__c,
               Days_Red__c
        FROM Red_Account__c
        WHERE Red_Account__c IN ('{ids_soql}')
        AND Stage__c != 'Resolved'
        ORDER BY Days_Red__c DESC
    """

    result = sf.query_all(query)
    records = result.get("records", [])

    # Build lookup map
    red_map = {}
    for r in records:
        acct_id = r.get("Red_Account__c", "")
        if acct_id:
            red_map[acct_id[:15]] = {
                "stage": r.get("Stage__c", ""),
                "acv_at_risk": r.get("ACV_at_Risk__c", 0),
                "latest_updates": r.get("Latest_Updates__c", ""),
                "action_plan": r.get("Action_Plan__c", ""),
                "issue_product": r.get("Issue_Product__c", ""),
                "days_red": r.get("Days_Red__c", 0),
            }

    return red_map


def get_opp_dynamic_fields_bulk(opp_ids: list[str]) -> dict[str, dict]:
    """
    Fetch dynamic org62 fields for up to 500 opp IDs in one SOQL query.
    Returns dict keyed by opp_id_15.
    """
    if not opp_ids:
        return {}

    result: dict[str, dict] = {}
    for i in range(0, len(opp_ids), 500):
        batch = [str(x).strip() for x in opp_ids[i : i + 500] if str(x).strip()]
        if not batch:
            continue
        id_list = "','".join(batch)
        data = sf_query(
            f"""SELECT Id,
                       Forecasted_Attrition__c,
                       Description,
                       StageName,
                       CloseDate,
                       IsClosed,
                       Account.Id,
                       Account.Name
                FROM Opportunity
                WHERE Id IN ('{id_list}')"""
        )
        for rec in (data.get("records") or []):
            oid = str(rec.get("Id") or "")[:15]
            result[oid] = {
                "forecasted_attrition": float(rec.get("Forecasted_Attrition__c") or 0),
                "description": str(rec.get("Description") or "").strip(),
                "stage": str(rec.get("StageName") or "").strip(),
                "close_date": str(rec.get("CloseDate") or "").strip(),
                "is_closed": bool(rec.get("IsClosed") or False),
                "account_id": str((rec.get("Account") or {}).get("Id") or "")[:15],
                "account_name": str((rec.get("Account") or {}).get("Name") or "").strip(),
            }
    return result
