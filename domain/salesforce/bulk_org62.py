"""
Bulk org62 Salesforce queries.
Single SOQL returns data for ALL accounts at once.
"""
from domain.salesforce.org62_client import get_sf_client
from domain.salesforce.org62_client import sf_query


def get_red_accounts_bulk(account_ids: list[str]) -> dict:
    """
    Single SOQL query for all red accounts.
    Returns {account_id: red_account_data}.

    Filters: ``Stage__c IN ('Open', 'Precautionary')`` only; commerce-related
    ``Issue_Product__c`` via LIKE (null / blank product excludes the row — unknown product).
    Selected ``Issue_Product__c`` is exposed as ``issue_product`` on each value dict.
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
        AND Stage__c IN ('Open', 'Precautionary')
        AND (
            Issue_Product__c LIKE '%Commerce%'
            OR Issue_Product__c LIKE '%B2C%'
            OR Issue_Product__c LIKE '%B2B%'
            OR Issue_Product__c LIKE '%Order Management%'
        )
        ORDER BY Days_Red__c DESC
    """

    result = sf.query_all(query)
    records = result.get("records", [])

    # Build lookup map
    red_map = {}
    for r in records:
        acct_id = r.get("Red_Account__c", "")
        if acct_id:
            rid = str(r.get("Id") or "")
            red_map[acct_id[:15]] = {
                "red_account_id": rid,
                "red_account_url": (
                    f"https://org62.lightning.force.com/lightning/r/Red_Account__c/{rid}/view"
                    if rid
                    else ""
                ),
                "stage": r.get("Stage__c", ""),
                "acv_at_risk": r.get("ACV_at_Risk__c", 0),
                "latest_updates": r.get("Latest_Updates__c", ""),
                "action_plan": r.get("Action_Plan__c", ""),
                "issue_product": str(r.get("Issue_Product__c") or "").strip(),
                "days_red": r.get("Days_Red__c", 0),
            }

    return red_map


def get_opp_dynamic_fields_bulk(opp_ids: list[str]) -> dict[str, dict]:
    """
    Fetch dynamic org62 fields for up to 500 opp IDs in one SOQL query.
    Returns dict keyed by opp Id (both 15- and 18-char forms alias the same bucket).
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
                       Description,
                       StageName,
                       CloseDate,
                       IsClosed,
                       sfbase__PriorContractStartDate__c,
                       sfbase__PriorContractTerm__c,
                       Account.Id,
                       Account.Name
                FROM Opportunity
                WHERE Id IN ('{id_list}')"""
        )
        for rec in (data.get("records") or []):
            oid_18 = str(rec.get("Id") or "").strip()
            oid_15 = oid_18[:15] if oid_18 else ""
            if not oid_15:
                continue
            pct = rec.get("sfbase__PriorContractTerm__c")
            try:
                prior_term = int(float(pct)) if pct not in (None, "") else None
            except (TypeError, ValueError):
                prior_term = None
            bucket = {
                "description": str(rec.get("Description") or "").strip(),
                "stage": str(rec.get("StageName") or "").strip(),
                "close_date": str(rec.get("CloseDate") or "").strip(),
                "is_closed": bool(rec.get("IsClosed") or False),
                "prior_contract_start_date": str(
                    rec.get("sfbase__PriorContractStartDate__c") or ""
                ).strip(),
                "prior_contract_term_months": prior_term,
                "account_id": str((rec.get("Account") or {}).get("Id") or "")[:15],
                "account_name": str((rec.get("Account") or {}).get("Name") or "").strip(),
            }
            result[oid_15] = bucket
            result[oid_18] = bucket
    return result
