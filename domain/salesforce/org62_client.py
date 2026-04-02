"""
org62_client.py
Salesforce org62 operations: account lookup, opportunities, team, red accounts.
"""
import os
from simple_salesforce import Salesforce
from log_utils import log_debug, log_error

_sf_client = None


def get_sf_client():
    """Get or create Salesforce client."""
    global _sf_client
    if _sf_client is None:
        username = os.environ.get("SALESFORCE_USERNAME")
        password = os.environ.get("SALESFORCE_PASSWORD")
        security_token = os.environ.get("SALESFORCE_SECURITY_TOKEN")

        if not all([username, password, security_token]):
            raise Exception("Missing Salesforce credentials in .env")

        _sf_client = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
        )
        log_debug("✅ Connected to Salesforce org62")

    return _sf_client


def _escape(s: str) -> str:
    """Escape single quotes for SOQL."""
    return str(s).replace("'", "\\'")


def resolve_account(account_name: str, cloud: str = "Commerce Cloud") -> dict:
    """
    Resolve account name to ID and details.
    Returns: {"id": "001xxx", "name": "Acme Corp", "country": "US"}
    """
    sf = get_sf_client()

    try:
        # Try exact match first
        query = f"""
        SELECT Id, Name, BillingCountry
        FROM Account
        WHERE Name = '{_escape(account_name)}'
        LIMIT 1
        """
        result = sf.query(query)

        if not result.get("records"):
            # Try LIKE search
            query = f"""
            SELECT Id, Name, BillingCountry
            FROM Account
            WHERE Name LIKE '%{_escape(account_name)}%'
            LIMIT 5
            """
            result = sf.query(query)

        if not result.get("records"):
            return None

        # Get first match
        account = result["records"][0]

        return {
            "id": account["Id"],
            "name": account["Name"],
            "country": account.get("BillingCountry", ""),
        }

    except Exception as e:
        log_error(f"resolve_account error: {e}")
        return None


def get_renewal_opportunities(account_id: str, cloud: str = "Commerce Cloud") -> list:
    """Get renewal opportunities for account."""
    sf = get_sf_client()

    try:
        query = f"""
        SELECT
            Id, Name, StageName, Amount, CloseDate,
            AccountId, Account.Name, Account.BillingCountry,
            ForecastCategoryName,
            Forecasted_Attrition__c, Swing__c,
            License_At_Risk_Reason__c,
            ACV_Reason_Detail__c, NextStep,
            Description, Specialist_Sales_Notes__c,
            Manager_Forecast_Judgement__c
        FROM Opportunity
        WHERE AccountId = '{_escape(account_id)}'
        AND (Name LIKE '%Renewal%' OR Name LIKE '%renew%')
        AND IsClosed = false
        ORDER BY CloseDate ASC
        LIMIT 10
        """
        result = sf.query(query)
        return result.get("records", [])

    except Exception as e:
        log_error(f"get_renewal_opportunities error: {e}")
        return []


def get_red_account(account_id: str) -> dict:
    """Get Red Account record if exists."""
    sf = get_sf_client()

    try:
        query = f"""
        SELECT
            Id, Name, Stage__c, ACV_at_Risk__c,
            Days_Red__c, Red_Trending__c,
            Latest_Updates__c, Action_Plan__c,
            Issue_Product__c, Red_Account__c
        FROM Red_Account__c
        WHERE Red_Account__c = '{_escape(account_id)}'
        AND Stage__c != 'Closed'
        ORDER BY CreatedDate DESC
        LIMIT 1
        """
        result = sf.query(query)

        if result.get("records"):
            return result["records"][0]

        # Check for historical (closed) red accounts
        query_historical = f"""
        SELECT Id, Stage__c
        FROM Red_Account__c
        WHERE Red_Account__c = '{_escape(account_id)}'
        ORDER BY CreatedDate DESC
        LIMIT 1
        """
        result_historical = sf.query(query_historical)

        if result_historical.get("records"):
            historical = result_historical["records"][0]
            historical["_historical"] = True
            return historical

        return None

    except Exception as e:
        log_error(f"get_red_account error: {e}")
        return None


def get_account_team(account_id: str) -> dict:
    """Get account team (AE, CSM, Renewal Manager)."""
    sf = get_sf_client()

    try:
        query = f"""
        SELECT UserId, User.Name, TeamMemberRole
        FROM AccountTeamMember
        WHERE AccountId = '{_escape(account_id)}'
        """
        result = sf.query(query)

        team = {}
        for member in result.get("records", []):
            role = member.get("TeamMemberRole", "").lower()
            name = member.get("User", {}).get("Name", "")

            if "account executive" in role or "ae" in role:
                team["ae"] = name
            elif "csm" in role or "success" in role:
                team["csm"] = name
            elif "renewal" in role:
                team["renewal_mgr"] = name

        return team

    except Exception as e:
        log_error(f"get_account_team error: {e}")
        return {}


def expand_canvas_records_with_all_renewals(seed_records: list, cloud: str) -> list:
    """
    Given seed records (from Snowflake), fetch all renewal opps for those accounts.
    Returns list of opportunities with _account embedded.
    """
    opportunities = []

    for record in seed_records:
        account = record.get("_account", {})
        account_id = account.get("id", "")

        if not account_id:
            continue

        opps = get_renewal_opportunities(account_id, cloud)

        for opp in opps:
            opp["_account"] = account
            opp["Account"] = {
                "Id": account.get("id"),
                "Name": account.get("name"),
                "BillingCountry": account.get("country", ""),
            }
            opportunities.append(opp)

    return opportunities
