"""
org62_client.py
Salesforce org62 operations: account lookup, opportunities, team, red accounts.
"""
import os
from typing import Any, Dict, List, Optional

from simple_salesforce import Salesforce
from log_utils import log_debug, log_error

_sf_client = None


def get_sf_client():
    """Get or create Salesforce client."""
    global _sf_client
    if _sf_client is None:
        access_token = os.environ.get("SF_ACCESS_TOKEN")
        instance_url = os.environ.get("SF_INSTANCE_URL")

        if access_token and instance_url:
            _sf_client = Salesforce(
                instance_url=instance_url,
                session_id=access_token,
            )
            log_debug("✅ Connected to Salesforce org62 (via access token)")
        else:
            raise Exception(
                "Missing Salesforce credentials. "
                "Run: sf org login web --instance-url https://org62.my.salesforce.com --alias org62 "
                "then: sf org display --target-org org62 --json "
                "and copy SF_ACCESS_TOKEN and SF_INSTANCE_URL to .env"
            )

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


class Org62Client:
    """Salesforce org62 client using an explicit access token (for adapters)."""

    def __init__(self, access_token: str, instance_url: str):
        if not access_token or not instance_url:
            raise ValueError("access_token and instance_url are required")
        self._sf = Salesforce(
            instance_url=instance_url.rstrip("/"),
            session_id=access_token,
        )
        log_debug("✅ Org62Client connected (token)")

    def resolve_account_id(self, account_name: str) -> Optional[str]:
        """Resolve account name to 15/18-char Account Id."""
        try:
            q = f"""
            SELECT Id FROM Account
            WHERE Name = '{_escape(account_name)}'
            LIMIT 1
            """
            result = self._sf.query(q)
            if not result.get("records"):
                q = f"""
                SELECT Id FROM Account
                WHERE Name LIKE '%{_escape(account_name)}%'
                LIMIT 5
                """
                result = self._sf.query(q)
            if result.get("records"):
                return result["records"][0]["Id"]
            return None
        except Exception as e:
            log_error(f"Org62Client.resolve_account_id error: {e}")
            return None

    def resolve_opportunity_id(self, opp_name: str) -> Optional[str]:
        """Resolve opportunity name to Opportunity Id (first open match)."""
        try:
            q = f"""
            SELECT Id FROM Opportunity
            WHERE Name = '{_escape(opp_name)}' AND IsClosed = false
            LIMIT 1
            """
            result = self._sf.query(q)
            if result.get("records"):
                return result["records"][0]["Id"]
            q2 = f"""
            SELECT Id FROM Opportunity
            WHERE Name LIKE '%{_escape(opp_name)}%' AND IsClosed = false
            ORDER BY CloseDate ASC
            LIMIT 1
            """
            result2 = self._sf.query(q2)
            if result2.get("records"):
                return result2["records"][0]["Id"]
            return None
        except Exception as e:
            log_error(f"Org62Client.resolve_opportunity_id error: {e}")
            return None

    def get_account_details(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Return Account record fields for Id."""
        try:
            q = f"""
            SELECT Id, Name, BillingCountry, BillingCity, BillingState, Industry, Type,
                   Website, Phone, OwnerId, Owner.Name
            FROM Account
            WHERE Id = '{_escape(account_id)}'
            LIMIT 1
            """
            result = self._sf.query(q)
            if not result.get("records"):
                return None
            return result["records"][0]
        except Exception as e:
            log_error(f"Org62Client.get_account_details error: {e}")
            return None

    def get_opportunity_details(self, opp_id: str) -> Optional[Dict[str, Any]]:
        """Return Opportunity record with common renewal fields."""
        try:
            q = f"""
            SELECT
                Id, Name, StageName, Amount, CloseDate, AccountId,
                Account.Id, Account.Name, Account.BillingCountry,
                ForecastCategoryName,
                Forecasted_Attrition__c, Swing__c,
                License_At_Risk_Reason__c, ACV_Reason_Detail__c, NextStep,
                Description, Specialist_Sales_Notes__c, Manager_Forecast_Judgement__c
            FROM Opportunity
            WHERE Id = '{_escape(opp_id)}'
            LIMIT 1
            """
            result = self._sf.query(q)
            if not result.get("records"):
                return None
            return result["records"][0]
        except Exception as e:
            log_error(f"Org62Client.get_opportunity_details error: {e}")
            return None

    def get_account_team(self, account_id: str) -> List[Dict[str, Any]]:
        """Account team members as a list of {user_id, name, team_member_role}."""
        try:
            q = f"""
            SELECT UserId, User.Name, TeamMemberRole
            FROM AccountTeamMember
            WHERE AccountId = '{_escape(account_id)}'
            """
            result = self._sf.query(q)
            out: List[Dict[str, Any]] = []
            for row in result.get("records", []):
                out.append({
                    "user_id": row.get("UserId"),
                    "name": (row.get("User") or {}).get("Name", ""),
                    "team_member_role": row.get("TeamMemberRole", ""),
                })
            return out
        except Exception as e:
            log_error(f"Org62Client.get_account_team error: {e}")
            return []

    def get_red_account_info(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Red Account custom object row for this Account Id, if any."""
        try:
            q = f"""
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
            result = self._sf.query(q)
            if result.get("records"):
                return result["records"][0]
            qh = f"""
            SELECT Id, Stage__c
            FROM Red_Account__c
            WHERE Red_Account__c = '{_escape(account_id)}'
            ORDER BY CreatedDate DESC
            LIMIT 1
            """
            rh = self._sf.query(qh)
            if rh.get("records"):
                historical = rh["records"][0]
                historical["_historical"] = True
                return historical
            return None
        except Exception as e:
            log_error(f"Org62Client.get_red_account_info error: {e}")
            return None

    def get_renewal_opportunity(
        self, account_id: str, _cloud: str = "Commerce Cloud"
    ) -> Optional[Dict[str, Any]]:
        """First open renewal opportunity for account (by close date), if any."""
        try:
            q = f"""
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
            LIMIT 1
            """
            result = self._sf.query(q)
            recs = result.get("records", [])
            return recs[0] if recs else None
        except Exception as e:
            log_error(f"Org62Client.get_renewal_opportunity error: {e}")
            return None
