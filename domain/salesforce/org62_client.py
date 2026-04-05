"""
domain/salesforce/org62_client.py
Salesforce org62 operations: account lookup, opportunities, team, red accounts.

Loads `.env` from the repository root (not only CWD). Accepts SF_* and SALESFORCE_* tokens.
"""
import os
import re
import threading
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError
from log_utils import log_debug, log_error

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")
load_dotenv()

_sf_client = None


def _sf_max_concurrent() -> int:
    try:
        return max(1, int(os.getenv("SF_MAX_CONCURRENT", "10")))
    except ValueError:
        return 10


_SF_SEMAPHORE = threading.Semaphore(_sf_max_concurrent())


def _sf_call_with_limit_logging(
    fn: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """Run ``fn``; log Salesforce limit / timeout errors for monitoring."""
    try:
        return fn(*args, **kwargs)
    except SalesforceError as e:
        err_str = str(e)
        if "REQUEST_LIMIT_EXCEEDED" in err_str:
            log_error(f"SF REQUEST_LIMIT_EXCEEDED: {err_str[:120]}")
        elif "QUERY_TIMEOUT" in err_str:
            log_debug(f"SF QUERY_TIMEOUT: {err_str[:120]}")
        else:
            log_debug(f"SF error: {err_str[:120]}")
        raise
    except Exception as e:
        if "timeout" in str(e).lower():
            log_debug(f"SF timeout: {str(e)[:120]}")
        raise


def _sf_call_guarded(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Acquire global SF concurrency slot, then run with limit logging."""
    with _SF_SEMAPHORE:
        return _sf_call_with_limit_logging(fn, *args, **kwargs)


def get_sf_client():
    """Return authenticated Salesforce client (session token preferred, else username/password)."""
    global _sf_client
    if _sf_client is None:
        sf_token = os.getenv("SF_ACCESS_TOKEN") or os.getenv("SALESFORCE_ACCESS_TOKEN")
        sf_instance = os.getenv("SF_INSTANCE_URL") or os.getenv("SALESFORCE_INSTANCE_URL")

        if sf_token and sf_instance:
            _sf_client = Salesforce(
                instance_url=sf_instance.rstrip("/"),
                session_id=sf_token,
            )
            log_debug("✓ Connected to Salesforce org62 (session token)")
        else:
            sf_username = os.getenv("SF_USERNAME")
            sf_password = os.getenv("SF_PASSWORD")
            sf_security_token = os.getenv("SF_SECURITY_TOKEN", "")

            if not sf_username or not sf_password:
                raise ValueError(
                    "Salesforce credentials not found in environment.\n\n"
                    "Set one of:\n"
                    "  • SF_ACCESS_TOKEN + SF_INSTANCE_URL\n"
                    "  • SALESFORCE_ACCESS_TOKEN + SALESFORCE_INSTANCE_URL\n"
                    "  • SF_USERNAME + SF_PASSWORD (+ optional SF_SECURITY_TOKEN)\n\n"
                    f".env is loaded from: {_REPO_ROOT / '.env'}\n"
                    "Run commands from the project root or ensure those variables are exported."
                )

            domain = os.getenv("SF_DOMAIN", "login")
            _sf_client = Salesforce(
                username=sf_username,
                password=sf_password,
                security_token=sf_security_token,
                domain=domain,
            )
            log_debug("✓ Connected to Salesforce org62 (username/password)")

    return _sf_client


def _escape(s: str) -> str:
    """Escape single quotes for SOQL."""
    return str(s).replace("'", "\\'")


def _soql_line(soql: str) -> str:
    """Collapse whitespace/newlines — multi-line SOQL can break REST query URLs."""
    return " ".join(str(soql).split())


def clean_html(text: str) -> str:
    """Strip HTML tags and normalize whitespace for Latest Updates and similar fields."""
    if not text:
        return ""
    text = text.replace("<p>", "").replace("</p>", " ")
    text = text.replace("<br>", " ").replace("<br/>", " ")
    text = text.replace("<strong>", "**").replace("</strong>", "**")
    text = text.replace("<em>", "_").replace("</em>", "_")
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&#39;", "'").replace("&nbsp;", " ")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def resolve_account(account_name: str, cloud: str = "Commerce Cloud") -> dict:
    """
    Resolve account name to ID and details.
    Returns: {"id": "001xxx", "name": "Acme Corp", "country": "US"}
    """
    sf = get_sf_client()

    try:
        sosl = (
            f"FIND {{{_escape(account_name)}}} IN NAME FIELDS "
            f"RETURNING Account(Id, Name, BillingCountry)"
        )
        try:
            results = sf.search(sosl)
            if results and results.get("searchRecords"):
                acct = results["searchRecords"][0]
                if acct.get("Id"):
                    bc = acct.get("BillingCountry") or ""
                    return {
                        "id": acct["Id"],
                        "name": acct["Name"],
                        "country": bc,
                        "billing_country": bc,
                    }
        except Exception as e:
            log_debug(f"SOSL search skipped: {e}")

        # Try exact match first
        query = f"""
        SELECT Id, Name, BillingCountry
        FROM Account
        WHERE Name = '{_escape(account_name)}'
        LIMIT 1
        """
        result = sf.query(_soql_line(query))

        if not result.get("records"):
            # Try LIKE search
            query = f"""
            SELECT Id, Name, BillingCountry
            FROM Account
            WHERE Name LIKE '%{_escape(account_name)}%'
            LIMIT 5
            """
            result = sf.query(_soql_line(query))

        if not result.get("records"):
            return None

        # Get first match
        account = result["records"][0]

        bc = account.get("BillingCountry", "")
        return {
            "id": account["Id"],
            "name": account["Name"],
            "country": bc,
            "billing_country": bc,
        }

    except Exception as e:
        log_error(f"resolve_account error: {e}")
        return None


def resolve_account_enhanced(name: str, cloud: str = "Commerce Cloud") -> Optional[dict]:
    """
    Salesforce-first account resolution (SOSL/SOQL), then Snowflake fuzzy on renewal view
    (plus CSS fallback inside Snowflake client), then Salesforce Account row by Id.

    Returns same shape as resolve_account: id, name, country, billing_country.
    """
    result = resolve_account(name, cloud)
    if result:
        return result

    try:
        from domain.analytics.snowflake_client import resolve_account_from_snowflake

        snow = resolve_account_from_snowflake(name, cloud)
        account_id = (snow or {}).get("account_id")
        if not account_id:
            return None

        sf = get_sf_client()
        sf_query = (
            f"SELECT Id, Name, BillingCountry FROM Account "
            f"WHERE Id = '{_escape(account_id)}' LIMIT 1"
        )
        records = sf.query(sf_query).get("records", [])
        if records:
            bc = records[0].get("BillingCountry") or ""
            return {
                "id": records[0]["Id"],
                "name": records[0]["Name"],
                "country": bc,
                "billing_country": bc,
            }
    except Exception as e:
        log_debug(f"resolve_account_enhanced Snowflake fallback: {str(e)[:100]}")

    return None


def get_renewal_opportunities(account_id: str, cloud: str = "Commerce Cloud") -> list:
    """Renewal opps for account, filtered by cloud name in Opportunity Name."""
    sf = get_sf_client()
    aid = _escape(account_id)
    fields = (
        "Id, Name, StageName, Amount, CloseDate, "
        "Account.Id, Account.Name, Account.BillingCountry, "
        "ForecastCategoryName, Forecasted_Attrition__c, Swing__c, "
        "License_At_Risk_Reason__c, ACV_Reason_Detail__c, NextStep, "
        "Description, Specialist_Sales_Notes__c, "
        "Manager_Forecast_Judgement__c"
    )
    where = (
        f"AccountId = '{aid}' "
        f"AND Name LIKE '%{_escape(cloud)}%' "
        f"AND Name LIKE '%Renewal%' "
        f"AND IsClosed = false "
        f"AND CloseDate >= 2025-01-01"
    )
    query = (
        f"SELECT {fields} FROM Opportunity WHERE {where} "
        f"ORDER BY Forecasted_Attrition__c DESC LIMIT 10"
    )
    try:
        return sf.query(_soql_line(query)).get("records", [])
    except Exception as e:
        log_debug(f"Error fetching opportunities: {str(e)[:100]}")
        return []


def get_renewal_opportunities_any_cloud(account_id: str) -> list:
    """Open renewal opps without filtering Opportunity Name by cloud (enrichment fallback)."""
    sf = get_sf_client()
    aid = _escape(account_id)
    fields = (
        "Id, Name, StageName, Amount, CloseDate, "
        "Account.Id, Account.Name, Account.BillingCountry, "
        "ForecastCategoryName, Forecasted_Attrition__c, Swing__c, "
        "License_At_Risk_Reason__c, ACV_Reason_Detail__c, NextStep, "
        "Description, Specialist_Sales_Notes__c, "
        "Manager_Forecast_Judgement__c"
    )
    where = (
        f"AccountId = '{aid}' "
        f"AND Name LIKE '%Renewal%' "
        f"AND IsClosed = false "
        f"AND CloseDate >= 2025-01-01"
    )
    query = (
        f"SELECT {fields} FROM Opportunity WHERE {where} "
        f"ORDER BY Forecasted_Attrition__c DESC LIMIT 10"
    )
    try:
        return sf.query(_soql_line(query)).get("records", [])
    except Exception as e:
        log_debug(f"Error fetching opportunities (any cloud): {str(e)[:100]}")
        return []


def _fill_days_red_from_start_date(red: dict) -> None:
    """Set Days_Red__c from Red_Start_Date__c when the field is null."""
    if red.get("Days_Red__c") is not None:
        return
    rs = red.get("Red_Start_Date__c")
    if not rs:
        return
    try:
        if hasattr(rs, "date"):
            start = rs.date()
        elif isinstance(rs, str):
            start = date.fromisoformat(rs.split("T")[0])
        else:
            return
        red["Days_Red__c"] = (date.today() - start).days
    except (TypeError, ValueError):
        pass


def _finalize_red_account_record(red: dict) -> None:
    """Derive Days Red when missing; strip HTML from Latest Updates."""
    _fill_days_red_from_start_date(red)
    lu = red.get("Latest_Updates__c")
    if lu:
        red["Latest_Updates__c"] = clean_html(str(lu))
    try:
        red["days_red"] = int(red.get("Days_Red__c") or 0)
    except (TypeError, ValueError):
        red["days_red"] = 0


def get_red_account(account_id: str) -> Optional[dict]:
    """Active Red Account (Open / Precautionary); derive days red when missing."""
    sf = get_sf_client()
    aid = _escape(account_id)

    query = f"""
        SELECT
            Id, Name, Stage__c, ACV_at_Risk__c,
            Days_Red__c, Red_Trending__c, Red_Start_Date__c,
            Latest_Updates__c, Action_Plan__c,
            Issue_Product__c, Red_Account__c
        FROM Red_Account__c
        WHERE Red_Account__c = '{aid}'
        AND Stage__c IN ('Open', 'Precautionary')
        ORDER BY Red_Start_Date__c DESC NULLS LAST, CreatedDate DESC
        LIMIT 1
    """

    try:
        result = sf.query(_soql_line(query))
        if result.get("records"):
            red = result["records"][0]
            _finalize_red_account_record(red)
            return red

        query_historical = f"""
            SELECT
                Id, Name, Stage__c, Days_Red__c, Red_Start_Date__c,
                Latest_Updates__c, Action_Plan__c, Issue_Product__c, Red_Account__c
            FROM Red_Account__c
            WHERE Red_Account__c = '{aid}'
            ORDER BY CreatedDate DESC
            LIMIT 1
        """
        result_historical = sf.query(_soql_line(query_historical))
        if result_historical.get("records"):
            historical = result_historical["records"][0]
            _finalize_red_account_record(historical)
            historical["_historical"] = True
            return historical

        return None

    except Exception as e:
        log_debug(f"Error fetching red account: {str(e)[:100]}")
        return None


def get_account_team(account_id: str) -> dict:
    """AE (Account Owner), Renewal Manager, and CSM from Account + AccountTeamMember."""
    sf = get_sf_client()

    try:
        acc_query = f"""
        SELECT Id, Name, OwnerId, Owner.Name
        FROM Account
        WHERE Id = '{_escape(account_id)}'
        """
        result = sf.query(_soql_line(acc_query)).get("records", [])
        if not result:
            return {}

        rec = result[0]
        owner_name = rec.get("Owner", {}).get("Name", "") if rec.get("Owner") else ""

        team_query = f"""
        SELECT User.Name, TeamMemberRole
        FROM AccountTeamMember
        WHERE AccountId = '{_escape(account_id)}'
        AND (TeamMemberRole = 'Renewal Manager' OR TeamMemberRole = 'CSM')
        """
        team_members = sf.query(_soql_line(team_query)).get("records", [])
        renewal_mgr = ""
        csm = ""
        for member in team_members:
            role = member.get("TeamMemberRole", "")
            user_name = member.get("User", {}).get("Name", "") if member.get("User") else ""
            if role == "Renewal Manager":
                renewal_mgr = user_name
            elif role == "CSM":
                csm = user_name

        return {
            "ae": owner_name,
            "renewal_mgr": renewal_mgr,
            "csm": csm,
        }

    except Exception as e:
        log_debug(f"Error fetching account team: {str(e)[:100]}")
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
            result = self._sf.query(_soql_line(q))
            if not result.get("records"):
                q = f"""
                SELECT Id FROM Account
                WHERE Name LIKE '%{_escape(account_name)}%'
                LIMIT 5
                """
                result = self._sf.query(_soql_line(q))
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
            result = self._sf.query(_soql_line(q))
            if result.get("records"):
                return result["records"][0]["Id"]
            q2 = f"""
            SELECT Id FROM Opportunity
            WHERE Name LIKE '%{_escape(opp_name)}%' AND IsClosed = false
            ORDER BY CloseDate ASC
            LIMIT 1
            """
            result2 = self._sf.query(_soql_line(q2))
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
                   Website, OwnerId, Owner.Name
            FROM Account
            WHERE Id = '{_escape(account_id)}'
            LIMIT 1
            """
            result = self._sf.query(_soql_line(q))
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
            result = self._sf.query(_soql_line(q))
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
            result = self._sf.query(_soql_line(q))
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
            return get_red_account(account_id)
        except Exception as e:
            log_error(f"Org62Client.get_red_account_info error: {e}")
            return None

    def get_renewal_opportunity(
        self, account_id: str, _cloud: str = "Commerce Cloud"
    ) -> Optional[Dict[str, Any]]:
        """First renewal opportunity (same query logic as get_renewal_opportunities)."""
        try:
            opps = get_renewal_opportunities(account_id, _cloud)
            if not opps:
                opps = get_renewal_opportunities_any_cloud(account_id)
            return opps[0] if opps else None
        except Exception as e:
            log_error(f"Org62Client.get_renewal_opportunity error: {e}")
            return None
