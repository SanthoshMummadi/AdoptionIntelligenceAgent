"""Salesforce adapter for CRM data access."""
from typing import Any, Dict, List, Optional

from domain.salesforce.org62_client import Org62Client


class SalesforceAdapter:
    """Adapter for Salesforce operations."""

    def __init__(self, access_token: str, instance_url: str):
        """Initialize Salesforce adapter."""
        self.client = Org62Client(access_token, instance_url)

    def resolve_account_id(self, account_name: str) -> Optional[str]:
        """Resolve account name to account ID."""
        return self.client.resolve_account_id(account_name)

    def resolve_opportunity_id(self, opp_name: str) -> Optional[str]:
        """Resolve opportunity name to opportunity ID."""
        return self.client.resolve_opportunity_id(opp_name)

    def get_account_details(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get account details."""
        return self.client.get_account_details(account_id)

    def get_opportunity_details(self, opp_id: str) -> Optional[Dict[str, Any]]:
        """Get opportunity details."""
        return self.client.get_opportunity_details(opp_id)

    def get_account_team(self, account_id: str) -> List[Dict[str, Any]]:
        """Get account team members."""
        return self.client.get_account_team(account_id)

    def get_red_account_info(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get red account information if exists."""
        return self.client.get_red_account_info(account_id)

    def get_renewal_opportunity(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get renewal opportunity for account."""
        return self.client.get_renewal_opportunity(account_id)
