"""Snowflake adapter for analytics data access."""
import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from domain.analytics.snowflake_client import SnowflakeClient

# Load environment variables
load_dotenv()


class SnowflakeAdapter:
    """Adapter for Snowflake data operations."""

    def __init__(self):
        """Initialize Snowflake adapter with credentials from environment."""
        self.client = SnowflakeClient(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        )

    def get_account_usage(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Fetch usage metrics for an account."""
        return self.client.get_account_usage(account_id)

    def get_ari_score(self, account_id: str) -> Optional[float]:
        """Get ARI (At-Risk Index) score for account."""
        return self.client.get_ari_score(account_id)

    def get_attrition_signals(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get attrition signals for account."""
        return self.client.get_attrition_signals(account_id)

    def close(self):
        """Close Snowflake connection."""
        if self.client:
            self.client.close()
