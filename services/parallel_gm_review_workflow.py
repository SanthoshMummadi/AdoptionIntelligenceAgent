"""Parallel GM Review Workflow Service."""
import asyncio
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from adapters.canvas_adapter import CanvasAdapter
from adapters.salesforce_adapter import SalesforceAdapter
from adapters.snowflake_adapter import SnowflakeAdapter
from domain.intelligence.risk_engine import RiskEngine

_OPP_ID_RE = re.compile(r"^006[A-Za-z0-9]{12,15}$")


class ParallelGMReviewWorkflow:
    """Orchestrates parallel GM review generation."""

    def __init__(
        self,
        salesforce_adapter: SalesforceAdapter,
        snowflake_adapter: SnowflakeAdapter,
        canvas_adapter: CanvasAdapter,
        risk_engine: RiskEngine,
        max_concurrent: int = 5,
    ):
        """Initialize workflow with adapters."""
        self.sf = salesforce_adapter
        self.snow = snowflake_adapter
        self.canvas = canvas_adapter
        self.risk_engine = risk_engine
        self.max_concurrent = max_concurrent

    def resolve_inputs(self, inputs: List[str]) -> List[str]:
        """
        Resolve account names or opportunity IDs to account IDs.

        Args:
            inputs: List of account names or opportunity IDs (006…)

        Returns:
            List of resolved account IDs (deduplicated, order preserved)
        """
        account_ids: List[str] = []
        for input_val in inputs:
            raw = (input_val or "").strip()
            if not raw:
                continue

            account_id = None
            if _OPP_ID_RE.match(raw):
                opp_details = self.sf.get_opportunity_details(raw)
                if opp_details:
                    account_id = opp_details.get("AccountId")
            else:
                account_id = self.sf.resolve_account_id(raw)
                if not account_id:
                    opp_id = self.sf.resolve_opportunity_id(raw)
                    if opp_id:
                        od = self.sf.get_opportunity_details(opp_id)
                        if od:
                            account_id = od.get("AccountId")

            if account_id:
                account_ids.append(account_id)

        seen = set()
        unique: List[str] = []
        for aid in account_ids:
            if aid not in seen:
                seen.add(aid)
                unique.append(aid)
        return unique

    def fetch_account_data(self, account_id: str) -> Dict[str, Any]:
        """
        Fetch all data for a single account (Salesforce + Snowflake).

        Args:
            account_id: Salesforce account ID

        Returns:
            Merged account data dictionary
        """
        sf_data = {
            "account": self.sf.get_account_details(account_id),
            "team": self.sf.get_account_team(account_id),
            "red_account": self.sf.get_red_account_info(account_id),
        }

        snow_data = {
            "usage": self.snow.get_account_usage(account_id),
            "ari_score": self.snow.get_ari_score(account_id),
            "attrition": self.snow.get_attrition_signals(account_id),
        }

        return {
            "account_id": account_id,
            "salesforce": sf_data,
            "analytics": snow_data,
        }

    def generate_review(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate GM review for account.

        Args:
            account_data: Merged account data

        Returns:
            Review data with risk analysis and adoption POV
        """
        risk_analysis = self.risk_engine.analyze_risk(account_data)
        adoption_pov = self.risk_engine.generate_adoption_pov(account_data)

        canvas_content = self.canvas.build_gm_review({
            **account_data,
            "risk_analysis": risk_analysis,
            "adoption_pov": adoption_pov,
        })

        acc = account_data.get("salesforce", {}).get("account") or {}
        account_name = acc.get("Name") if acc else str(account_data.get("account_id", "Unknown"))

        return {
            "account_id": account_data["account_id"],
            "account_name": account_name,
            "risk_analysis": risk_analysis,
            "adoption_pov": adoption_pov,
            "canvas_content": canvas_content,
        }

    async def process_accounts_parallel(self, account_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Process multiple accounts in parallel.

        Args:
            account_ids: List of account IDs to process

        Returns:
            List of review results
        """
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            fetch_tasks = [
                loop.run_in_executor(executor, self.fetch_account_data, account_id)
                for account_id in account_ids
            ]
            account_data_list = await asyncio.gather(*fetch_tasks)

            review_tasks = [
                loop.run_in_executor(executor, self.generate_review, account_data)
                for account_data in account_data_list
            ]
            reviews = await asyncio.gather(*review_tasks)

        return reviews

    def run(self, inputs: List[str]) -> List[Dict[str, Any]]:
        """
        Main entry point - run the full workflow.

        Args:
            inputs: List of account names or opportunity IDs

        Returns:
            List of GM review results
        """
        account_ids = self.resolve_inputs(inputs)

        if not account_ids:
            return []

        return asyncio.run(self.process_accounts_parallel(account_ids))
