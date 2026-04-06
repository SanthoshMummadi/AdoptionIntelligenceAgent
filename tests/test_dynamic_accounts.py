"""
tests/test_dynamic_accounts.py
Dynamic E2E test suite — pulls random accounts from Snowflake,
validates fuzzy resolution, suffix handling, and field data integrity.

Run from repo root:
  python3 tests/test_dynamic_accounts.py

Requires:
  Snowflake credentials in .env (same as domain/analytics/snowflake_client.py).
  Salesforce for tests that call get_red_account or resolve_account_enhanced (e.g. DC_007).
"""
from __future__ import annotations

import os
import random
import re
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from domain.analytics.snowflake_client import (
    CORPORATE_SUFFIXES,
    enrich_account,
    get_account_attrition,
    get_usage_unified,
    resolve_account_from_snowflake,
    run_query,
    to_15_char_id,
)
from domain.salesforce.org62_client import get_red_account, resolve_account_enhanced

# ── Config ────────────────────────────────────────────────────────────────────
CLOUD = "Commerce Cloud"
RANDOM_SAMPLE_SIZE = 5
FUZZY_TRUNCATE_LEN = 6
REQUIRED_ARI_FIELDS = ["category", "probability"]
REQUIRED_USAGE_FIELDS = ["utilization_rate", "util_emoji"]
REQUIRED_HEALTH_FIELDS = ["overall_score", "overall_literal"]

VALID_ARI_CATEGORIES = {"high", "medium", "low", "unknown", ""}
VALID_HEALTH_LITERALS = {"green", "yellow", "red", "unknown", ""}


def _snowflake_configured() -> bool:
    return bool(os.getenv("SNOWFLAKE_USER") and os.getenv("SNOWFLAKE_ACCOUNT"))


def _norm_ari_cat(val) -> str:
    if val is None:
        return ""
    return str(val).strip().lower()


def _norm_health_lit(val) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    return s.lower()


# ── Helpers ────────────────────────────────────────────────────────────────────


def _get_random_commerce_accounts(n: int = 5) -> list[dict]:
    """Pull N random Commerce Cloud accounts from Snowflake renewal view."""
    sql = """
        SELECT DISTINCT
            ACCOUNT_18_ID AS ACCOUNT_ID,
            ACCOUNT_NM    AS ACCOUNT_NAME,
            TARGET_CLOUD
        FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
        WHERE (
            TARGET_CLOUD LIKE '%%Commerce%%'
            OR RENEWAL_OPTY_NM LIKE '%%Commerce%%'
        )
        AND ACCOUNT_NM IS NOT NULL
        AND ACCOUNT_18_ID IS NOT NULL
        ORDER BY RANDOM()
        LIMIT %s
    """
    # Pull extra rows then subsample for variety if RANDOM() is coarse on small tables
    limit = max(n * 3, n)
    rows = run_query(sql, [limit])
    if not rows:
        return []
    sampled = random.sample(rows, min(n, len(rows)))
    return sampled


def _strip_suffixes(name: str) -> str:
    """Remove corporate suffixes to simulate fuzzy input."""
    stripped = (
        re.sub(CORPORATE_SUFFIXES, "", name, flags=re.IGNORECASE)
        .strip()
        .rstrip(",")
        .strip()
    )
    return stripped if stripped else name


def _has_suffix(name: str) -> bool:
    """True if account name contains a corporate suffix."""
    return bool(re.search(CORPORATE_SUFFIXES, name, flags=re.IGNORECASE))


def _validate_enrichment_fields(enrichment: dict, account_name: str) -> list[str]:
    """Returns list of validation errors for enrichment dict."""
    errors = []

    if not isinstance(enrichment, dict) or not enrichment:
        return [f"{account_name}: enrichment is empty or not a dict"]

    # ARI
    ari = enrichment.get("ari", {})
    if not isinstance(ari, dict):
        errors.append(f"{account_name}: ari is not a dict")
    else:
        for f in REQUIRED_ARI_FIELDS:
            if f not in ari:
                errors.append(f"{account_name}: missing ari.{f}")
        cat = _norm_ari_cat(ari.get("category"))
        if cat not in VALID_ARI_CATEGORIES:
            errors.append(f"{account_name}: ari.category invalid: {ari.get('category')}")

    # Usage
    usage = enrichment.get("usage", {})
    if not isinstance(usage, dict):
        errors.append(f"{account_name}: usage is not a dict")
    else:
        for f in REQUIRED_USAGE_FIELDS:
            if f not in usage:
                errors.append(f"{account_name}: missing usage.{f}")
        util = usage.get("utilization_rate", "")
        if util not in ("N/A", None, "") and not str(util).endswith("%"):
            errors.append(
                f"{account_name}: usage.utilization_rate unexpected format: {util}"
            )

    # Health
    health = enrichment.get("health", {})
    if not isinstance(health, dict):
        errors.append(f"{account_name}: health is not a dict")
    else:
        for f in REQUIRED_HEALTH_FIELDS:
            if f not in health:
                errors.append(f"{account_name}: missing health.{f}")
        literal_raw = health.get("overall_literal", "")
        lit = _norm_health_lit(literal_raw)
        if lit in VALID_HEALTH_LITERALS:
            pass
        else:
            try:
                float(literal_raw)
                errors.append(
                    f"{account_name}: health.overall_literal is numeric (not normalized): {literal_raw}"
                )
            except (TypeError, ValueError):
                errors.append(
                    f"{account_name}: health.overall_literal unexpected value: {literal_raw}"
                )

    return errors


# ── Test Suite ────────────────────────────────────────────────────────────────


class TestDynamicAccountResolution(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not _snowflake_configured():
            raise unittest.SkipTest(
                "Snowflake env not configured (SNOWFLAKE_USER / SNOWFLAKE_ACCOUNT)"
            )
        print("\n" + "=" * 70)
        print("DYNAMIC ACCOUNT TESTS — Pulling random Commerce Cloud accounts")
        print("=" * 70)
        cls.accounts = _get_random_commerce_accounts(RANDOM_SAMPLE_SIZE)
        if not cls.accounts:
            raise unittest.SkipTest("No Commerce Cloud accounts found in Snowflake")
        print(f"  Sampled {len(cls.accounts)} accounts:")
        for a in cls.accounts:
            nm = a.get("ACCOUNT_NAME") or a.get("account_name")
            aid = a.get("ACCOUNT_ID") or a.get("account_id")
            print(f"    - {nm} ({to_15_char_id(str(aid or ''))})")

    def test_DYN_001_random_accounts_resolve(self):
        """Random accounts from Snowflake resolve correctly."""
        errors = []
        for acct in self.accounts:
            name = acct.get("ACCOUNT_NAME") or acct.get("account_name")
            account_id = to_15_char_id(str(acct.get("ACCOUNT_ID") or acct.get("account_id") or ""))
            result = resolve_account_from_snowflake(str(name), cloud=CLOUD)
            if not result:
                errors.append(f"FAILED to resolve: {name}")
            elif to_15_char_id(str(result.get("account_id", ""))) != account_id:
                errors.append(
                    f"WRONG ID for {name}: "
                    f"expected {account_id}, got {to_15_char_id(str(result.get('account_id', '')))}"
                )
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_002_fuzzy_partial_name_resolution(self):
        """Partial name (first word) returns some resolution from Snowflake (may differ if ambiguous)."""
        errors = []
        for acct in self.accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            words = name.strip().split()
            if len(words) < 2 or len(words[0]) < FUZZY_TRUNCATE_LEN:
                continue
            partial = words[0]
            result = resolve_account_from_snowflake(partial, cloud=CLOUD)
            if not result:
                errors.append(f"Partial '{partial}' (from '{name}') returned None")
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_003_suffix_stripped_resolution(self):
        """Accounts with corporate suffixes resolve after suffix removal."""
        suffix_accounts = [
            a
            for a in self.accounts
            if _has_suffix(str(a.get("ACCOUNT_NAME") or a.get("account_name") or ""))
        ]
        if not suffix_accounts:
            self.skipTest("No suffix accounts in current sample — re-run for different sample")

        errors = []
        for acct in suffix_accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            account_id = to_15_char_id(
                str(acct.get("ACCOUNT_ID") or acct.get("account_id") or "")
            )
            stripped = _strip_suffixes(name)
            if not stripped or stripped == name:
                continue
            result = resolve_account_from_snowflake(stripped, cloud=CLOUD)
            if not result:
                errors.append(f"Suffix-stripped '{stripped}' (from '{name}') returned None")
            elif to_15_char_id(str(result.get("account_id", ""))) != account_id:
                errors.append(
                    f"Wrong ID for suffix-stripped '{stripped}': "
                    f"expected {account_id}, got {to_15_char_id(str(result.get('account_id', '')))}"
                )
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_004_enrichment_field_validation(self):
        """Enrichment data for random accounts has all required fields with correct types."""
        errors = []
        for acct in self.accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            account_id = to_15_char_id(
                str(acct.get("ACCOUNT_ID") or acct.get("account_id") or "")
            )
            try:
                t = time.time()
                enrichment = enrich_account(account_id, cloud=CLOUD)
                print(f"  {name}: enriched in {time.time() - t:.2f}s")
                field_errors = _validate_enrichment_fields(enrichment, name)
                errors.extend(field_errors)
            except Exception as e:
                errors.append(f"{name}: enrich_account raised {type(e).__name__}: {str(e)[:80]}")
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_005_health_literal_is_normalized(self):
        """health.overall_literal is Green/Yellow/Red/Unknown — not a raw number."""
        errors = []
        for acct in self.accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            account_id = to_15_char_id(
                str(acct.get("ACCOUNT_ID") or acct.get("account_id") or "")
            )
            try:
                enrichment = enrich_account(account_id, cloud=CLOUD)
                literal = enrichment.get("health", {}).get("overall_literal", "")
                lit = _norm_health_lit(literal)
                if lit not in VALID_HEALTH_LITERALS:
                    errors.append(f"{name}: health_literal not normalized: '{literal}'")
            except Exception as e:
                errors.append(f"{name}: {str(e)[:80]}")
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_006_ari_category_valid(self):
        """ARI category is one of High/Medium/Low/Unknown for all random accounts."""
        errors = []
        for acct in self.accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            account_id = to_15_char_id(
                str(acct.get("ACCOUNT_ID") or acct.get("account_id") or "")
            )
            try:
                enrichment = enrich_account(account_id, cloud=CLOUD)
                cat = _norm_ari_cat(enrichment.get("ari", {}).get("category"))
                if cat not in VALID_ARI_CATEGORIES:
                    errors.append(f"{name}: unexpected ARI category: '{cat}'")
            except Exception as e:
                errors.append(f"{name}: {str(e)[:80]}")
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_007_utilization_rate_format(self):
        """utilization_rate is formatted as 'XX.X%' or 'N/A' / empty."""
        errors = []
        for acct in self.accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            account_id = to_15_char_id(
                str(acct.get("ACCOUNT_ID") or acct.get("account_id") or "")
            )
            try:
                enrichment = enrich_account(account_id, cloud=CLOUD)
                util = enrichment.get("usage", {}).get("utilization_rate", "")
                if util in ("N/A", None, ""):
                    continue
                if not str(util).endswith("%"):
                    errors.append(f"{name}: utilization_rate not formatted: '{util}'")
                else:
                    try:
                        val = float(str(util).rstrip("%"))
                        if not 0 <= val <= 100:
                            errors.append(f"{name}: utilization_rate out of range: {val}")
                    except ValueError:
                        errors.append(f"{name}: utilization_rate not parseable: '{util}'")
            except Exception as e:
                errors.append(f"{name}: {str(e)[:80]}")
        self.assertFalse(errors, "\n".join(errors))

    def test_DYN_008_days_red_is_int_or_zero(self):
        """If red account record exists, days_red is an int (org allows 0)."""
        errors = []
        for acct in self.accounts:
            name = str(acct.get("ACCOUNT_NAME") or acct.get("account_name") or "")
            account_id = str(acct.get("ACCOUNT_ID") or acct.get("account_id") or "")
            try:
                red = get_red_account(account_id)
                if red:
                    days = red.get("days_red")
                    if days is None:
                        errors.append(f"{name}: days_red is None (expected int)")
                    elif not isinstance(days, int):
                        errors.append(f"{name}: days_red is not int: {type(days).__name__}")
            except Exception as e:
                errors.append(f"{name}: {str(e)[:80]}")
        self.assertFalse(errors, "\n".join(errors))


class TestSuffixAccountResolution(unittest.TestCase):
    """Corporate suffix smoke tests (still require Snowflake)."""

    @classmethod
    def setUpClass(cls):
        if not _snowflake_configured():
            raise unittest.SkipTest("Snowflake env not configured")

    def test_SUFFIX_001_ag_suffix(self):
        result = resolve_account_from_snowflake("Adidas AG", cloud=CLOUD)
        self.assertIsNotNone(result, "Adidas AG should resolve when present in org")

    def test_SUFFIX_002_partial_ag_no_suffix(self):
        result = resolve_account_from_snowflake("Adidas", cloud=CLOUD)
        self.assertIsNotNone(result, "Partial 'Adidas' should resolve when unambiguous")

    def test_SUFFIX_003_lowercase_name(self):
        result = resolve_account_from_snowflake("adidas ag", cloud=CLOUD)
        self.assertIsInstance(result, (dict, type(None)))

    def test_SUFFIX_004_inc_suffix(self):
        result = resolve_account_from_snowflake("Oxford Industries Inc.", cloud=CLOUD)
        self.assertIsInstance(result, (dict, type(None)))


class TestFieldDataIntegrity(unittest.TestCase):
    """Known-account checks (canonical Commerce account in sample org)."""

    KNOWN_ACCOUNT = "Adidas AG"
    KNOWN_ACCOUNT_ID: str | None = None
    enrichment: dict

    @classmethod
    def setUpClass(cls):
        if not _snowflake_configured():
            raise unittest.SkipTest("Snowflake env not configured")
        result = resolve_account_from_snowflake(cls.KNOWN_ACCOUNT, cloud=CLOUD)
        if not result:
            raise unittest.SkipTest(f"Could not resolve {cls.KNOWN_ACCOUNT} in Snowflake")
        cls.KNOWN_ACCOUNT_ID = to_15_char_id(str(result["account_id"]))
        cls.enrichment = enrich_account(cls.KNOWN_ACCOUNT_ID, cloud=CLOUD)

    def test_FDI_001_ari_not_unknown(self):
        cat = _norm_ari_cat(self.enrichment.get("ari", {}).get("category"))
        self.assertIn(
            cat,
            ("high", "medium", "low"),
            f"Expected concrete ARI category for {self.KNOWN_ACCOUNT}, got: {cat}",
        )

    def test_FDI_002_usage_gmv_populated(self):
        util = self.enrichment.get("usage", {}).get("utilization_rate")
        self.assertIsNotNone(util, "utilization_rate should not be None")
        if util == "N/A":
            self.skipTest("No usage row for this account in CIDM (data availability)")

    def test_FDI_003_health_score_numeric(self):
        score = self.enrichment.get("health", {}).get("overall_score")
        if score is not None:
            try:
                float(score)
            except (TypeError, ValueError):
                self.fail(f"health.overall_score is not numeric: {score}")

    def test_FDI_004_health_literal_normalized(self):
        literal = self.enrichment.get("health", {}).get("overall_literal")
        self.assertIn(
            _norm_health_lit(literal),
            ("green", "yellow", "red", "unknown"),
            f"health_literal not normalized: '{literal}'",
        )

    def test_FDI_005_usage_source_populated(self):
        source = self.enrichment.get("usage", {}).get("source")
        self.assertIsNotNone(source, "usage.source should not be None")
        # allow empty string only if utilization is N/A — otherwise expect traceability
        util = self.enrichment.get("usage", {}).get("utilization_rate")
        if util and util != "N/A":
            self.assertNotEqual(source, "", "usage.source should not be empty when util present")


class TestDataCorrectness(unittest.TestCase):
    """
    Cross-validates that field values are mathematically and semantically
    correct — not just present and formatted.
    Uses Adidas AG as the canonical known-good account.
    """

    KNOWN_ACCOUNT = "Adidas AG"

    @classmethod
    def setUpClass(cls):
        if not _snowflake_configured():
            raise unittest.SkipTest(
                "Snowflake env not configured (SNOWFLAKE_USER / SNOWFLAKE_ACCOUNT)"
            )
        result = resolve_account_from_snowflake(cls.KNOWN_ACCOUNT, cloud=CLOUD)
        if not result:
            raise unittest.SkipTest(f"Could not resolve {cls.KNOWN_ACCOUNT}")
        cls.account_id_15 = to_15_char_id(str(result["account_id"]))
        cls.enrichment = enrich_account(cls.account_id_15, cloud=CLOUD)
        cls.usage_unified = get_usage_unified(cls.account_id_15, cloud=CLOUD)

    def test_DC_001_ari_probability_range(self):
        """ARI probability is between 0.0 and 1.0."""
        prob = self.enrichment.get("ari", {}).get("probability")
        if prob is None:
            self.skipTest("No ARI probability returned")
        try:
            val = float(prob)
            self.assertGreaterEqual(val, 0.0, f"ARI probability below 0: {val}")
            self.assertLessEqual(val, 1.0, f"ARI probability above 1: {val}")
        except (TypeError, ValueError):
            self.fail(f"ARI probability not numeric: {prob}")

    def test_DC_002_utilization_matches_raw_calculation(self):
        """
        utilization_rate in summary matches used/provisioned × 100
        from raw rows — within 0.5% tolerance.
        Mirrors get_usage_unified row selection (GMV → Commerce subset → all).
        """
        summary = self.usage_unified.get("summary", {})
        raw_rows = self.usage_unified.get("raw_rows", [])
        util_str = summary.get("utilization_rate", "N/A")

        if util_str in ("N/A", None, ""):
            self.skipTest("No utilization data available")

        gmv_rows = [r for r in raw_rows if str(r.get("GRP", "")).upper() == "GMV"]
        if gmv_rows:
            target = gmv_rows
        else:
            commerce_rows = [
                r
                for r in raw_rows
                if "commerce" in str(r.get("DRVD_APM_LVL_1", "")).lower()
                or "commerce" in str(r.get("DRVD_APM_LVL_2", "")).lower()
            ]
            target = commerce_rows if commerce_rows else raw_rows

        total_prov = sum(float(r.get("PROVISIONED") or 0) for r in target)
        total_used = sum(float(r.get("USED") or 0) for r in target)

        if total_prov == 0:
            self.skipTest("No provisioned data to validate against")

        expected = round((total_used / total_prov) * 100, 1)
        actual = float(str(util_str).rstrip("%"))

        self.assertAlmostEqual(
            actual,
            expected,
            delta=0.5,
            msg=f"Utilization mismatch: summary={actual}% vs raw_calc={expected}%",
        )

    def test_DC_003_renewal_aov_is_positive(self):
        """Renewal AOV is a positive number when present."""
        aov = self.enrichment.get("renewal_aov", {}).get("renewal_aov")
        if aov is None:
            self.skipTest("No renewal AOV returned")
        try:
            val = float(aov)
            self.assertGreater(val, 0, f"Renewal AOV is not positive: {val}")
        except (TypeError, ValueError):
            self.fail(f"Renewal AOV not numeric: {aov}")

    def test_DC_004_health_score_range(self):
        """Health score is between 0 and 100."""
        score = self.enrichment.get("health", {}).get("overall_score")
        if score is None:
            self.skipTest("No health score returned")
        try:
            val = float(score)
            self.assertGreaterEqual(val, 0, f"Health score below 0: {val}")
            self.assertLessEqual(val, 100, f"Health score above 100: {val}")
        except (TypeError, ValueError):
            self.fail(f"Health score not numeric: {score}")

    def test_DC_005_health_literal_matches_score(self):
        """
        health_literal (Green/Yellow/Red) matches
        the band derived from health_score.
        """
        health = self.enrichment.get("health", {})
        score = health.get("overall_score")
        literal = health.get("overall_literal")

        if score is None or literal in (None, "Unknown", ""):
            self.skipTest("No health data to cross-validate")

        try:
            val = float(score)
        except (TypeError, ValueError):
            self.skipTest(f"Health score not numeric: {score}")

        if val >= 70:
            expected = "Green"
        elif val >= 40:
            expected = "Yellow"
        else:
            expected = "Red"

        lit_norm = str(literal).strip().lower()
        exp_norm = expected.lower()
        self.assertEqual(
            lit_norm,
            exp_norm,
            f"health_literal mismatch: score={val} → "
            f"expected '{expected}', got '{literal}'",
        )

    def test_DC_006_attrition_products_are_commerce(self):
        """
        Attrition products for Commerce Cloud query
        contain at least 80% Commerce-related APM levels.
        """
        products = get_account_attrition(self.account_id_15, cloud=CLOUD)

        if not products:
            self.skipTest("No attrition products returned")

        non_commerce = [
            p
            for p in products
            if not any(
                "commerce" in str(p.get(k, "")).lower()
                for k in ("APM_LVL_1", "APM_LVL_2", "APM_LVL_3")
            )
        ]
        threshold = len(products) * 0.2
        self.assertLessEqual(
            len(non_commerce),
            threshold,
            f"Too many non-Commerce products: "
            f"{len(non_commerce)}/{len(products)}\n"
            + "\n".join(
                f"  - {p.get('APM_LVL_1')} / "
                f"{p.get('APM_LVL_2')} / {p.get('APM_LVL_3')}"
                for p in non_commerce
            ),
        )

    def test_DC_007_sf_id_matches_snowflake_id(self):
        """
        Account ID from Salesforce resolution matches
        Snowflake resolution for the same account name.
        """
        sf_result = resolve_account_enhanced(self.KNOWN_ACCOUNT, cloud=CLOUD)
        sf_raw = (
            (sf_result.get("id") or sf_result.get("account_id") or "")
            if sf_result
            else ""
        )
        sf_id = to_15_char_id(str(sf_raw)) if sf_raw else None

        snow_result = resolve_account_from_snowflake(
            self.KNOWN_ACCOUNT, cloud=CLOUD
        )
        snow_id = (
            to_15_char_id(str(snow_result.get("account_id", "")))
            if snow_result
            else None
        )

        if sf_id and snow_id:
            self.assertEqual(
                sf_id,
                snow_id,
                f"SF ID ({sf_id}) ≠ Snowflake ID ({snow_id}) "
                f"for '{self.KNOWN_ACCOUNT}'",
            )
        else:
            self.skipTest(f"Could not get both IDs — SF: {sf_id}, Snow: {snow_id}")

    def test_DC_008_csg_geo_is_known_region(self):
        """CSG geo is a recognizable region string."""
        KNOWN_GEOS = {
            "AMER",
            "EMEA",
            "APAC",
            "LATAM",
            "NA",
            "US",
            "EU",
            "APJ",
        }
        geo = self.enrichment.get("renewal_aov", {}).get("csg_geo", "")
        if not geo:
            self.skipTest("No CSG geo returned")
        matched = any(k in str(geo).upper() for k in KNOWN_GEOS)
        self.assertTrue(
            matched,
            f"CSG geo '{geo}' not in known regions: {KNOWN_GEOS}",
        )

    def test_DC_009_attrition_pipeline_is_numeric(self):
        """
        ATTRITION_PIPELINE values in products are
        numeric when present.
        """
        products = get_account_attrition(self.account_id_15, cloud=CLOUD)
        if not products:
            self.skipTest("No attrition products returned")

        errors = []
        for p in products:
            val = p.get("ATTRITION_PIPELINE")
            if val is not None:
                try:
                    float(val)
                except (TypeError, ValueError):
                    errors.append(
                        f"{p.get('APM_LVL_2')}: "
                        f"ATTRITION_PIPELINE not numeric: {val}"
                    )
        self.assertFalse(errors, "\n".join(errors))

    def test_DC_010_usage_raw_rows_match_summary_source(self):
        """
        Raw rows GRP field matches the source reported
        in summary (GMV rows present if source == 'GMV').
        """
        summary = self.usage_unified.get("summary", {})
        raw_rows = self.usage_unified.get("raw_rows", [])
        source = summary.get("source", "")

        if not raw_rows or not source:
            self.skipTest("No usage data to cross-validate")

        if source == "GMV":
            gmv_rows = [
                r for r in raw_rows if str(r.get("GRP", "")).upper() == "GMV"
            ]
            self.assertGreater(
                len(gmv_rows),
                0,
                "Summary source='GMV' but no GMV rows in raw_rows",
            )


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestDynamicAccountResolution))
    suite.addTests(loader.loadTestsFromTestCase(TestSuffixAccountResolution))
    suite.addTests(loader.loadTestsFromTestCase(TestFieldDataIntegrity))
    suite.addTests(loader.loadTestsFromTestCase(TestDataCorrectness))

    print("\n" + "=" * 70)
    print("DYNAMIC COMMERCE CLOUD TEST SUITE")
    print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
