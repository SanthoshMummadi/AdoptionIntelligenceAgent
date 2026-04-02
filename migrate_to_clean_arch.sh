#!/bin/bash
echo "=== MIGRATION TO CLEAN ARCHITECTURE ==="
echo ""

echo "Step 1: Check what imports adapters..."
echo "--- salesforce_adapter imports ---"
grep -r "salesforce_adapter\|SalesforceAdapter" . --include="*.py" | grep -v venv | grep -v __pycache__

echo ""
echo "--- snowflake_adapter imports ---"
grep -r "snowflake_adapter\|SnowflakeAdapter" . --include="*.py" | grep -v venv | grep -v __pycache__

echo ""
echo "--- canvas_adapter imports ---"
grep -r "canvas_adapter\|CanvasAdapter" . --include="*.py" | grep -v venv | grep -v __pycache__

echo ""
echo "Step 2: Check services..."
grep -r "parallel_gm_review\|ParallelGMReviewWorkflow" . --include="*.py" | grep -v venv | grep -v __pycache__

echo ""
echo "Step 3: Current file sizes..."
wc -l slack_app.py adapters/*.py services/*.py 2>/dev/null
