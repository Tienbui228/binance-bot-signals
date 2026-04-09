#!/bin/bash
# Run this from your project root (same folder as oi_scanner.py)
# Usage: bash r2_lite/setup.sh

set -e

echo "Creating r2_lite folder structure..."

mkdir -p r2_lite/research/signature_measurement
mkdir -p r2_lite/scripts
mkdir -p r2_lite/data/research_cache/binance_proxy
mkdir -p r2_lite/data/research_outputs

# Create __init__.py files
echo "# research package" > r2_lite/research/__init__.py
echo "# research.signature_measurement" > r2_lite/research/signature_measurement/__init__.py

echo ""
echo "Done. Now copy the remaining files:"
echo ""
echo "  r2_lite/research_config.yaml"
echo "  r2_lite/scripts/run_signature_measurement.py"
echo "  r2_lite/research/signature_measurement/contracts.py"
echo "  r2_lite/research/signature_measurement/io.py"
echo "  r2_lite/research/signature_measurement/proxy_features.py"
echo "  r2_lite/research/signature_measurement/classifier.py"
echo "  r2_lite/research/signature_measurement/rule_engine.py"
echo "  r2_lite/research/signature_measurement/event_builder.py"
echo "  r2_lite/research/signature_measurement/outcome_engine.py"
echo "  r2_lite/research/signature_measurement/report_builder.py"
echo ""
echo "Then run:"
echo "  python3 r2_lite/scripts/run_signature_measurement.py"
