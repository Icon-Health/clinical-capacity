#!/bin/bash

set -euo pipefail

echo "Starting clinical-capacity SQL execution from assets.yml..."

# Path to assets.yml from current dir
ASSETS_FILE="assets.yml"

# Read run_order from assets.yml
RUN_ORDER=($(awk '/run_order:/ {flag=1; next} /^[^ ]/ {flag=0} flag {print $2}' "$ASSETS_FILE"))

# Loop through each item and run the corresponding SQL file
for name in "${RUN_ORDER[@]}"; do
  # Find the SQL file in subfolders
  SQL_FILE=$(find . -type f -name "$name.sql" | head -n 1)

  if [[ -z "$SQL_FILE" ]]; then
    echo "❌ SQL file not found for: $name"
    exit 1
  fi

  echo "▶️  Running: $SQL_FILE"
  bq query --use_legacy_sql=false < "$SQL_FILE"
done

echo "✅ All scripts executed successfully."
