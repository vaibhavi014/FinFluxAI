#!/usr/bin/env bash
set -euo pipefail
echo "Customers are created by Flyway migration V2. Inserting a few extra source rows..."
ROOT="$(cd "$(dirname "$0")" && pwd)"
"$ROOT/generate-transactions.sh" normal
