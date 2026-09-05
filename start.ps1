$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  Write-Host "Created .env from .env.example"
}

docker compose up -d --build

function Wait-Http($url, $tries) {
  for ($i = 0; $i -lt $tries; $i++) {
    try {
      Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3 | Out-Null
      return
    } catch {
      Start-Sleep -Seconds 2
    }
  }
  throw "Timed out waiting for $url"
}

Write-Host "Waiting for Kafka Connect and the app..."
Wait-Http "http://localhost:8083/connectors" 60
Wait-Http "http://localhost:8080/api/v1/health" 60

$password = "finflux"
if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    if ($_ -match "^\s*DATABASE_PASSWORD=(.*)$") { $password = $Matches[1].Trim() }
  }
}

$body = @{
  "connector.class" = "io.debezium.connector.postgresql.PostgresConnector"
  "database.hostname" = "postgres"
  "database.port" = "5432"
  "database.user" = "finflux"
  "database.password" = $password
  "database.dbname" = "finflux"
  "topic.prefix" = "finflux"
  "table.include.list" = "public.transactions"
  "plugin.name" = "pgoutput"
  "slot.name" = "finflux_debezium"
  "publication.name" = "finflux_publication"
  "publication.autocreate.mode" = "filtered"
  "tombstones.on.delete" = "false"
  "decimal.handling.mode" = "string"
  "time.precision.mode" = "adaptive"
  "snapshot.mode" = "initial"
  "slot.drop.on.stop" = "false"
  "key.converter" = "org.apache.kafka.connect.json.JsonConverter"
  "value.converter" = "org.apache.kafka.connect.json.JsonConverter"
  "key.converter.schemas.enable" = "false"
  "value.converter.schemas.enable" = "false"
  "message.key.columns" = "public.transactions:transaction_id"
} | ConvertTo-Json -Compress

Invoke-RestMethod -Method Put -Uri "http://localhost:8083/connectors/finflux-transactions-connector/config" `
  -ContentType "application/json" -Body $body | Out-Null

Write-Host "FinFlux is up. Dashboard: http://localhost:8080"
Write-Host "Next: .\scripts\generate-transactions.ps1 suspicious"
