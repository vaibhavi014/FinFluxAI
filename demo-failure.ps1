$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Api = "http://localhost:8080"

Write-Host "=== Generate normal traffic ==="
& "$PSScriptRoot\generate-transactions.ps1" -Mode normal
Start-Sleep 3
try { Invoke-RestMethod "$Api/api/v1/health" | ConvertTo-Json } catch { Write-Host $_ }

Write-Host "=== Stop PostgreSQL ==="
docker compose stop postgres
Start-Sleep 2
try { Invoke-WebRequest "$Api/actuator/health" -UseBasicParsing | Select-Object -ExpandProperty Content } catch { Write-Host "Health check failed as expected: $($_.Exception.Message)" }

Write-Host "=== Restore PostgreSQL ==="
docker compose start postgres
Start-Sleep 8
& "$PSScriptRoot\generate-transactions.ps1" -Mode normal

Write-Host "=== Malformed Kafka message -> DLQ ==="
"this is not valid json" | docker compose exec -T kafka /kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic finflux.public.transactions
Start-Sleep 5
docker compose exec -T kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transactions.dlq --from-beginning --timeout-ms 8000
try { Invoke-RestMethod "$Api/api/v1/errors" | ConvertTo-Json -Depth 5 } catch { Write-Host $_ }
Write-Host "Done."
