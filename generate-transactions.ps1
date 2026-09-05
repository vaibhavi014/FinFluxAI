param(
  [ValidateSet("normal", "suspicious")]
  [string]$Mode = "normal"
)

$ErrorActionPreference = "Stop"
$Api = if ($env:FINFLUX_API) { $env:FINFLUX_API } else { "http://localhost:8080" }
$suffix = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

function Post-Txn($id, $customer, $amount, $city, $category, $status, $type) {
  $payload = @{
    transactionId = $id
    customerId = $customer
    accountId = "ACC-$($customer.Replace('CUST-',''))"
    amount = $amount
    currency = "INR"
    merchantId = "MERCHANT-55"
    merchantCategory = $category
    deviceId = "DEVICE-77"
    ipAddress = "192.168.1.10"
    location = $city
    timestamp = [DateTime]::UtcNow.ToString("o")
    type = $type
    status = $status
  } | ConvertTo-Json
  Invoke-RestMethod -Method Post -Uri "$Api/api/v1/transactions" -ContentType "application/json" -Body $payload | Out-Null
  Write-Host "Posted $id ($amount INR, $city, $category, $status)"
}

if ($Mode -eq "normal") {
  Post-Txn "TXN-N1-$suffix" "CUST-101" 450 "Mumbai" "GROCERY" "SUCCESS" "PAYMENT"
  Post-Txn "TXN-N2-$suffix" "CUST-102" 1200 "Bengaluru" "FOOD" "SUCCESS" "PAYMENT"
  Post-Txn "TXN-N3-$suffix" "CUST-103" 800 "Delhi" "TRAVEL" "SUCCESS" "PAYMENT"
} else {
  1..4 | ForEach-Object { Post-Txn "TXN-S0$_-$suffix" "CUST-101" 400 "Mumbai" "GROCERY" "SUCCESS" "PAYMENT" }
  Post-Txn "TXN-S1-$suffix" "CUST-101" 95000 "Mumbai" "ELECTRONICS" "SUCCESS" "PAYMENT"
  Post-Txn "TXN-S2-$suffix" "CUST-104" 1000 "Pune" "GROCERY" "SUCCESS" "PAYMENT"
  Post-Txn "TXN-S3-$suffix" "CUST-104" 1100 "Delhi" "GROCERY" "SUCCESS" "PAYMENT"
  Post-Txn "TXN-S4-$suffix" "CUST-105" 50 "Hyderabad" "GROCERY" "FAILED" "PAYMENT"
  Post-Txn "TXN-S5-$suffix" "CUST-105" 50 "Hyderabad" "GROCERY" "FAILED" "PAYMENT"
  Post-Txn "TXN-S6-$suffix" "CUST-105" 50 "Hyderabad" "CRYPTO" "FAILED" "PAYMENT"
  1..5 | ForEach-Object { Post-Txn "TXN-SV$_-$suffix" "CUST-102" 300 "Bengaluru" "FOOD" "SUCCESS" "PAYMENT" }
}

Write-Host "Open http://localhost:8080 and wait for processingStatus=SCORED."
