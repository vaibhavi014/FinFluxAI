const errorEl = document.getElementById("error");

async function load() {
  errorEl.hidden = true;
  try {
    const [metrics, txns, alerts] = await Promise.all([
      fetch("/api/v1/metrics/summary").then(ok),
      fetch("/api/v1/transactions").then(ok),
      fetch("/api/v1/alerts").then(ok)
    ]);
    document.getElementById("total").textContent = metrics.transactions.total;
    document.getElementById("low").textContent = metrics.transactions.low;
    document.getElementById("medium").textContent = metrics.transactions.medium;
    document.getElementById("high").textContent = metrics.transactions.high;
    document.getElementById("alerts-count").textContent = metrics.alerts;
    document.getElementById("dlq").textContent = metrics.dlqRows;

    const body = document.getElementById("txn-body");
    body.innerHTML = txns.map((row) => `
      <tr>
        <td>${esc(row.transactionId)}</td>
        <td>${esc(row.customerId)}</td>
        <td>${esc(row.amount)} ${esc(row.currency)}</td>
        <td><span class="pill ${esc(row.processingStatus)}">${esc(row.processingStatus)}</span></td>
        <td>${row.riskLevel ? `<span class="pill ${esc(row.riskLevel)}">${esc(row.riskLevel)} ${row.riskScore}</span>` : "—"}</td>
        <td>${esc(row.explanation || "")}</td>
      </tr>
    `).join("") || `<tr><td colspan="6">No transactions yet. Run the generator script.</td></tr>`;

    const alertsBox = document.getElementById("alerts");
    alertsBox.innerHTML = alerts.map((alert) => `
      <div class="alert">
        <strong>${esc(alert.transactionId)}</strong>
        <span class="pill ${esc(alert.riskLevel)}">${esc(alert.riskLevel)} ${alert.riskScore}</span>
        <p>${esc(alert.explanation)}</p>
      </div>
    `).join("") || `<div class="alert"><p>No alerts. Generate a suspicious batch to see them.</p></div>`;
  } catch (err) {
    errorEl.hidden = false;
    errorEl.textContent = "Could not load API data. Is the app running? " + err.message;
  }
}

async function ok(response) {
  if (!response.ok) {
    throw new Error("HTTP " + response.status);
  }
  return response.json();
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

document.getElementById("refresh").addEventListener("click", load);
load();
setInterval(load, 4000);
