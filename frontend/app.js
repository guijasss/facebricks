const state = {
  windowDays: 30,
};

const statusMessage = document.getElementById("status-message");
const windowDaysInput = document.getElementById("window-days");
const syncButton = document.getElementById("sync-button");

windowDaysInput.addEventListener("change", async (event) => {
  state.windowDays = Number(event.target.value);
  await loadDashboard();
});

syncButton.addEventListener("click", async () => {
  syncButton.disabled = true;
  setStatus("Syncing Databricks metadata into the backend…");
  try {
    const response = await fetch("/api/finops/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ runs_limit: 250 }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Sync failed.");
    }
    setStatus(`Sync complete. Saved ${payload.saved_jobs} jobs, ${payload.saved_runs} runs, ${payload.saved_clusters} clusters.`);
    await loadDashboard();
  } catch (error) {
    setStatus(error.message);
  } finally {
    syncButton.disabled = false;
  }
});

async function loadDashboard() {
  setStatus("Loading dashboard data…");
  try {
    const response = await fetch(`/api/finops/dashboard?window_days=${state.windowDays}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Could not load dashboard.");
    }
    renderSummary(payload.summary);
    renderCostChart(payload.cost_over_time, payload.summary.currency);
    renderCoverage(payload.coverage);
    renderTopJobs(payload.top_jobs, payload.summary.currency);
    renderInsights(payload.insights, payload.summary.currency);
    renderRecentRuns(payload.recent_runs, payload.summary.currency);
    setStatus(payload.summary.last_sync_at
      ? `Last sync: ${formatTimestamp(payload.summary.last_sync_at)}`
      : "No sync has been run yet. Configure the backend env vars and trigger the first sync.");
  } catch (error) {
    renderEmptyState();
    setStatus(error.message);
  }
}

function renderSummary(summary) {
  const items = [
    { label: "Total cost", value: formatCurrency(summary.total_cost, summary.currency) },
    { label: "Costed runs", value: String(summary.run_count) },
    { label: "Jobs covered", value: String(summary.job_count) },
    { label: "Avg per run", value: formatCurrency(summary.avg_cost_per_run, summary.currency) },
  ];
  document.getElementById("summary-grid").innerHTML = items
    .map((item) => `
      <article class="metric-card">
        <p>${item.label}</p>
        <strong>${item.value}</strong>
      </article>
    `)
    .join("");
}

function renderCostChart(points, currency) {
  const root = document.getElementById("cost-chart");
  if (!points.length) {
    root.innerHTML = `<p class="empty">No cost data available for this window.</p>`;
    return;
  }
  const maxCost = Math.max(...points.map((point) => point.total_cost), 1);
  root.innerHTML = points
    .map((point) => `
      <div class="bar-row">
        <span>${point.date}</span>
        <div class="bar-track">
          <div class="bar-fill" style="width:${Math.max(8, (point.total_cost / maxCost) * 100)}%"></div>
        </div>
        <strong>${formatCurrency(point.total_cost, currency)}</strong>
      </div>
    `)
    .join("");
}

function renderCoverage(coverage) {
  document.getElementById("coverage-panel").innerHTML = `
    <div class="coverage-item">
      <span>Stored runs</span>
      <strong>${coverage.total_runs}</strong>
    </div>
    <div class="coverage-item">
      <span>Costed runs</span>
      <strong>${coverage.costed_runs}</strong>
    </div>
    <div class="coverage-item">
      <span>Uncosted runs</span>
      <strong>${coverage.uncosted_runs}</strong>
    </div>
    <div class="coverage-item stacked">
      <span>Priced node types</span>
      <strong>${coverage.priced_node_types.join(", ") || "None configured"}</strong>
    </div>
  `;
}

function renderTopJobs(jobs, currency) {
  const root = document.getElementById("top-jobs");
  if (!jobs.length) {
    root.innerHTML = `<p class="empty">No jobs available yet.</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Job</th>
          <th>Total cost</th>
          <th>Share</th>
          <th>Runs</th>
        </tr>
      </thead>
      <tbody>
        ${jobs.slice(0, 10).map((job) => `
          <tr>
            <td>${escapeHtml(job.job_name)}</td>
            <td>${formatCurrency(job.total_cost, currency)}</td>
            <td>${(job.cost_share * 100).toFixed(1)}%</td>
            <td>${job.run_count}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderInsights(insights, currency) {
  const root = document.getElementById("insights");
  if (!insights.length) {
    root.innerHTML = `<p class="empty">No insights generated yet.</p>`;
    return;
  }
  root.innerHTML = insights
    .map((insight) => `
      <article class="insight-card">
        <p class="insight-kind">${escapeHtml(insight.kind)}</p>
        <h3>${escapeHtml(insight.subject_type)} ${escapeHtml(insight.subject_key)}</h3>
        <p>${escapeHtml(insight.message)}</p>
      </article>
    `)
    .join("");
}

function renderRecentRuns(runs, currency) {
  const root = document.getElementById("recent-runs");
  if (!runs.length) {
    root.innerHTML = `<p class="empty">No recent runs available yet.</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Run</th>
          <th>Job</th>
          <th>Started</th>
          <th>Duration</th>
          <th>Cost</th>
        </tr>
      </thead>
      <tbody>
        ${runs.map((run) => `
          <tr>
            <td>${run.run_page_url
              ? `<a href="${run.run_page_url}" target="_blank" rel="noreferrer">${run.run_id}</a>`
              : run.run_id}</td>
            <td>${escapeHtml(run.job_name)}</td>
            <td>${run.start_time ? formatTimestamp(run.start_time) : "n/a"}</td>
            <td>${formatDuration(run.duration_ms)}</td>
            <td>${formatCurrency(run.estimated_cost, currency)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderEmptyState() {
  document.getElementById("summary-grid").innerHTML = "";
  document.getElementById("cost-chart").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("coverage-panel").innerHTML = "";
  document.getElementById("top-jobs").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("insights").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("recent-runs").innerHTML = `<p class="empty">No data.</p>`;
}

function setStatus(message) {
  statusMessage.textContent = message;
}

function formatCurrency(value, currency) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: currency || "USD",
    maximumFractionDigits: 2,
  }).format(value || 0);
}

function formatDuration(durationMs) {
  const totalMinutes = Math.round((durationMs || 0) / 60000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (!hours) {
    return `${minutes}m`;
  }
  return `${hours}h ${minutes}m`;
}

function formatTimestamp(value) {
  const date = typeof value === "number" ? new Date(value) : new Date(value);
  return date.toLocaleString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

loadDashboard();
