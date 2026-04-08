const state = {
  activePanel: "overview",
  windowDays: 30,
  dashboard: null,
  pricingConfig: null,
};

const pageTitles = {
  overview: "Dashboard",
  finops: "FinOps",
  config: "Config",
  jobs: "Job observability",
  freshness: "Data freshness",
  alerts: "Alerts",
};

const statusMessage = document.getElementById("status-message");
const sidebarSync = document.getElementById("sidebar-sync");
const pageTitle = document.getElementById("page-title");
const syncButton = document.getElementById("sync-button");
const refreshConfigButton = document.getElementById("refresh-config-button");
const saveConfigButton = document.getElementById("save-config-button");

document.querySelectorAll(".nav-item[data-panel]").forEach((button) => {
  button.addEventListener("click", () => switchPanel(button.dataset.panel));
});

document.querySelectorAll(".pill[data-window]").forEach((button) => {
  button.addEventListener("click", async () => {
    const nextWindow = Number(button.dataset.window);
    if (!nextWindow || nextWindow === state.windowDays) {
      return;
    }
    state.windowDays = nextWindow;
    updateWindowButtons();
    await loadDashboard();
  });
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
    setStatus(
      `Sync complete. Saved ${payload.saved_jobs} jobs, ${payload.saved_runs} runs, ${payload.saved_clusters} clusters.`
    );
    await loadDashboard();
  } catch (error) {
    setStatus(error.message);
  } finally {
    syncButton.disabled = false;
  }
});

refreshConfigButton.addEventListener("click", async () => {
  await loadPricingConfig({ refresh: true });
});

saveConfigButton.addEventListener("click", async () => {
  await savePricingConfig();
});

function switchPanel(panelName) {
  state.activePanel = panelName;
  pageTitle.textContent = pageTitles[panelName] || "Dashboard";

  document.querySelectorAll(".nav-item[data-panel]").forEach((button) => {
    button.classList.toggle("active", button.dataset.panel === panelName);
  });
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.classList.toggle("active", panel.id === `panel-${panelName}`);
  });
  if (panelName === "config") {
    loadPricingConfig();
  }
}

function updateWindowButtons() {
  document.querySelectorAll(".pill[data-window]").forEach((button) => {
    button.classList.toggle("active", Number(button.dataset.window) === state.windowDays);
  });
}

async function loadDashboard() {
  setStatus("Loading dashboard data…");
  try {
    const response = await fetch(`/api/finops/dashboard?window_days=${state.windowDays}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Could not load dashboard.");
    }
    state.dashboard = payload;
    renderDashboard(payload);
    const syncText = payload.summary.last_sync_at
      ? formatTimestamp(payload.summary.last_sync_at)
      : "Waiting for first sync";
    sidebarSync.textContent = syncText;
    setStatus(
      payload.summary.last_sync_at
        ? `Window ${payload.summary.window_days}d loaded. Last sync ${formatTimestamp(payload.summary.last_sync_at)}.`
        : "No sync has been run yet. Configure the backend env vars and trigger the first sync."
    );
  } catch (error) {
    renderEmptyState();
    setStatus(error.message);
  }
}

async function loadPricingConfig(options = {}) {
  const shouldRefresh = Boolean(options.refresh);
  if (shouldRefresh) {
    refreshConfigButton.disabled = true;
    setStatus("Refreshing cluster node types from Databricks…");
  } else {
    setStatus("Loading cluster pricing config…");
  }
  try {
    const response = await fetch(`/api/finops/config?refresh=${shouldRefresh ? "true" : "false"}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Could not load cluster pricing config.");
    }
    state.pricingConfig = payload;
    renderPricingConfig(payload);
    setStatus(
      payload.last_refreshed_at
        ? `Cluster pricing config loaded. Last refresh ${formatTimestamp(payload.last_refreshed_at)}.`
        : "Cluster pricing config loaded. Refresh cluster types to discover node types from Databricks."
    );
  } catch (error) {
    renderPricingConfigEmpty(error.message);
    setStatus(error.message);
  } finally {
    refreshConfigButton.disabled = false;
  }
}

async function savePricingConfig() {
  const rows = Array.from(document.querySelectorAll("#config-table tbody tr[data-node-type-id]"));
  const entries = rows.map((row) => ({
    node_type_id: row.dataset.nodeTypeId,
    dbus_per_hour: Number(row.querySelector('input[data-field="dbus"]').value || 0),
    plan: row.querySelector('select[data-field="plan"]').value,
  }));

  saveConfigButton.disabled = true;
  setStatus("Saving cluster pricing config…");
  try {
    const response = await fetch("/api/finops/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ entries }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Could not save cluster pricing config.");
    }
    state.pricingConfig = payload;
    renderPricingConfig(payload);
    setStatus("Cluster pricing config saved.");
    await loadDashboard();
  } catch (error) {
    setStatus(error.message);
  } finally {
    saveConfigButton.disabled = false;
  }
}

function renderDashboard(payload) {
  renderOverviewMetrics(payload.summary);
  renderOverviewTopJobs(payload.top_jobs, payload.summary.currency);
  renderInsightList(
    document.getElementById("overview-insights"),
    payload.insights.slice(0, 4)
  );
  renderCostChart(document.getElementById("overview-cost-chart"), payload.cost_over_time, payload.summary.currency);
  renderCoverage(document.getElementById("overview-coverage"), payload.coverage);

  renderFinOpsMetrics(payload.summary, payload.top_jobs, payload.top_tables);
  renderBarList(document.getElementById("finops-jobs"), payload.top_jobs, payload.summary.currency);
  renderInsightList(
    document.getElementById("finops-insights"),
    payload.insights.filter((insight) => insight.subject_type === "job" || insight.subject_type === "table" || insight.subject_type === "pipeline")
  );
  renderNamedCostTable(
    document.getElementById("finops-pipelines"),
    payload.top_pipelines,
    payload.summary.currency,
    { primaryLabel: "Pipeline", includeCostPerDay: false }
  );
  renderNamedCostTable(
    document.getElementById("finops-tables"),
    payload.top_tables,
    payload.summary.currency,
    { primaryLabel: "Table", includeCostPerDay: true }
  );
  renderRecentRuns(document.getElementById("finops-runs"), payload.recent_runs, payload.summary.currency);
}

function renderPricingConfig(payload) {
  renderPricingSummary(payload);
  const root = document.getElementById("config-table");
  if (!payload.entries.length) {
    root.innerHTML = `<p class="empty">No node types discovered yet. Refresh cluster types to query Databricks.</p>`;
    return;
  }

  root.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Node type</th>
          <th>DBUs / hour</th>
          <th>Plan</th>
          <th>Jobs rate</th>
          <th>All-purpose rate</th>
        </tr>
      </thead>
      <tbody>
        ${payload.entries.map((entry) => `
          <tr data-node-type-id="${escapeAttribute(entry.node_type_id)}">
            <td>${escapeHtml(entry.node_type_id)}</td>
            <td>
              <input
                class="table-input"
                data-field="dbus"
                type="number"
                min="0"
                step="0.01"
                value="${Number(entry.dbus_per_hour || 0)}"
              />
            </td>
            <td>
              <select class="table-input" data-field="plan">
                <option value="premium"${entry.plan === "premium" ? " selected" : ""}>Premium</option>
                <option value="enterprise"${entry.plan === "enterprise" ? " selected" : ""}>Enterprise</option>
              </select>
            </td>
            <td>${formatCurrency(entry.jobs_rate_per_hour, payload.currency)}</td>
            <td>${formatCurrency(entry.all_purpose_rate_per_hour, payload.currency)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderPricingSummary(payload) {
  const configuredCount = payload.entries.filter((entry) => Number(entry.dbus_per_hour) > 0).length;
  document.getElementById("config-summary").innerHTML = `
    <div class="coverage-item">
      <span>Discovered node types</span>
      <strong>${payload.cluster_node_types.length}</strong>
    </div>
    <div class="coverage-item">
      <span>Configured node types</span>
      <strong>${configuredCount}</strong>
    </div>
    <div class="coverage-item stacked">
      <span>Last refresh</span>
      <strong>${payload.last_refreshed_at ? formatTimestamp(payload.last_refreshed_at) : "Not refreshed yet"}</strong>
    </div>
  `;
}

function renderPricingConfigEmpty(message) {
  document.getElementById("config-summary").innerHTML = "";
  document.getElementById("config-table").innerHTML = `<p class="empty">${escapeHtml(message)}</p>`;
}

function renderOverviewMetrics(summary) {
  const items = [
    {
      label: "Total compute cost",
      value: formatCurrency(summary.total_cost, summary.currency),
      detail: `${summary.window_days}-day analysis window`,
    },
    {
      label: "Active jobs",
      value: String(summary.job_count),
      detail: summary.most_expensive_job ? `Top driver: ${summary.most_expensive_job}` : "No priced jobs yet",
    },
    {
      label: "Mapped pipelines",
      value: String(summary.pipeline_count),
      detail: summary.most_expensive_pipeline || "No pipeline metadata found",
    },
    {
      label: "Maintained datasets",
      value: String(summary.table_count),
      detail: summary.most_expensive_table || "No table metadata found",
    },
  ];
  document.getElementById("overview-metrics").innerHTML = items.map(renderMetricCard).join("");
}

function renderFinOpsMetrics(summary, jobs, tables) {
  const topJob = jobs[0];
  const topTable = tables[0];
  const items = [
    {
      label: "Total spend",
      value: formatCurrency(summary.total_cost, summary.currency),
      detail: `${summary.run_count} priced runs`,
    },
    {
      label: "Cost per run",
      value: formatCurrency(summary.avg_cost_per_run, summary.currency),
      detail: `Across ${summary.window_days} days`,
    },
    {
      label: "Most expensive job",
      value: topJob ? escapeHtml(topJob.job_name) : "n/a",
      detail: topJob ? `${(topJob.cost_share * 100).toFixed(1)}% of spend` : "No jobs priced",
      compact: true,
    },
    {
      label: "Top dataset per day",
      value: topTable ? formatCurrency(topTable.cost_per_day, summary.currency) : "n/a",
      detail: topTable ? escapeHtml(topTable.label) : "No table attribution",
    },
  ];
  document.getElementById("finops-metrics").innerHTML = items.map(renderMetricCard).join("");
}

function renderMetricCard(item) {
  return `
    <article class="metric-card">
      <div class="metric-label">${escapeHtml(item.label)}</div>
      <div class="metric-value${item.compact ? " compact" : ""}">${item.value}</div>
      <div class="metric-delta">${item.detail}</div>
    </article>
  `;
}

function renderOverviewTopJobs(jobs, currency) {
  const root = document.getElementById("overview-top-jobs");
  if (!jobs.length) {
    root.innerHTML = `<p class="empty">No jobs available yet.</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Job</th>
          <th>Cost</th>
          <th>Share</th>
        </tr>
      </thead>
      <tbody>
        ${jobs.slice(0, 6).map((job) => `
          <tr>
            <td>${escapeHtml(job.job_name)}</td>
            <td>${formatCurrency(job.total_cost, currency)}</td>
            <td>${(job.cost_share * 100).toFixed(1)}%</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderCostChart(root, points, currency) {
  if (!points.length) {
    root.innerHTML = `<p class="empty">No cost data available for this window.</p>`;
    return;
  }
  const maxCost = Math.max(...points.map((point) => point.total_cost), 1);
  root.innerHTML = points.map((point) => `
    <div class="bar-item">
      <span class="bar-name">${escapeHtml(point.date)}</span>
      <div class="bar-track">
        <div class="bar-fill" style="width:${Math.max(8, (point.total_cost / maxCost) * 100)}%"></div>
      </div>
      <span class="bar-val">${formatCurrency(point.total_cost, currency)}</span>
    </div>
  `).join("");
}

function renderCoverage(root, coverage) {
  root.innerHTML = `
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
      <strong>${coverage.priced_node_types.length ? coverage.priced_node_types.map(escapeHtml).join(", ") : "None configured"}</strong>
    </div>
  `;
}

function renderBarList(root, jobs, currency) {
  if (!jobs.length) {
    root.innerHTML = `<p class="empty">No job cost data available yet.</p>`;
    return;
  }
  const maxCost = Math.max(...jobs.map((job) => job.total_cost), 1);
  root.innerHTML = jobs.slice(0, 8).map((job) => `
    <div class="bar-item">
      <span class="bar-name">${escapeHtml(job.job_name)}</span>
      <div class="bar-track">
        <div class="bar-fill" style="width:${Math.max(10, (job.total_cost / maxCost) * 100)}%"></div>
      </div>
      <span class="bar-val">${formatCurrency(job.total_cost, currency)}</span>
    </div>
  `).join("");
}

function renderInsightList(root, insights) {
  if (!insights.length) {
    root.innerHTML = `<p class="empty">No insights generated yet.</p>`;
    return;
  }
  root.innerHTML = insights.slice(0, 6).map((insight) => `
    <article class="insight-card">
      <div class="insight-icon ${iconClassForInsight(insight)}">${iconForInsight(insight)}</div>
      <div class="insight-body">
        <div class="insight-title">${escapeHtml(insight.message)}</div>
        <div class="insight-desc">${escapeHtml(insight.subject_type)} / ${escapeHtml(insight.subject_key)}</div>
        <span class="insight-tag ${tagClassForInsight(insight)}">${escapeHtml(insight.subject_type)}</span>
      </div>
    </article>
  `).join("");
}

function renderNamedCostTable(root, rows, currency, options) {
  if (!rows.length) {
    root.innerHTML = `<p class="empty">No ${escapeHtml(options.primaryLabel.toLowerCase())} attribution metadata available.</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>${escapeHtml(options.primaryLabel)}</th>
          <th>Total cost</th>
          ${options.includeCostPerDay ? "<th>Cost / day</th>" : "<th>Share</th>"}
          <th>Runs</th>
        </tr>
      </thead>
      <tbody>
        ${rows.slice(0, 8).map((row) => `
          <tr>
            <td>${escapeHtml(row.label)}</td>
            <td>${formatCurrency(row.total_cost, currency)}</td>
            <td>${options.includeCostPerDay ? formatCurrency(row.cost_per_day, currency) : `${(row.cost_share * 100).toFixed(1)}%`}</td>
            <td>${row.run_count}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderRecentRuns(root, runs, currency) {
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
          <th>Source</th>
        </tr>
      </thead>
      <tbody>
        ${runs.map((run) => `
          <tr>
            <td>${run.run_page_url ? `<a href="${escapeAttribute(run.run_page_url)}" target="_blank" rel="noreferrer">${run.run_id}</a>` : run.run_id}</td>
            <td>${escapeHtml(run.job_name)}</td>
            <td>${run.start_time ? formatTimestamp(run.start_time) : "n/a"}</td>
            <td>${formatDuration(run.duration_ms)}</td>
            <td>${formatCurrency(run.estimated_cost, currency)}</td>
            <td>${escapeHtml(run.source)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderEmptyState() {
  document.getElementById("overview-metrics").innerHTML = "";
  document.getElementById("overview-top-jobs").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("overview-insights").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("overview-cost-chart").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("overview-coverage").innerHTML = "";
  document.getElementById("finops-metrics").innerHTML = "";
  document.getElementById("finops-jobs").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("finops-insights").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("finops-pipelines").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("finops-tables").innerHTML = `<p class="empty">No data.</p>`;
  document.getElementById("finops-runs").innerHTML = `<p class="empty">No data.</p>`;
}

function tagClassForInsight(insight) {
  if (insight.subject_type === "table") {
    return "tag-fresh";
  }
  if (insight.subject_type === "pipeline") {
    return "tag-job";
  }
  return "tag-finops";
}

function iconClassForInsight(insight) {
  if (insight.kind.includes("dominant")) {
    return "ic-warn";
  }
  if (insight.subject_type === "table") {
    return "ic-info";
  }
  return "ic-teal";
}

function iconForInsight(insight) {
  if (insight.subject_type === "table") {
    return "≈";
  }
  if (insight.kind.includes("dominant")) {
    return "!";
  }
  return "↑";
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

function escapeAttribute(value) {
  return escapeHtml(value);
}

switchPanel("overview");
updateWindowButtons();
loadDashboard();
