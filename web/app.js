const MAX_HOLDINGS_ASSETS = 8;
const HOLDINGS_OTHER_COLOR = {
  stroke: "#94a3b8",
  fill: "rgba(148, 163, 184, 0.20)",
};

const HOLDINGS_PALETTE = [
  { stroke: "#4fa3ff", fill: "rgba(79, 163, 255, 0.22)" },
  { stroke: "#9d71ff", fill: "rgba(157, 113, 255, 0.20)" },
  { stroke: "#f7b733", fill: "rgba(247, 183, 51, 0.20)" },
  { stroke: "#1dd1a1", fill: "rgba(29, 209, 161, 0.20)" },
  { stroke: "#ff6b6b", fill: "rgba(255, 107, 107, 0.20)" },
  { stroke: "#48dbfb", fill: "rgba(72, 219, 251, 0.20)" },
  { stroke: "#f368e0", fill: "rgba(243, 104, 224, 0.20)" },
  { stroke: "#ffa502", fill: "rgba(255, 165, 2, 0.20)" },
  { stroke: "#2ed573", fill: "rgba(46, 213, 115, 0.20)" },
  { stroke: "#70a1ff", fill: "rgba(112, 161, 255, 0.20)" },
];

const state = {
  index: null,
  funds: [],
  filteredFunds: [],
  currentFund: null,
  charts: {},
  filters: {},
  searchQuery: "",
  holdingsDate: null,
};

function setStatus(message, type = "info") {
  const banner = document.getElementById("fund-status");
  if (!banner) return;
  if (!message) {
    banner.textContent = "";
    banner.classList.add("hidden");
    delete banner.dataset.type;
    return;
  }
  banner.textContent = message;
  banner.classList.remove("hidden");
  banner.dataset.type = type;
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Falha ao carregar ${path}: ${response.status}`);
  }
  return response.json();
}

function formatCurrency(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 2,
  });
}

function formatNumber(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return "-";
  }
  return Math.round(Number(value)).toLocaleString("pt-BR");
}

function formatPercent(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return "-";
  }
  return `${Number(value).toLocaleString("pt-BR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })}%`;
}

function normalizeText(text) {
  return (text || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function setActiveButton(cnpj) {
  document.querySelectorAll(".fund-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.cnpj === cnpj);
  });
}

function ensureChart(id, config) {
  if (state.charts[id]) {
    state.charts[id].destroy();
  }
  const ctx = document.getElementById(id);
  if (!ctx) return;
  state.charts[id] = new Chart(ctx, config);
}

function createFilter(min, max) {
  return {
    start: min || null,
    end: max || null,
    min: min || null,
    max: max || null,
  };
}

function updateFilter(filterKey, partial) {
  const filter = state.filters[filterKey] || {};
  if (Object.prototype.hasOwnProperty.call(partial, "start")) {
    filter.start = partial.start || null;
  }
  if (Object.prototype.hasOwnProperty.call(partial, "end")) {
    filter.end = partial.end || null;
  }

  if (filter.start && filter.end && filter.start > filter.end) {
    if (Object.prototype.hasOwnProperty.call(partial, "start")) {
      filter.end = filter.start;
      const endInput = document.getElementById(`${filterKey}-end`);
      if (endInput) {
        endInput.value = filter.end;
      }
    } else if (Object.prototype.hasOwnProperty.call(partial, "end")) {
      filter.start = filter.end;
      const startInput = document.getElementById(`${filterKey}-start`);
      if (startInput) {
        startInput.value = filter.start;
      }
    }
  }

  state.filters[filterKey] = filter;
}

function filterSeries(data, filterKey, accessor) {
  if (!Array.isArray(data) || !data.length) {
    return [];
  }
  const filter = state.filters[filterKey];
  if (!filter || (!filter.start && !filter.end)) {
    return data.slice();
  }
  return data.filter((item) => {
    const value = accessor(item);
    if (!value) return false;
    if (filter.start && value < filter.start) return false;
    if (filter.end && value > filter.end) return false;
    return true;
  });
}

function setupDateFilter(filterKey, dataset, accessor, onChange) {
  const startInput = document.getElementById(`${filterKey}-start`);
  const endInput = document.getElementById(`${filterKey}-end`);
  const resetButton = document.querySelector(
    `.reset-filter[data-filter="${filterKey}"]`
  );

  const dates = Array.from(
    new Set((dataset || []).map(accessor).filter(Boolean))
  ).sort();

  if (!dates.length) {
    state.filters[filterKey] = createFilter(null, null);
    if (startInput) {
      startInput.value = "";
      startInput.disabled = true;
      startInput.min = "";
      startInput.max = "";
      startInput.onchange = null;
    }
    if (endInput) {
      endInput.value = "";
      endInput.disabled = true;
      endInput.min = "";
      endInput.max = "";
      endInput.onchange = null;
    }
    if (resetButton) {
      resetButton.disabled = true;
      resetButton.onclick = null;
    }
    return;
  }

  const min = dates[0];
  const max = dates[dates.length - 1];
  state.filters[filterKey] = createFilter(min, max);

  if (startInput) {
    startInput.disabled = false;
    startInput.min = min;
    startInput.max = max;
    startInput.value = min;
    startInput.onchange = () => {
      updateFilter(filterKey, { start: startInput.value || null });
      onChange();
    };
  }

  if (endInput) {
    endInput.disabled = false;
    endInput.min = min;
    endInput.max = max;
    endInput.value = max;
    endInput.onchange = () => {
      updateFilter(filterKey, { end: endInput.value || null });
      onChange();
    };
  }

  if (resetButton) {
    resetButton.disabled = false;
    resetButton.onclick = () => {
      const filter = state.filters[filterKey];
      updateFilter(filterKey, {
        start: filter.min || null,
        end: filter.max || null,
      });
      if (startInput) {
        startInput.value = filter.min || "";
      }
      if (endInput) {
        endInput.value = filter.max || "";
      }
      onChange();
    };
  }
}

function initializeFilters(fund) {
  const series = fund.series || {};
  setupDateFilter("daily", series.daily || [], (item) => item.data, () =>
    updateValorCotaChart()
  );
  setupDateFilter("cotistas", series.cotistas || [], (item) => item.data, () =>
    updateCotistasChart()
  );
  setupDateFilter(
    "holdings",
    series.carteira_por_tipo || [],
    (item) => item.data,
    () => updateHoldings()
  );
  state.holdingsDate = null;
}

function clearFundDetails(message = "") {
  state.currentFund = null;
  document.getElementById("fund-name").textContent = "Selecione um fundo";
  document.getElementById("fund-meta").textContent = "";
  ["insights-date", "insights-valor-cota", "insights-patrimonio", "insights-cotistas"].forEach(
    (id) => {
      const el = document.getElementById(id);
      if (el) el.textContent = "-";
    }
  );
  Object.values(state.charts).forEach((chart) => chart.destroy());
  state.charts = {};
  if (message) {
    setStatus(message, "info");
  } else {
    setStatus("");
  }
  const tableBody = document.querySelector("#holdings-table tbody");
  if (tableBody) {
    tableBody.innerHTML = "";
  }
  const holdingsSelect = document.getElementById("holdings-date-select");
  if (holdingsSelect) {
    holdingsSelect.innerHTML = "";
    holdingsSelect.disabled = true;
  }
}

function updateSummary(fundData) {
  const meta = fundData.metadata || {};
  const latest = fundData.latest_snapshot || {};

  document.getElementById("fund-name").textContent = meta.nome || meta.cnpj;
  document.getElementById("fund-meta").textContent = `${meta.cnpj || "-"} · ${
    meta.categoria_cvm || "-"
  } · ${meta.gestora || "-"}`;

  document.getElementById("insights-date").textContent = latest.data || "-";
  document.getElementById("insights-valor-cota").textContent = formatCurrency(
    latest.valor_cota
  );
  document.getElementById("insights-patrimonio").textContent = formatCurrency(
    latest.patrimonio_liquido
  );

  const latestCotistas = fundData.latest_cotistas || {};
  document.getElementById("insights-cotistas").textContent = formatNumber(
    latestCotistas.numero_cotistas ?? latest.numero_cotistas
  );
}

function updateValorCotaChart() {
  const series = state.currentFund?.series || {};
  const data = filterSeries(series.daily || [], "daily", (item) => item.data);
  if (!data.length) {
    ensureChart("valor-cota-chart", {
      type: "line",
      data: { labels: [], datasets: [] },
    });
    return;
  }

  const labels = data.map((item) => item.data);
  const valores = data.map((item) => item.valor_cota);
  const retornos = data.map((item) => item.retorno_pct);

  ensureChart("valor-cota-chart", {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Valor da cota",
          data: valores,
          borderColor: "#4fa3ff",
          backgroundColor: "rgba(79, 163, 255, 0.2)",
          tension: 0.25,
          pointRadius: 0,
          yAxisID: "y",
        },
        {
          label: "Retorno diário (%)",
          data: retornos,
          borderColor: "#f7b733",
          backgroundColor: "rgba(247, 183, 51, 0.25)",
          type: "bar",
          yAxisID: "y1",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      scales: {
        y: {
          type: "linear",
          position: "left",
          ticks: {
            callback: (value) =>
              Number(value).toLocaleString("pt-BR", {
                minimumFractionDigits: 2,
              }),
          },
          grid: {
            color: "rgba(255,255,255,0.05)",
          },
        },
        y1: {
          type: "linear",
          display: true,
          position: "right",
          grid: {
            drawOnChartArea: false,
          },
          ticks: {
            callback: (value) => formatPercent(value),
          },
        },
        x: {
          ticks: {
            maxTicksLimit: 10,
          },
          grid: {
            display: false,
          },
        },
      },
      plugins: {
        legend: {
          display: true,
        },
        tooltip: {
          callbacks: {
            label(context) {
              const label = context.dataset.label || "";
              const value = context.parsed.y;
              if (context.dataset.yAxisID === "y") {
                return `${label}: ${formatCurrency(value)}`;
              }
              return `${label}: ${formatPercent(value)}`;
            },
          },
        },
      },
    },
  });
}

function updateCotistasChart() {
  const series = state.currentFund?.series || {};
  const data = filterSeries(series.cotistas || [], "cotistas", (item) => item.data);
  if (!data.length) {
    ensureChart("cotistas-chart", {
      type: "line",
      data: { labels: [], datasets: [] },
    });
    return;
  }

  const labels = data.map((item) => item.data);
  const cotistas = data.map((item) => item.numero_cotistas);
  const patrimonio = data.map((item) => item.patrimonio_liquido);

  ensureChart("cotistas-chart", {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          type: "line",
          label: "Número de cotistas",
          data: cotistas,
          borderColor: "#4fa3ff",
          backgroundColor: "rgba(79,163,255,0.2)",
          tension: 0.2,
          pointRadius: 0,
          yAxisID: "yCotistas",
        },
        {
          type: "bar",
          label: "Patrimônio líquido (R$)",
          data: patrimonio,
          backgroundColor: "rgba(157, 113, 255, 0.25)",
          borderColor: "rgba(157, 113, 255, 0.45)",
          yAxisID: "yPatrimonio",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      scales: {
        yCotistas: {
          type: "linear",
          position: "left",
          ticks: {
            callback: (value) => formatNumber(value),
          },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
        yPatrimonio: {
          type: "linear",
          position: "right",
          grid: { drawOnChartArea: false },
          ticks: {
            callback: (value) => formatCurrency(value),
          },
        },
        x: {
          ticks: { maxTicksLimit: 10 },
          grid: { display: false },
        },
      },
      plugins: {
        legend: {
          display: true,
        },
        tooltip: {
          callbacks: {
            label(context) {
              const label = context.dataset.label || "";
              const value = context.parsed.y;
              if (context.dataset.yAxisID === "yCotistas") {
                return `${label}: ${formatNumber(value)}`;
              }
              return `${label}: ${formatCurrency(value)}`;
            },
          },
        },
      },
    },
  });
}

function renderHoldingsTimeline(data) {
  if (!data.length) {
    ensureChart("holdings-timeline-chart", {
      type: "line",
      data: { labels: [], datasets: [] },
    });
    return;
  }

  const dates = Array.from(new Set(data.map((item) => item.data))).sort();
  const typeNames = Array.from(
    new Set(data.map((item) => item.tipo_ativo || "Não classificado"))
  );

  const timelineDatasets = typeNames.map((type, index) => {
    const map = new Map();
    data
      .filter((item) => (item.tipo_ativo || "Não classificado") === type)
      .forEach((item) => map.set(item.data, Number(item.percentual) || 0));
    const palette = HOLDINGS_PALETTE[index % HOLDINGS_PALETTE.length];
    return {
      label: type,
      data: dates.map((date) => map.get(date) ?? 0),
      borderColor: palette.stroke,
      backgroundColor: palette.fill,
      fill: true,
      tension: 0.25,
      pointRadius: 0,
      borderWidth: 1.5,
      stack: "classes",
    };
  });

  ensureChart("holdings-timeline-chart", {
    type: "line",
    data: { labels: dates, datasets: timelineDatasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: {
          mode: "index",
          intersect: false,
          callbacks: {
            label(context) {
              const label = context.dataset.label || "";
              return `${label}: ${formatPercent(context.parsed.y)}`;
            },
          },
        },
      },
      scales: {
        y: {
          stacked: true,
          title: { display: true, text: "Participação (%)" },
          ticks: { callback: (value) => formatPercent(value) },
          min: 0,
          max: 100,
        },
        x: { ticks: { maxTicksLimit: 12 } },
      },
    },
  });
}

function renderHoldingsSnapshot(selectedDate, rows, fallbackLatest) {
  const donutLabels = [];
  const donutValues = [];
  const tableRows = [];

  if (rows.length) {
    const sortedRows = [...rows].sort(
      (a, b) => Number(b.valor_mercado) - Number(a.valor_mercado)
    );
    const totalValue = sortedRows.reduce(
      (sum, item) => sum + (Number(item.valor_mercado) || 0),
      0
    );

    sortedRows.forEach((item) => {
      const valor = Number(item.valor_mercado) || 0;
      const percentual =
        Number.isFinite(Number(item.percentual))
          ? Number(item.percentual)
          : totalValue > 0
          ? (valor / totalValue) * 100
          : 0;
      donutLabels.push(item.emissor || "Sem emissor");
      donutValues.push(Number(percentual.toFixed(2)));
      tableRows.push({
        emissor: item.emissor || "-",
        tipo_ativo: item.tipo_ativo || "-",
        valor: valor,
        percentual,
      });
    });
  } else {
    (fallbackLatest.top || []).forEach((item) => {
      donutLabels.push(item.emissor || "Sem emissor");
      donutValues.push(Number(item.percentual || 0));
      tableRows.push({
        emissor: item.emissor || "-",
        tipo_ativo: item.tipo_ativo || "-",
        valor: Number(item.valor_mercado) || 0,
        percentual: Number(item.percentual) || 0,
      });
    });
  }

  ensureChart("holdings-chart", {
    type: "doughnut",
    data: {
      labels: donutLabels,
      datasets: [
        {
          data: donutValues,
          backgroundColor: donutLabels.map(
            (_, index) => HOLDINGS_PALETTE[index % HOLDINGS_PALETTE.length].stroke
          ),
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "right" },
        tooltip: {
          callbacks: {
            label(context) {
              const label = context.label || "";
              return `${label}: ${formatPercent(context.parsed)}`;
            },
          },
        },
      },
    },
  });

  const tbody = document.querySelector("#holdings-table tbody");
  if (!tbody) {
    return;
  }

  tbody.innerHTML = "";

  if (!tableRows.length) {
    const row = document.createElement("tr");
    row.innerHTML = '<td colspan="4">Sem composição disponível.</td>';
    tbody.appendChild(row);
    return;
  }

  tableRows
    .sort((a, b) => b.percentual - a.percentual)
    .forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.emissor}</td>
        <td>${item.tipo_ativo}</td>
        <td>${formatCurrency(item.valor)}</td>
        <td>${Number(item.percentual).toFixed(2)}%</td>
      `;
      tbody.appendChild(row);
    });

  const dateLabel = document.getElementById("holdings-date-label");
  if (dateLabel) {
    dateLabel.textContent = selectedDate || "-";
  }
}

function renderHoldingsAssetsHistory(assetData, dates) {
  const chartDates = Array.from(
    new Set(assetData.map((item) => item.data))
  ).sort();

  if (!chartDates.length) {
    ensureChart("holdings-assets-chart", {
      type: "line",
      data: { labels: [], datasets: [] },
    });
    return;
  }

  const assetMap = new Map();
  assetData.forEach((item) => {
    const labelBase = item.emissor || "Sem emissor";
    const label = item.isin ? `${labelBase} (${item.isin})` : labelBase;
    const key = item.isin || label;
    const percent = Number(item.percentual) || 0;

    if (!assetMap.has(key)) {
      assetMap.set(key, {
        label,
        values: new Map(),
        total: 0,
      });
    }

    const entry = assetMap.get(key);
    entry.values.set(item.data, percent);
    entry.total += percent;
  });

  const sortedAssets = Array.from(assetMap.values()).sort(
    (a, b) => b.total - a.total
  );
  const topAssets = sortedAssets.slice(0, MAX_HOLDINGS_ASSETS);
  const otherAssets = sortedAssets.slice(MAX_HOLDINGS_ASSETS);

  const datasets = topAssets.map((asset, index) => {
    const palette = HOLDINGS_PALETTE[index % HOLDINGS_PALETTE.length];
    return {
      label: asset.label,
      data: chartDates.map((date) => asset.values.get(date) ?? 0),
      borderColor: palette.stroke,
      backgroundColor: palette.fill,
      fill: true,
      tension: 0.25,
      pointRadius: 0,
      borderWidth: 1.5,
      stack: "ativos",
    };
  });

  if (otherAssets.length) {
    const otherValues = chartDates.map((date) =>
      otherAssets.reduce(
        (sum, asset) => sum + (asset.values.get(date) ?? 0),
        0
      )
    );
    datasets.push({
      label: "Outros",
      data: otherValues,
      borderColor: HOLDINGS_OTHER_COLOR.stroke,
      backgroundColor: HOLDINGS_OTHER_COLOR.fill,
      fill: true,
      tension: 0.25,
      pointRadius: 0,
      borderWidth: 1.5,
      stack: "ativos",
    });
  }

  ensureChart("holdings-assets-chart", {
    type: "line",
    data: { labels: chartDates, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: {
          mode: "index",
          intersect: false,
          callbacks: {
            label(context) {
              const label = context.dataset.label || "";
              return `${label}: ${formatPercent(context.parsed.y)}`;
            },
          },
        },
      },
      scales: {
        y: {
          stacked: true,
          title: { display: true, text: "Participação (%)" },
          ticks: { callback: (value) => formatPercent(value) },
          min: 0,
          max: 100,
        },
        x: { ticks: { maxTicksLimit: 12 } },
      },
    },
  });
}

function updateHoldings() {
  const fund = state.currentFund;
  if (!fund) {
    ensureChart("holdings-timeline-chart", {
      type: "line",
      data: { labels: [], datasets: [] },
    });
    ensureChart("holdings-chart", {
      type: "doughnut",
      data: { labels: [], datasets: [] },
    });
    ensureChart("holdings-assets-chart", {
      type: "line",
      data: { labels: [], datasets: [] },
    });
    return;
  }

  const series = fund.series || {};
  const timelineData = filterSeries(
    series.carteira_por_tipo || [],
    "holdings",
    (item) => item.data
  );
  renderHoldingsTimeline(timelineData);

  const assetAll = series.carteira_por_ativo || [];
  const assetAllDates = Array.from(
    new Set(assetAll.map((item) => item.data).filter(Boolean))
  ).sort();
  const assetFiltered = filterSeries(assetAll, "holdings", (item) => item.data);

  const dateSelect = document.getElementById("holdings-date-select");
  if (dateSelect) {
    dateSelect.innerHTML = "";
    if (!assetAllDates.length) {
      dateSelect.disabled = true;
    } else {
      assetAllDates.forEach((date) => {
        const option = document.createElement("option");
        option.value = date;
        option.textContent = date;
        dateSelect.appendChild(option);
      });
      dateSelect.disabled = false;
      dateSelect.onchange = (event) => {
        state.holdingsDate = event.target.value || null;
        updateHoldings();
      };
    }
  }

  let selectedDate = state.holdingsDate;
  if (!selectedDate || !assetAllDates.includes(selectedDate)) {
    selectedDate = assetAllDates[assetAllDates.length - 1] || null;
  }
  state.holdingsDate = selectedDate;
  if (dateSelect) {
    dateSelect.value = selectedDate || "";
  }

  let rowsForDate = [];
  if (selectedDate) {
    rowsForDate = assetFiltered.filter((item) => item.data === selectedDate);
    if (!rowsForDate.length) {
      rowsForDate = assetAll.filter((item) => item.data === selectedDate);
    }
  }

  renderHoldingsSnapshot(selectedDate, rowsForDate, fund.latest_holdings || {});
  renderHoldingsAssetsHistory(assetFiltered.length ? assetFiltered : assetAll, assetAllDates);
}

function selectFund(cnpj) {
  return fetchJson(`data/funds/${cnpj}.json`)
    .then((fund) => {
      state.currentFund = fund;
      setActiveButton(cnpj);
      setStatus("");
      initializeFilters(fund);
      updateSummary(fund);
      updateValorCotaChart();
      updateCotistasChart();
      updateHoldings();
    })
    .catch((error) => {
      console.error(error);
      setStatus(
        "Não foi possível carregar os dados do fundo selecionado. Tente novamente mais tarde.",
        "error"
      );
    });
}

function renderFundList() {
  const container = document.getElementById("fund-buttons");
  if (!container) return;

  container.innerHTML = "";
  const funds = state.filteredFunds.length
    ? state.filteredFunds
    : state.funds.slice();

  const currentCnpj = state.currentFund?.metadata?.cnpj;
  let hasCurrent = false;

  funds.forEach((fund) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "fund-button";
    button.dataset.cnpj = fund.cnpj;
    button.innerHTML = `<strong>${fund.nome}</strong><br /><small>${fund.cnpj}</small>`;
    button.addEventListener("click", () => selectFund(fund.cnpj));
    container.appendChild(button);

    if (fund.cnpj === currentCnpj) {
      hasCurrent = true;
      button.classList.add("active");
    }
  });

  if (!funds.length) {
    clearFundDetails(
      state.searchQuery
        ? "Nenhum fundo encontrado para a busca."
        : "Nenhum fundo disponível."
    );
    const emptyMsg = document.createElement("p");
    emptyMsg.className = "fund-empty";
    emptyMsg.textContent = state.searchQuery
      ? "Nenhum fundo encontrado para a busca."
      : "Nenhum fundo configurado.";
    container.appendChild(emptyMsg);
    return;
  }

  if (!hasCurrent) {
    selectFund(funds[0].cnpj);
  }
}

function applySearch(query) {
  state.searchQuery = query.trim();
  const normalizedQuery = normalizeText(state.searchQuery);
  if (!normalizedQuery) {
    state.filteredFunds = state.funds.slice();
  } else {
    state.filteredFunds = state.funds.filter((fund) => {
      const normalizedName = normalizeText(fund.nome);
      const normalizedCnpj = normalizeText(fund.cnpj);
      return (
        normalizedName.includes(normalizedQuery) ||
        normalizedCnpj.includes(normalizedQuery)
      );
    });
  }
  renderFundList();
}

function setupFundSearch() {
  const searchInput = document.getElementById("fund-search");
  if (!searchInput) return;
  searchInput.addEventListener("input", (event) => {
    applySearch(event.target.value);
  });
}

async function bootstrap() {
  try {
    const index = await fetchJson("data/index.json");
    state.index = index;
    state.funds = (index.funds || []).slice();
    state.filteredFunds = state.funds.slice();

    setupFundSearch();
    renderFundList();
  } catch (error) {
    console.error(error);
    clearFundDetails("Falha ao carregar os dados publicados.");
  }
}

document.addEventListener("DOMContentLoaded", bootstrap);
