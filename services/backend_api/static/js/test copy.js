// --- Utility Functions ---
const formatCurrency = (amount, exchange_mode) => {
  const val = Number(amount) || 0;
  if (exchange_mode && typeof exchange_mode === 'string') {
    const mode = exchange_mode.toLowerCase();
    if (mode === 'binance') {
      // For Binance, format as USD currency (e.g. "$1,000")
      return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      }).format(val);
    } else if (mode === 'nse') {
      // For NSE, format as INR currency (e.g. "₹50,13,982")
      return new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      }).format(val);
    }
  }
  // Default: return numeric value formatted to 2 decimals
  return val.toFixed(2);
};

const randomSecondsCache = {};
const formatDateShort = (dateString) => {
  const d = new Date(dateString);
  if (isNaN(d)) return ''; // Return empty string for invalid dates

  // Get the time part – if seconds are missing, fill in with a random cached second
  const timePart = dateString.split(' ')[1] || '';
  const timeComponents = timePart.split(':');
  if (timeComponents.length < 3) {
    if (!(dateString in randomSecondsCache)) {
      randomSecondsCache[dateString] = Math.floor(Math.random() * 60);
    }
    d.setSeconds(randomSecondsCache[dateString]);
  }

  // Format date (day and abbreviated month) and time (24-hour format)
  const date = d.toLocaleDateString('en-IN', { day: 'numeric', month: 'short' });
  const time = d.toLocaleTimeString('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  return `${date} ${time}`;
};

const formatDate = (dateString) => {
  const d = new Date(dateString);
  if (isNaN(d)) return ''; // Return empty string for invalid dates

  // Get the time part – if seconds are missing, fill in with a random cached second
  const timePart = dateString.split(' ')[1] || '';
  const timeComponents = timePart.split(':');
  if (timeComponents.length < 3) {
    if (!(dateString in randomSecondsCache)) {
      randomSecondsCache[dateString] = Math.floor(Math.random() * 60);
    }
    d.setSeconds(randomSecondsCache[dateString]);
  }

  // Format date (day and abbreviated month) and time (24-hour format)
  const date = d.toLocaleDateString('en-IN', { day: 'numeric', month: 'short',year: 'numeric' });
  const time = d.toLocaleTimeString('en-IN', {
    day: 'numeric',
    month: 'short',
    year: 'numeric'
  });
  return `${date}`;
};

const mapStatus = (pnl, rawStatus) => {
  // Handle invalid or missing rawStatus
  if (!rawStatus || typeof rawStatus !== 'string') return '';

  const lowerStatus = rawStatus.toLowerCase();

  // Handle 'active' status
  if (lowerStatus === 'active') return 'Active';

  // Ensure pnl is a number, default to 0 if invalid
  const parsedPnl = typeof pnl === 'number' && !isNaN(pnl) ? pnl : 0;

  // Map based on pnl value
  if (parsedPnl < 0) return 'Expense';
  if (parsedPnl > 0) return 'Revenue';
  if (parsedPnl === 0) return 'Break Even';

  // Default case: return rawStatus capitalized or a fallback
  return rawStatus.charAt(0).toUpperCase() + rawStatus.slice(1);
};

function formatDateTime(dateString) {
  if (!dateString) return '';
  const dateObj = new Date(dateString);
  return dateObj.toLocaleDateString('en-IN', {
    day: 'numeric',
    month: 'short',
    year: 'numeric'
  });
}


function formatNumber(value) {
  if (value === null || value === undefined || value === '') return '';
  return parseFloat(value).toFixed(2);
}

// --- Table Column Definitions ---
const tableColumns = [
  'symbol',
  'candle_time',
  'action',
  'price',
  'stop_loss',
  'target_price',
  'current_price',
  'pnl',
  'status',
  'executed_at'
];

const notificationTableColumns = [
  'action',
  'execution_time',
  'created_at',
  'symbol',
  'message'
];

const historyTableColumns = [
  'symbol',
  'candle_time',
  'action',
  'exit_price',
  'stop_loss',
  'target_price',
  'pnl',
  'status',
  'executed_at'
];

const activeTableColumns = [
  'symbol',
  'action',
  'price',
  'stop_loss',
  'target_price',
  'status'
];

// --- Table Creation Function ---
function createTable(container, data, columns) {
  const table = document.createElement('table');
  table.border = '1';

  const headerRow = document.createElement('tr');
  columns.forEach(col => {
    const th = document.createElement('th');
    // Capitalize header labels
    th.textContent = col
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
    headerRow.appendChild(th);
  });
  table.appendChild(headerRow);

  data.forEach(item => {
    const row = document.createElement('tr');
    columns.forEach(key => {
      const cell = document.createElement('td');
      let cellValue = item[key] !== undefined ? item[key] : '';

      // Format date/time columns
      if (['candle_time', 'executed_at', 'time'].includes(key)) {
        cellValue = formatDateTime(cellValue);
      }
      // Format numeric columns
      else if (['entry_price', 'exit_price', 'price', 'stop_loss', 'target_price', 'pnl', 'current_price'].includes(key)) {
        cellValue = formatNumber(cellValue);
        if (key === 'pnl') {
          const pnlNumber = parseFloat(cellValue);
          cell.style.color = pnlNumber >= 0 ? '#10B981' : '#EF4444';
        }
      }
      // Format status column using the mapStatus function
      else if (key === 'status') {
        const mapped = mapStatus(item.pnl, item.status);
        cellValue = mapped;
        cell.style.color =
          mapped === 'Active'
            ? '#EAB308'
            : mapped === 'Revenue'
              ? '#10B981'
              : mapped === 'Expense'
                ? '#EF4444'
                : '';
      }
      cell.textContent = cellValue;
      row.appendChild(cell);
    });
    table.appendChild(row);
  });
  container.appendChild(table);
}

document.addEventListener('DOMContentLoaded', () => {
  // Variables for trades, history, notifications, and chart
  let combinedTrades = [];
  let combinedTradesforsocket = [];
  let combinedTradesforactive = [];
  let combinedTradesforactivesocket = [];
  let closedTrades = [];
  let allNotifications = [];
  let displayedNotifications = new Set();
  let currentTradeFilter = 'all';
  let currentTradePage = 1;
  const tradesPerPage = 7;
  let currentNotificationsPage = 1;
  const notificationsPerPage = 6;
  let tradePerformanceChart = null;
  let latestHistory = [];
  let offset = 0; // Start from the first row
  const limit = 50; // Number of rows to fetch per request
  let isFetching = false; // Prevent duplicate fetches
  let currentPeriod = '1w';
  let currentExchange = 'nse';
  // let isFetching = false;
  let hasMoreData = true;
  let scrollListenerActive = false;
  let activeTradesInterval = null;
  let lastRenderedActiveTrades = [];
  let for_strt_cap_pnl_sum = 0;



  function isSameTradeList(newList, oldList) {
    if (newList.length !== oldList.length) return false;

    for (let i = 0; i < newList.length; i++) {
      if (newList[i].id !== oldList[i].id || newList[i].pnl !== oldList[i].pnl) {
        return false;
      }
    }

    return true;
  }

  // --- Initialize Chart ---
  const initPerformanceChart = () => {
    if (tradePerformanceChart) return; // Prevent initializing more than once

    const ctx = document.getElementById('tradePerformanceChart').getContext('2d');
    tradePerformanceChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: 'Capital Growth',
          data: [],
          borderColor: '#1DB954',
          backgroundColor: 'rgba(53, 105, 70, 0.09)',
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.parsed.y.toLocaleString()}`
            }
          }
        },
        scales: {
          x: {
            ticks: {
              color: '#fff',
              callback: function (value) {
                const label = this.getLabelForValue(value);
                return label.substring(0, 12);
              }
            }
          },
          y: {
            ticks: {
              color: '#fff',
              callback: (value) => `${(value / 100000).toFixed(1)}L`
            }
          }
        }
      }
    });
  };

  initPerformanceChart();
  // --- Fetch Data Functions ---
  async function fetchAllData(period = '1w', exchangeType = 'nse') {
    try {
      // Update global variables
      currentPeriod = period;
      currentExchange = exchangeType;

      // Fetch metrics data first
      await fetchMetricsData(period, exchangeType);

      // Then fetch trades and history for dealbook
      await fetchTradeAndHistory(period, exchangeType);

      // Finally fetch chart data
      await fetchChartData(period, exchangeType);
    } catch (error) {
      console.error('Error fetching all data:', error);
    }
  }

  async function fetchAllDataforchartmatrics(period = '1w', exchangeType = 'nse') {
    try {
      // Update global variables
      currentPeriod = period;
      currentExchange = exchangeType;

      // Fetch metrics data first
      await fetchMetricsData(period, exchangeType);

      // Then fetch trades and history for dealbook
//      await fetchTradeAndHistory(period, exchangeType);

      // Finally fetch chart data
      await fetchChartData(period, exchangeType);
    } catch (error) {
      console.error('Error fetching all data:', error);
    }
  }

    async function fetchMetricsData(period, exchangeType) {
      try {
        const response = await fetch(
          `/api/trade_and_history_metrics?period=${period}&exchange=${exchangeType}`
        );
        const data = await response.json();

        const exchangeMode = data.current_filter?.exchange || exchangeType;

        /* ---------------- Starting Capital ---------------- */
        const startingCapital = exchangeType === 'binance' ? 0 : data.calc_data.for_strt_cap_pnl_sum;

        const startingCapEl = document.getElementById('starting_capital');
        startingCapEl.textContent = formatCurrency(startingCapital, exchangeMode);
        startingCapEl.style.color = '#FFFFFF';

        /* ---------------- Current Capital ----------------- */
        const currentCapital = exchangeType === 'binance' ? 0 : data.calc_data.for_end_cap_pnl_sum;
        const currentCapEl = document.getElementById('ending_capital');
        currentCapEl.textContent = formatCurrency(currentCapital, exchangeMode);
        currentCapEl.style.color = '#10B981';

        /* ---------------- Net Collection ------------------ */
        const netcollection = data.stats.netcollection;
        const netCollectionEl = document.getElementById('netcollection');

        // Format and display the value
        netCollectionEl.textContent = formatCurrency(netcollection, exchangeMode);

        // Set color based on value
        if (netcollection < 0) {
            netCollectionEl.style.color = '#EF4444';  // Red for negative
        } else {
            netCollectionEl.style.color = '#10B981';  // Green for positive
        }


        /* ------------------ ROI & Other Stats ------------- */
        const roi = data.stats.roi;
        const roiEl = document.getElementById('roiVal');
        roiEl.textContent = `${roi >= 0 ? '+' : ''}${roi}%`;
        roiEl.style.color = roi >= 0 ? '#10B981' : '#EF4444';

        document.getElementById('closeTradesCount').textContent  = data.stats.closed_count;
        document.getElementById('activeTradesCount').textContent = data.stats.active_count;
        // document.getElementById('activeTradesCount').textContent = 8;
        document.getElementById('averagedealtime').textContent   = `${data.stats.avg_deal_time_hours} Hrs`;
      } catch (error) {
        console.error('Error fetching metrics data:', error);
      }
    }


async function fetchTradeAndHistory(period, exchangeType) {
  if (isFetching || !hasMoreData) return;

  isFetching = true;
  try {
    const response = await fetch(`/api/trades_and_history?period=${period}&exchange=${exchangeType}&limit=${limit}&offset=${offset}`);
    const result = await response.json();

    if (result.status === "success") {
      if (result.data.length === 0) {
        hasMoreData = false;
        return;
      }

      const newTrades = result.data;
      if (offset === 0) {
        combinedTrades = newTrades;
        latestHistory = result.history_source || [];
      } else {
        combinedTrades = [...combinedTrades, ...newTrades];
        latestHistory = [...latestHistory, ...(result.history_source || [])];
      }

      // Merge with active trades from WebSocket
      combinedTrades = mergeAndSortTrades(activeTrades, combinedTrades.filter(t => t.status?.toLowerCase() !== 'active'));

      // Update filtered lists
      closedTrades = combinedTrades.filter(t => t.status?.toLowerCase() !== 'active');
      activeTrades = combinedTrades.filter(t => t.status?.toLowerCase() === 'active');
      latestHistory = latestHistory.filter(t => t.status?.toLowerCase() !== 'active').sort((a, b) => new Date(b.executed_at) - new Date(a.executed_at));

      console.log(`Fetched trades: ${newTrades.length}, Active trades: ${activeTrades.length}, Closed trades: ${closedTrades.length}`);

      renderTradeBook();
      offset += limit;
    }
  } catch (error) {
    console.error("Error fetching trades and history:", error);
  } finally {
    isFetching = false;
  }
}


  // --- Setup Scroll Listener ---
  // --- Modified Scroll Handling ---
    function setupScrollListener() {
      if (scrollListenerActive) return;

      const tableContainer = document.querySelector('.table-container');
      tableContainer.addEventListener('scroll', handleScroll);
      scrollListenerActive = true;
    }

  function handleScroll() {
    const dealBookSection = document.querySelector('.deal-book');
    const { scrollTop, scrollHeight, clientHeight } = dealBookSection;

    // console.log("scrollTop:", scrollTop);
    // console.log("clientHeight:", clientHeight);
    // console.log("scrollHeight:", scrollHeight);

    if (scrollTop + clientHeight >= scrollHeight - 100) {
      console.log("Near bottom! Fetching more data...");
      loadMoreTrades();
    }
  }


  // --- Load More Trades Function ---
  async function loadMoreTrades() {
    console.log("loadMoreTrades() called");

    if (isFetching || !hasMoreData) {
      console.log("Already fetching or no more data.");
      return;
    }

    await fetchTradeAndHistory(currentPeriod, currentExchange);
  }


  // --- Initial Load Function ---
  function initialLoad() {
    offset = 0;
    hasMoreData = true;
    fetchTradeAndHistory(currentPeriod, currentExchange)
      .then(() => setupScrollListener()); // Only setup scroll listener after initial load
  }

  async function fetchChartData(period, exchangeType) {
    try {
      const response = await fetch(`/api/trade_and_history_for_chart?period=${period}&exchange=${exchangeType}`);
      const data = await response.json();
      // console.log("Chart data:", data.base_capital);

      if (data.status === "success") {
        updateChart(data.data, parseFloat(data.base_capital)); // Ensure baseCapital is a number
      }
    } catch (error) {
      console.error("Error fetching chart data:", error);
    }
  }

  function updateChart(chartData, baseCapital) {
    const labels = [];
    const dataPoints = [];

    // Add the starting point with base capital
    if (chartData.length > 0) {
      const firstTime = new Date(chartData[0].time);
      labels.push(firstTime.toLocaleString('en-IN', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        // hour12: true
      }));
      dataPoints.push(baseCapital);
    }

    // Accumulate capital changes
    let capital = baseCapital;
    chartData.forEach(item => {
      const pnl = parseFloat(item.pnl_sum); // Ensure pnl_sum is treated as a number
      capital += pnl;

      const date = new Date(item.time);
      labels.push(date.toLocaleString('en-IN', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        // hour12: true
      }));
      dataPoints.push(capital);
    });

    if (tradePerformanceChart) {
      tradePerformanceChart.data.labels = labels;
      tradePerformanceChart.data.datasets[0].data = dataPoints;
      tradePerformanceChart.update();
    }
  }

  // --- Range Button and Exchange Dropdown Event Listeners ---// Handle range buttons (use click)
  document.querySelectorAll('.range-btn').forEach(btn => {
    btn.addEventListener('click', function (e) {
      e.preventDefault();

      document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
      this.classList.add('active');

      currentPeriod = this.dataset.range;

      // ✅ Correct order: Fetch all data in correct sequence
      fetchAllDataforchartmatrics(currentPeriod, currentExchange);  // handles all 3 in order
    });
  });

  // Handle exchange dropdown change (use change)
  document.querySelector('#exchange-type').addEventListener('change', function () {
    currentExchange = this.value;
    localStorage.setItem('selectedExchange', currentExchange);
    window.location.reload(); // Refresh the page on exchange change
  });

  // --- Update fetchTradeAndHistory to show/hide loader ---
  async function fetchTradeAndHistory(period, exchangeType) {
    if (isFetching || !hasMoreData) return;

    isFetching = true;
    // document.getElementById('loadingIndicator').style.display = 'block';

    try {
      const response = await fetch(`/api/trades_and_history?period=${period}&exchange=${exchangeType}&limit=${limit}&offset=${offset}`);
      const result = await response.json();

      if (result.status === "success") {
        if (result.data.length === 0) {
          hasMoreData = false;
          return;
        }

        if (offset === 0) {
          combinedTrades = result.data;
        } else {
          combinedTrades = [...combinedTrades, ...result.data];
        }

        closedTrades = combinedTrades.filter(t => t.status?.toLowerCase() !== 'active');
        combinedTradesforactive = combinedTrades.filter(t => t.status?.toLowerCase() === 'active');

        renderTradeBook();
        offset += limit;
      }
    } catch (error) {
      console.error("Error fetching trades and history:", error);
    } finally {
      isFetching = false;
      // document.getElementById('loadingIndicator').style.display = 'none';
    }
  }
  // --- Trade Book Rendering ---
const renderTradeBook = () => {
  let filtered = combinedTrades;

  // Apply filter
  if (currentTradeFilter === 'active') {
    filtered = activeTrades;
  } else if (currentTradeFilter === 'closed') {
    filtered = closedTrades;
  }

  // Sort by candle_time descending
  filtered = filtered.sort((a, b) => new Date(b.candle_time) - new Date(a.candle_time));

const tbody = document.getElementById('tradeHistoryBody');
tbody.innerHTML = '';
filtered.forEach(trade => {
  const row = document.createElement('tr');
    row.dataset.key = `${trade.symbol}_${trade.candle_time}`;
    row.innerHTML = `
      <td>${formatDate(trade.candle_time)}</td>
      <td>${trade.symbol?.toUpperCase() || ''}</td>
      <td>${transformAction(trade.action?.toUpperCase())}</td>
      <td style="color: ${trade.pnl >= 0 ? '#10B981' : '#EF4444'}">
        ${formatCurrency(trade.pnl, trade.exchange_mode)}
      </td>
      <td style="color: ${trade.status?.toLowerCase() === 'active'
          ? '#EAB308'
          : trade.pnl > 0
            ? '#10B981'
            : '#EF4444'
        }">
        ${mapStatus(trade.pnl, trade.status)}
      </td>
    `;
    tbody.appendChild(row);
  });
};

  // Helper to create a new row
  const createRow = (trade, key) => {
    const row = document.createElement('tr');
    row.dataset.key = key; // Set the key as a data attribute
    updateRow(row, trade);
    return row;
  };

  // Helper to update an existing row
  const updateRow = (row, trade) => {
    row.innerHTML = `
    <td>${formatDate(trade.candle_time)}</td>
    <td>${trade.symbol?.toUpperCase() || ''}</td>
    <td>${transformAction(trade.action?.toUpperCase())}</td>
    <td style="color: ${trade.pnl >= 0 ? '#10B981' : '#EF4444'}">
      ${formatCurrency(trade.pnl, trade.exchange_mode)}
    </td>
    <td style="color: ${trade.status?.toLowerCase() === 'active'
        ? '#EAB308'
        : trade.pnl > 0
          ? '#10B981'
          : '#EF4444'
      }">
      ${mapStatus(trade.pnl, trade.status)}
    </td>
  `;
  };


  // --- Initial Data Load ---
  // Set initial exchange from localStorage if available
  const storedExchange = localStorage.getItem('selectedExchange');
  if (storedExchange) {
    document.getElementById('exchange-type').value = storedExchange;
    currentExchange = storedExchange;
  }

  // Load all data initially
  fetchAllData(currentPeriod, currentExchange);

  // --- WebSocket Setup for Real-Time Updates ---
  // const socket = io('http://192.168.10.101:5000/');
  const socket = io(`${window.location.protocol}//${window.location.hostname}:${window.location.port}`);


  // Transform "BUY"/"SELL" into "Long"/"Short"
  const transformAction = (action) => {
    if (action === 'BUY') return 'Long';
    if (action === 'SELL') return 'Short';
    return action;
  };

  // --- Merge Active + Closed Trades (avoid duplicates)
function mergeAndSortTrades(active, closed) {
  const all = [...active, ...closed];
  const uniqueMap = {};
  const keyCounts = {}; // Track occurrences of each key

  all.forEach((t, index) => {
    // Prefer trade.id if available, otherwise use symbol + timestamp + index
    const baseKey = t.id
      ? t.id
      : `${t.symbol}_${t.executed_at || t.candle_time}`;
    const key = `${baseKey}_${index}`; // Append index to ensure uniqueness

    if (keyCounts[baseKey]) {
      keyCounts[baseKey]++;
       } else {
      keyCounts[baseKey] = 1;
    }

    uniqueMap[key] = t;
  });

  const mergedTrades = Object.values(uniqueMap).sort((a, b) => new Date(b.candle_time) - new Date(a.candle_time));
  console.log(`Merged trades: ${mergedTrades.length}, Expected: ${all.length}`);
  return mergedTrades;
}

  // --- Extract Active Trades from Payload
function extractActiveTrades(tradesArray) {
  return tradesArray.map(t => ({
    time: t.executed_at || t.candle_time,
    executed_at: t.executed_at,
    candle_time: t.candle_time,
    symbol: t.symbol,
    action: transformAction(t.action),
    price: t.entry_price,
    stop_loss: t.stop_loss,
    target_price: t.target_price,
    current_price: t.current_price ?? null,
    pnl: t.pnl,
    status: t.status,
    exchange_mode: t.exchange_mode,
  })).sort((a, b) => new Date(b.time) - new Date(a.time));
}

  // --- Handle WebSocket Payload ---
socket.on('data_update', (payload) => {
  activeTrades = extractActiveTrades(payload.trades || []);
//  console.log(`WebSocket update: ${activeTrades.length} active trades received`);
  combinedTrades = mergeAndSortTrades(activeTrades, closedTrades);
  closedTrades = combinedTrades.filter(t => t.status?.toLowerCase() !== 'active');
  activeTrades = combinedTrades.filter(t => t.status?.toLowerCase() === 'active');
  console.log(`After merge: Active trades: ${activeTrades.length}, Closed trades: ${closedTrades.length}`);
  combinedTradesforactivesocket = activeTrades;
  renderTradeBook();

  // Render Notifications
  if (payload.notifications?.all_notifications) {
    allNotifications = payload.notifications.all_notifications
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
    renderNotifications();
  }

  // Display Latest Notification(s)
  if (payload.notifications?.latest_notification) {
    const latest = Array.isArray(payload.notifications.latest_notification)
      ? payload.notifications.latest_notification
      : [payload.notifications.latest_notification];
    latest.forEach(n => {
      if (!displayedNotifications.has(n.id)) {
        displayNotificationPopup(n);
        displayedNotifications.add(n.id);
      }
    });
  }
});


  const renderNotifications = () => {
    const startIdx = (currentNotificationsPage - 1) * notificationsPerPage;
    const pageData = allNotifications.slice(startIdx, startIdx + notificationsPerPage);
    const list = document.getElementById('notificationsList');
    list.innerHTML = pageData.map(n => `
      <div class="notification-item">
        <div class="notification-header">
          <span>${n.symbol?.toUpperCase() || 'SYSTEM'}</span>
          <small>${formatDateShort(n.created_at)}</small>
        </div>
        <div class="notification-body">
          ${n.message || ''}
          ${n.pnl ? `
            <div class="pnl ${n.pnl >= 0 ? 'positive' : 'negative'}">
              ${formatCurrency(n.pnl, n.exchange_mode)}
            </div>
          ` : ''}
        </div>
      </div>
    `).join('');

    renderPagination(
      'notificationsPagination',
      allNotifications.length,
      notificationsPerPage,
      currentNotificationsPage,
      (page) => {
        currentNotificationsPage = page;
        renderNotifications();
      }
    );
  };

  const displayNotificationPopup = (notification) => {
    const container = document.getElementById('notificationContainer');
    const type = notification.status === 'error' ? 'error'
      : notification.pnl > 0 ? 'success' : 'info';
    const popup = document.createElement('div');
    popup.className = `notification-popup ${type}`;
    popup.innerHTML = `
      <div class="notification-header">
        <strong>${notification.symbol?.toUpperCase() || 'ALERT'}</strong>
        <button class="close-btn">&times;</button>
      </div>
      <div class="notification-content">
        <p>${notification.message || ''}</p>
        ${notification.pnl ? `
          <div class="pnl-display ${notification.pnl >= 0 ? 'positive' : 'negative'}">
            ${formatCurrency(notification.pnl, notification.exchange_mode)}
          </div>
        ` : ''}
      </div>
    `;
    popup.querySelector('.close-btn').addEventListener('click', () => {
      popup.remove();
      displayedNotifications.delete(notification.id);
    });
    container.prepend(popup);
    setTimeout(() => popup.remove(), 10000);
  };

  const renderPagination = (containerId, totalItems, perPage, currentPage, callback) => {
    const container = document.getElementById(containerId);
    if (!container) return;
    const totalPages = Math.ceil(totalItems / perPage);
    if (totalPages <= 1) {
      container.innerHTML = '';
      return;
    }
    const showEllipsis = totalPages > 3 && currentPage > 2 && currentPage < totalPages - 1;
    const pages = new Set([1, currentPage, totalPages]);
    if (currentPage - 1 > 1) pages.add(currentPage - 1);
    if (currentPage + 1 < totalPages) pages.add(currentPage + 1);
    container.innerHTML = `
      <div class="pagination-group">
        <button class="page-btn prev" ${currentPage === 1 ? 'disabled' : ''}>←</button>
        ${Array.from(pages)
        .sort((a, b) => a - b)
        .map((page, index, arr) => {
          const showRightEllipsis = showEllipsis && page === 1 && arr[1] - page > 1;
          const showLeftEllipsis = showEllipsis && page === totalPages && totalPages - arr[arr.length - 2] > 1;
          return `
              ${showRightEllipsis ? '<span class="page-btn ellipsis">...</span>' : ''}
              <button class="page-btn ${page === currentPage ? 'active' : ''}"
                data-page="${page}">${page}</button>
              ${showLeftEllipsis ? '<span class="page-btn ellipsis">...</span>' : ''}
            `;
        }).join('')}
        <button class="page-btn next" ${currentPage === totalPages ? 'disabled' : ''}>→</button>
      </div>
    `;
    container.querySelectorAll('button:not(.ellipsis)').forEach(btn => {
      btn.addEventListener('click', () => {
        let newPage = currentPage;
        if (btn.classList.contains('prev')) {
          newPage = Math.max(1, currentPage - 1);
        } else if (btn.classList.contains('next')) {
          newPage = Math.min(totalPages, currentPage + 1);
        } else {
          newPage = Number(btn.dataset.page);
        }
        callback(newPage);
      });
    });
  };

  // --- Event Listeners for Filtering & Modal ---
  const setActiveBtn = (id) => {
    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
    document.getElementById(id).classList.add('active');
  };

document.getElementById('filterAllBtn').addEventListener('click', () => {
  currentTradeFilter = 'all';
  currentTradePage = 1;
  if (activeTradesInterval) clearInterval(activeTradesInterval);
  renderTradeBook();
  setActiveBtn('filterAllBtn');
});

document.getElementById('filterActiveBtn').addEventListener('click', () => {
  currentTradeFilter = 'active';
  currentTradePage = 1;
  if (activeTradesInterval) clearInterval(activeTradesInterval);
  renderTradeBook();
  activeTradesInterval = setInterval(() => {
    renderTradeBook();
  }, 2000);
  setActiveBtn('filterActiveBtn');
});

document.getElementById('filterClosedBtn').addEventListener('click', () => {
  currentTradeFilter = 'closed';
  currentTradePage = 1;
  if (activeTradesInterval) clearInterval(activeTradesInterval);
  renderTradeBook();
  setActiveBtn('filterClosedBtn');
});


  document.getElementById('filterClosedBtn').addEventListener('click', () => {
    currentTradeFilter = 'closed';
    currentTradePage = 1;

    if (activeTradesInterval) clearInterval(activeTradesInterval); // stop live update

    renderTradeBook(); // from API
    setActiveBtn('filterClosedBtn');
  });

  document.getElementById('clearNotificationsBtn').addEventListener('click', () => {
    document.getElementById('notificationContainer').innerHTML = '';
    displayedNotifications.clear();
  });

// --- Modal for Popup Data ---
  const popupModal = document.getElementById('popupModal');
  const modalClose = document.getElementById('modalClose');
  const popupContent = document.getElementById('popupContent');

  modalClose.onclick = () => {
    popupModal.style.display = "none";
  };
  window.onclick = (event) => {
    if (event.target === popupModal) {
      popupModal.style.display = "none";
    }
  };

  document.getElementById('historyBtn').addEventListener('click', (event) => {
    event.preventDefault();
    fetch(`/api/trades_and_historyforsidebar?exchange=${currentExchange}&period=${currentPeriod}`)
      .then(response => response.json())
      .then(result => {
        let closedTradesforhistory = result.data.filter(trade =>
          typeof trade.pnl === 'number' && !isNaN(trade.pnl)
        );
        renderPopupData(closedTradesforhistory, 'history');
      })
      .catch(error => console.error("Error fetching history data:", error));
  });

  // Button to view active trades in a modal
  document.getElementById('activeBtn').addEventListener('click', (event) => {
    event.preventDefault();
    const inQueueTrades = combinedTradesforactivesocket.filter(trade => trade.status.toLowerCase() === 'active');
    renderPopupData(inQueueTrades, 'inQueue');
  });

function renderPopupData(data, type) {

  if (type === 'inQueue') {
    // Create a new window for Deal Queue
    const dealQueueWindow = window.open('', 'DealQueueWindow', 'width=800,height=1000,scrollbars=yes,resizable=yes');

    // Define the HTML content for the new window
    let windowHTML = `
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Deal Queue</title>
        <style>
        body {
            background-color: #121212;
            color: #e0e0e0;
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
          }
        .trade-card {
          background: linear-gradient(145deg, #1e1e1e, #262626);
          border-radius: 12px;
          padding: 14px;
          margin-bottom: 12px;
          transition: all 0.3s ease;
          overflow: hidden;
          box-sizing: border-box;
          text-align: center;
        }
        .trade-card:hover {
          transform: translateY(-4px);
          box-shadow: 0 6px 16px rgba(0, 0, 0, 0.5);
        }
        .feed-container {
            text-align: center;
            text-align: center;

        }
        .trade-header {
          display: flex;
          align-items: center;
          margin-bottom: 12px;
          font-size: 1em;
          font-weight: bold;
          text-transform: capitalize;
        }
        .trade-type {
          font-weight: 600;
          font-size: 0.9em;
          padding: 6px 12px;
          border-radius: 20px;
          text-transform: capitalize;
          letter-spacing: 0.5px;
        }
        .trade-type.buy {
          background-color: #1b5e20; /* Green for LONG */
          color: #fff;
        }
        .trade-type.sell {
          background-color: #b71c1c; /* Red for SHORT */
          color: #fff;
        }
        .trade-status {
          font-size: 0.85em;
          color: #fff;
          background:rgba(218, 171, 33, 0.7); /* Yellow for Pending */
          padding: 4px 10px;
          border-radius: 20px;
          font-weight: bold;
          margin-left: 10px;
        }
        .trade-pair {
          font-size: 0.85em;
          color: #fff;
          background: #333333;
          padding: 4px 10px;
          border-radius: 6px;
          font-weight: bold;
          margin-left: 300px;
        }
        .trade-details {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
          gap: 12px;
          font-size: 1em;
          font-weight: bold;
          text-transform: capitalize;
          justify-items: center; /* Center grid items */
        }
        .trade-details div {
          background-color: #2a2a2a;
          padding: 8px;
          border-radius: 8px;
          text-align: center;
          transition: background-color 0.2s;
          width: 100%; /* Ensure full width for centering */
          box-sizing: border-box;
        }
        .trade-details .entry-exit {
          font-weight: 700;
          font-size: 0.95em;
          color: #ffffff;
        }
        .trade-details div:hover {
          background-color: #3a3a3a;
        }
        h2 {
          color: #e0e0e0;
          font-weight: 500;
          text-align: center;
          margin-bottom: 20px;
        }
    /* Media query for larger screens */
    @media (min-width: 1200px) {
      .trade-card {
        margin-right: 300px;
        margin-left: 300px;
      }
    .trade-pair {
      font-size: 0.85em;
      color: #fff;
      background: #333333;
      padding: 4px 10px;
      border-radius: 6px;
      font-weight: bold;
      margin-left: 870px;
    }
    }
      </style>
      <div class="feed-container">
        <h2>Deal Queue</h2>
    `;

    // Process inQueue trades
    data.forEach(trade => {
      // Extract fields from the trade
      let action = trade.action !== undefined ? String(trade.action).toUpperCase() : '';
      let symbol = trade.symbol !== undefined ? String(trade.symbol).toUpperCase() : '';
      let entryPrice = trade.entry_price !== undefined ? trade.entry_price : trade.price !== undefined ? trade.price : '';
      let executedAt = trade.executed_at !== undefined ? trade.executed_at : '';
      let stopLoss = trade.stop_loss !== undefined ? trade.stop_loss : '';
      let targetPrice = trade.target_price !== undefined ? trade.target_price : '';

      // Format action and trade type class
      let displayAction = action;
      let tradeTypeClass = '';
      if (action === 'BUY' || action === 'LONG') {
        displayAction = 'Long';
        tradeTypeClass = 'buy';
      } else if (action === 'SELL' || action === 'SHORT') {
        displayAction = 'Short';
        tradeTypeClass = 'sell';
      } else {
        tradeTypeClass = 'initiated'; // Fallback
      }

      // Round numerical values to 2 decimal places
      const formatNumber = (value) => {
        if (value === '' || value === undefined || value === null) return 'N/A';
        const num = parseFloat(value);
        return isNaN(num) ? 'N/A' : num.toFixed(2);
      };
      entryPrice = formatNumber(entryPrice);
      stopLoss = formatNumber(stopLoss);
      targetPrice = formatNumber(targetPrice);

      // Format symbol to replace underscore with a slash
      const formattedSymbol = symbol ? symbol.replace('_', ' | ') : 'UNKNOWN_PAIR';

      // Determine card class
      const cardClass = (action === 'BUY' || action === 'SELL' || action === 'LONG' || action === 'SHORT') ? 'trade-card initiated' : 'trade-card';

      // Build trade card HTML
      windowHTML += `
        <div class="${cardClass}">
          <div class="trade-header">
            <span class="trade-type ${tradeTypeClass}">${displayAction || 'TRADE'}</span>
            <span class="trade-status">In-Queue</span>
            <span class="trade-pair">${formattedSymbol}</span>
          </div>
          <div class="trade-details">
            ${entryPrice !== 'N/A' ? `<div class="entry-exit">Entry: ${entryPrice}</div>` : ''}
            ${stopLoss !== 'N/A' ? `<div>Stop Loss: ${stopLoss}</div>` : ''}
            ${targetPrice !== 'N/A' ? `<div>Target: ${targetPrice}</div>` : ''}
          </div>
        </div>
      `;
    });

    windowHTML += `
        </div>
      </body>
      </html>
    `;

    // Write the HTML to the new window
    dealQueueWindow.document.write(windowHTML);
    dealQueueWindow.document.close();
  } else {
    let columnsToUse = tableColumns;
    if (type === 'history') {
      columnsToUse = historyTableColumns;
    } else if (type === 'notifications') {
      columnsToUse = notificationTableColumns;
    } else if (type === 'inQueue') {
      columnsToUse = activeTableColumns;
    }
    createTable(popupContent, data, columnsToUse);
    popupModal.style.display = "block";
  }
}
// Button to open "Recent Notifications" in a popup window
  document.getElementById('notificationsBtn').addEventListener('click', (event) => {
    event.preventDefault();
    if (allNotifications && Array.isArray(allNotifications) && allNotifications.length > 0) {
        const popupWindow = window.open('', 'Notifications', 'width=700,height=1000,scrollbars=yes');
        if (popupWindow) {
            let popupHTML = `
                <html>
                <head>
                    <title>Notifications</title>
                    <style>
                        body {
                            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                            background-color: #121212;
                            color: #e0e0e0;
                            margin: 0;
                            padding: 20px;
                            display: flex;
                            justify-content: center;
                            line-height: 1.5;
                        }
                        .feed-container {
                            max-width: 700px;
                            width: 100%;
                            font-waight:bold;
                        }
                        .trade-card {
                            background: linear-gradient(145deg, #1e1e1e, #262626);
                            border-radius: 12px;
                            padding: 14px;
                            margin-bottom: 12px;
                            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
                            transition: all 0.3s ease;
                            position: relative;
                            overflow: hidden;
                        }
                        .trade-card:hover {
                            transform: translateY(-4px);
                            box-shadow: 0 6px 16px rgba(0, 0, 0, 0.5);
                        }
                        .trade-card.initiated::before {
                            content: '';
                            position: absolute;
                            top: 0;
                            left: 0;
                            width: 4px;
                            height: 100%;
                            background: #ffca28;
                        }
                        .trade-header {
                            display: flex;
                            justify-content: space-between;
                            align-items: center;
                            margin-bottom: 12px;
                            font-size: 1em;
                            font-weight: bold;
                            text-transform: Camelcase;
                        }
                        .trade-type {
                            font-weight: 600;
                            font-size: 0.9em;
                            padding: 6px 12px;
                            border-radius: 20px;
                            text-transform: Camelcase;
                            letter-spacing: 0.5px;
                        }
                        .trade-type.buy {
                            background-color:#1b5e20;
                            color: #fff;
                            text-transform: Camelcase;
                        }
                        .trade-type.sell {
                            background-color:#b71c1c;
                            color: #fff;
                            text-transform: Camelcase;
                        }
                        .trade-type.target {
                            background-color:rgba(27, 94, 31, 0.12);
                            color: #a5d6a7;
                        }
                        .trade-type.stop-loss {
                            background-color:rgba(183, 28, 28, 0.11);
                            color: #ef9a9a;
                        }
                        .trade-pair {
                            font-size: 0.85em;
                            color: #fff;
                            background: #333333;
                            padding: 4px 10px;
                            border-radius: 6px;
                            font-weight: bold;
                            text-transform: Camelcase;

                        }
                        .trade-details {
                            display: grid;
                            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
                            gap: 12px;
                            font-size: 1em;
                            font-weight: bold;
                            text-transform: Camelcase;
                        }
                        .trade-details div {
                            background-color: #2a2a2a;
                            padding: 8px;
                            border-radius: 8px;
                            text-align: center;
                            transition: background-color 0.2s;
                        }
                        .trade-details .entry-exit {
                            font-weight: 700;
                            font-size: 0.95em;
                            color: #ffffff;
                        }
                        .trade-details div:hover {
                            background-color: #3a3a3a;
                        }
                        .pnl {
                            font-weight: 600;
                            padding: 10px;
                            border-radius: 8px;
                        }
                        .pnl.positive {
                            background-color: rgba(27, 94, 31, 0.12);
                            color: #a5d6a7;
                        }
                        .pnl.negative {
                            background-color: rgba(183, 28, 28, 0.11);
                            color: #ef9a9a;
                        }
                        .timestamp {
                            font-size: 0.85em;
                            font-weight: 600;
                            color:rgba(255, 255, 255, 0.6);
                            margin-top: 16px;
                            text-align: right;
                            font-style: normal;
                            letter-spacing: 0.3px;
                        }
                        h2 {
                            color: #e0e0e0;
                            font-weight: 500;
                            text-align: center;
                            margin-bottom: 20px;
                        }
                        @media (max-width: 500px) {
                            .trade-details {
                                grid-template-columns: 1fr;
                            }
                            .trade-card {
                                padding: 15px;
                            }
                        }
                    </style>
                </head>
                <body>
                    <div class="feed-container">
                        <h2>Recent Notifications</h2>
            `;

            // Process notifications
            allNotifications.forEach(n => {
              // Extract fields from the notification
              let action = n.action !== undefined ? String(n.action).toUpperCase() : '';
              let symbol = n.symbol !== undefined ? String(n.symbol).toUpperCase() : '';
              let entryPrice = n.price !== undefined ? n.price : '';
              let createdAt = n.created_at !== undefined ? n.created_at : '';
              let message = n.message !== undefined ? String(n.message) : '';
              let stopLoss = n.stop_loss !== undefined ? n.stop_loss : '';
              let targetPrice = n.target_price !== undefined ? n.target_price : '';

              // Parse message for additional fields
              let exitPrice = '';
              let pnl = '';
              let status = '';

              if (message.startsWith('Order Closed:')) {
                  // Format message as per original logic
                  message = message.replace(/\n/g, ' ');
                  message = message
                      .replace(/Entry Price:/g, '| Entry Price:')
                      .replace(/Exit Price:/g, '| Exit Price:')
                      .replace(/PnL:/g, '| PnL:')
                      .replace(/Status:/g, '| Status:');
                  const parts = message.split('|').map(part => part.trim());

                  // Extract fields from message
                  parts.forEach(part => {
                      if (part.startsWith('Entry Price:')) {
                          entryPrice = part.replace('Entry Price:', '').trim();
                      } else if (part.startsWith('Exit Price:')) {
                          exitPrice = part.replace('Exit Price:', '').trim();
                      } else if (part.startsWith('PnL:')) {
                          pnl = part.replace('PnL:', '').trim();
                      } else if (part.startsWith('Status:')) {
                          status = part.replace('Status:', '').trim();
                      }
                  });
              }

              // Determine if trade is initiated (BUY or SELL) or completed (target_hit or stop_loss_hit)
              const isInitiated = action === 'BUY' || action === 'SELL';

              // Format action and banner text
              let displayAction = action;
              let tradeTypeClass = '';
              if (action === 'TARGET_HIT') {
                  displayAction = 'Target Achieved';
                  tradeTypeClass = 'target';
              } else if (action === 'STOP_LOSS_HIT') {
                  displayAction = 'Stoploss Hit';
                  tradeTypeClass = 'stop-loss';
              } else if (action === 'BUY') {
                  displayAction = 'Long';
                  tradeTypeClass = 'Buy';
              } else if (action === 'SELL') {
                displayAction = 'Short';
                  tradeTypeClass = 'Sell';
              } else {
                  tradeTypeClass = 'initiated'; // Fallback
              }

              // Format timestamp (ensure 24-hour format, use created_at as-is since it's already in desired format)
              const timestamp = createdAt || 'N/A';

              const formatDate = (date) => {
                const options = {
                  weekday: 'long',
                  month: 'long',
                  day: 'numeric',
                  hour: 'numeric',
                  minute: 'numeric',
                  second: 'numeric',
                  hour12: false
                };
                return new Date(date).toLocaleString('en-US', options);
              };

              const formattedTimestamp = formatDate(timestamp);

              // Round numerical values to 2 decimal places
              const formatNumber = (value) => {
                  const num = parseFloat(value);
                  return isNaN(num) ? value : num.toFixed(2);
              };
              entryPrice = entryPrice ? formatNumber(entryPrice) : '';
              exitPrice = exitPrice ? formatNumber(exitPrice) : '';
              pnl = pnl ? formatNumber(pnl) : '';
              stopLoss = stopLoss ? formatNumber(stopLoss) : '';
              targetPrice = targetPrice ? formatNumber(targetPrice) : '';

            // Format symbol to replace underscore with a slash
            const formattedSymbol = symbol ? symbol.replace('_', ' | ') : 'UNKNOWN_PAIR';

            // Determine card class
            const cardClass = isInitiated ? 'trade-card initiated' : 'trade-card';

            // Build trade card HTML
            popupHTML += `
                <div class="${cardClass}">
                    <div class="trade-header">
                        <span class="trade-type ${tradeTypeClass}">${displayAction || 'TRADE'}</span>
                        <span class="trade-pair">${formattedSymbol}</span>
                    </div>
                    <div class="trade-details">
                        ${entryPrice ? `<div class="entry-exit">Entry: ${entryPrice}</div>` : ''}
                        ${isInitiated && stopLoss ? `<div>Stop Loss: ${stopLoss}</div>` : ''}
                        ${isInitiated && targetPrice ? `<div>Target: ${targetPrice}</div>` : ''}
                        ${!isInitiated && exitPrice ? `<div class="entry-exit">Exit: ${exitPrice}</div>` : ''}
                        ${!isInitiated && pnl ? `<div class="pnl ${pnl.startsWith('-') ? 'negative' : 'positive'}">P/L : ${pnl}</div>` : ''}
                    </div>
                    <div class="timestamp">${formattedTimestamp}</div>
                </div>
            `;

          });

          popupHTML += `
                  </div>
              </body>
              </html>
          `;

          popupWindow.document.open();
          popupWindow.document.write(popupHTML);
          popupWindow.document.close();
      } else {
          alert("Popup blocked. Please allow popups for this site.");
      }
  } else {
      alert("No notifications available");
  }
});
  initialLoad();
});


function updateTime() {
  const liveTimeElement = document.getElementById('liveTime');
  if (liveTimeElement) {
    const now = new Date();
    const options = {
      timeZone: 'Asia/Kolkata',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    };
    liveTimeElement.textContent = now.toLocaleString('en-IN', options);
  }
}
setInterval(updateTime, 1000);
updateTime();

