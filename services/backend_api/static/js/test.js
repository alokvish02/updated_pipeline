// --- Utility Functions ---
const formatCurrency = (amount, exchange_mode) => {
  const val = Number(amount) || 0;
  if (exchange_mode && typeof exchange_mode === 'string') {
    const mode = exchange_mode.toLowerCase();
    if (mode === 'binance') {
      return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(val);
    } else if (mode === 'nse') {
      return new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(val);
    }
  }
  return val.toFixed(2);
};

const randomSecondsCache = {};
const formatDateShort = (dateString) => {
  const d = new Date(dateString);
  if (isNaN(d)) return '';

  const timePart = dateString.split(' ')[1] || '';
  const timeComponents = timePart.split(':');
  if (timeComponents.length < 3) {
    if (!(dateString in randomSecondsCache)) {
      randomSecondsCache[dateString] = Math.floor(Math.random() * 60);
    }
    d.setSeconds(randomSecondsCache[dateString]);
  }

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
  if (isNaN(d)) return '';

  const timePart = dateString.split(' ')[1] || '';
  const timeComponents = timePart.split(':');
  if (timeComponents.length < 3) {
    if (!(dateString in randomSecondsCache)) {
      randomSecondsCache[dateString] = Math.floor(Math.random() * 60);
    }
    d.setSeconds(randomSecondsCache[dateString]);
  }

  const date = d.toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' });
  return `${date}`;
};

const mapStatus = (pnl, rawStatus) => {
  if (!rawStatus || typeof rawStatus !== 'string') return '';

  const lowerStatus = rawStatus.toLowerCase();
  if (lowerStatus === 'active') return 'Active';

  const parsedPnl = typeof pnl === 'number' && !isNaN(pnl) ? pnl : 0;

  if (parsedPnl < 0) return 'Expense';
  if (parsedPnl > 0) return 'Revenue';
  if (parsedPnl === 0) return 'Break Even';

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
  'symbol', 'candle_time', 'action', 'price', 'stop_loss', 'target_price', 'current_price', 'pnl', 'status', 'executed_at'
];

const notificationTableColumns = [
  'action', 'execution_time', 'created_at', 'symbol', 'message'
];

const historyTableColumns = [
  'symbol', 'candle_time', 'action', 'exit_price', 'stop_loss', 'target_price', 'pnl', 'status', 'executed_at'
];

const activeTableColumns = [
  'symbol', 'action', 'price', 'stop_loss', 'target_price', 'status'
];

// --- Table Creation Function ---
function createTable(container, data, columns) {
  const table = document.createElement('table');
  table.border = '1';

  const headerRow = document.createElement('tr');
  columns.forEach(col => {
    const th = document.createElement('th');
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

      if (['candle_time', 'executed_at', 'time'].includes(key)) {
        cellValue = formatDateTime(cellValue);
      }
      else if (['entry_price', 'exit_price', 'price', 'stop_loss', 'target_price', 'pnl', 'current_price'].includes(key)) {
        cellValue = formatNumber(cellValue);
        if (key === 'pnl') {
          const pnlNumber = parseFloat(cellValue);
          cell.style.color = pnlNumber >= 0 ? '#10B981' : '#EF4444';
        }
      }
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
  // Variables
  let combinedTrades = [];
  let activeTrades = [];
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
  let offset = 0;
  const limit = 50;
  let isFetching = false;
  let currentPeriod = '1w';
  let currentExchange = 'nse';
  let hasMoreData = true;
  let scrollListenerActive = false;
  let activeTradesInterval = null;
  let lastRenderedActiveTrades = [];
  let for_strt_cap_pnl_sum = 0;
  let notificationsEnabled = true;

  // Unique trade tracking using Map for better performance
  const tradeMap = new Map();
  const lastNotificationId = { value: null };

  function generateTradeKey(trade) {
    // Create unique key for trade
    return trade.id || `${trade.symbol}_${trade.candle_time}_${trade.action}`;
  }

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
    if (tradePerformanceChart) return;

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
      currentPeriod = period;
      currentExchange = exchangeType;

      await fetchMetricsData(period, exchangeType);
      await fetchTradeAndHistory(period, exchangeType);
      await fetchChartData(period, exchangeType);
    } catch (error) {
      console.error('Error fetching all data:', error);
    }
  }

  async function fetchAllDataforchartmatrics(period = '1w', exchangeType = 'nse') {
    try {
      currentPeriod = period;
      currentExchange = exchangeType;

      await fetchMetricsData(period, exchangeType);
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

      const startingCapital = exchangeType === 'binance' ? 0 : data.calc_data.for_strt_cap_pnl_sum;
      const startingCapEl = document.getElementById('starting_capital');
      startingCapEl.textContent = formatCurrency(startingCapital, exchangeMode);
      startingCapEl.style.color = '#FFFFFF';

      const currentCapital = exchangeType === 'binance' ? 0 : data.calc_data.for_end_cap_pnl_sum;
      const currentCapEl = document.getElementById('ending_capital');
      currentCapEl.textContent = formatCurrency(currentCapital, exchangeMode);
      currentCapEl.style.color = '#10B981';

      const netcollection = data.stats.netcollection;
      const netCollectionEl = document.getElementById('netcollection');
      netCollectionEl.textContent = formatCurrency(netcollection, exchangeMode);

      if (netcollection < 0) {
        netCollectionEl.style.color = '#EF4444';
      } else {
        netCollectionEl.style.color = '#10B981';
      }

      const roi = data.stats.roi;
      const roiEl = document.getElementById('roiVal');
      roiEl.textContent = `${roi >= 0 ? '+' : ''}${roi}%`;
      roiEl.style.color = roi >= 0 ? '#10B981' : '#EF4444';

      document.getElementById('closeTradesCount').textContent = data.stats.closed_count;
      document.getElementById('activeTradesCount').textContent = data.stats.active_count;
      document.getElementById('averagedealtime').textContent = `${data.stats.avg_deal_time_hours} Hrs`;
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

        const newTrades = result.data.filter(trade => trade.exchange_mode === exchangeType);

        if (offset === 0) {
          // Clear existing data for fresh load
          tradeMap.clear();
          combinedTrades = [];
          latestHistory = [];
        }

        // Add new trades to map to avoid duplicates
        newTrades.forEach(trade => {
          const key = generateTradeKey(trade);
          if (!tradeMap.has(key)) {
            tradeMap.set(key, trade);
            combinedTrades.push(trade);
          }
        });

        // Update history
        if (result.history_source) {
          const newHistory = result.history_source.filter(trade => trade.exchange_mode === exchangeType);
          newHistory.forEach(trade => {
            const key = generateTradeKey(trade);
            if (!tradeMap.has(key)) {
              tradeMap.set(key, trade);
              latestHistory.push(trade);
            }
          });
        }

        // Separate active and closed trades
        activeTrades = combinedTrades.filter(t => t.status?.toLowerCase() === 'active');
        closedTrades = combinedTrades.filter(t => t.status?.toLowerCase() !== 'active');

        console.log(`Fetched trades for ${exchangeType}: Active: ${activeTrades.length}, Closed: ${closedTrades.length}`);

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
  function setupScrollListener() {
    if (scrollListenerActive) return;

    const tableContainer = document.querySelector('.table-container');
    if (tableContainer) {
      tableContainer.addEventListener('scroll', handleScroll);
      scrollListenerActive = true;
    }
  }

  function handleScroll() {
    const dealBookSection = document.querySelector('.deal-book');
    if (!dealBookSection) return;

    const { scrollTop, scrollHeight, clientHeight } = dealBookSection;

    if (scrollTop + clientHeight >= scrollHeight - 100) {
      console.log("Near bottom! Fetching more data...");
      loadMoreTrades();
    }
  }

  async function loadMoreTrades() {
    console.log("loadMoreTrades() called");

    if (isFetching || !hasMoreData) {
      console.log("Already fetching or no more data.");
      return;
    }

    await fetchTradeAndHistory(currentPeriod, currentExchange);
  }

  function initialLoad() {
    offset = 0;
    hasMoreData = true;
    tradeMap.clear();
    fetchTradeAndHistory(currentPeriod, currentExchange)
      .then(() => setupScrollListener());
  }

  async function fetchChartData(period, exchangeType) {
    try {
      const response = await fetch(`/api/trade_and_history_for_chart?period=${period}&exchange=${exchangeType}`);
      const data = await response.json();

      if (data.status === "success") {
        updateChart(data.data, parseFloat(data.base_capital));
      }
    } catch (error) {
      console.error("Error fetching chart data:", error);
    }
  }

  function updateChart(chartData, baseCapital) {
    const labels = [];
    const dataPoints = [];

    if (chartData.length > 0) {
      const firstTime = new Date(chartData[0].time);
      labels.push(firstTime.toLocaleString('en-IN', {
        month: 'short',
        day: 'numeric',
        year: 'numeric'
      }));
      dataPoints.push(baseCapital);
    }

    let capital = baseCapital;
    chartData.forEach(item => {
      const pnl = parseFloat(item.pnl_sum);
      capital += pnl;

      const date = new Date(item.time);
      labels.push(date.toLocaleString('en-IN', {
        month: 'short',
        day: 'numeric',
        year: 'numeric'
      }));
      dataPoints.push(capital);
    });

    if (tradePerformanceChart) {
      tradePerformanceChart.data.labels = labels;
      tradePerformanceChart.data.datasets[0].data = dataPoints;
      tradePerformanceChart.update();
    }
  }

  // --- Range Button and Exchange Dropdown Event Listeners ---
  document.querySelectorAll('.range-btn').forEach(btn => {
    btn.addEventListener('click', function (e) {
      e.preventDefault();

      document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
      this.classList.add('active');

      currentPeriod = this.dataset.range;
      fetchAllDataforchartmatrics(currentPeriod, currentExchange);
    });
  });

  document.querySelector('#exchange-type').addEventListener('change', function () {
    currentExchange = this.value;
    localStorage.setItem('selectedExchange', currentExchange);
    window.location.reload();
  });

  // --- Trade Book Rendering ---
  const renderTradeBook = () => {
    let filtered = combinedTrades;

    if (currentTradeFilter === 'active') {
      filtered = activeTrades;
    } else if (currentTradeFilter === 'closed') {
      filtered = closedTrades;
    }

    // Remove duplicates and sort
    const uniqueFiltered = [];
    const seenKeys = new Set();
    
    filtered.forEach(trade => {
      const key = generateTradeKey(trade);
      if (!seenKeys.has(key)) {
        seenKeys.add(key);
        uniqueFiltered.push(trade);
      }
    });

    const sortedFiltered = uniqueFiltered.sort((a, b) => new Date(b.candle_time) - new Date(a.candle_time));

    const tbody = document.getElementById('tradeHistoryBody');
    if (!tbody) return;

    tbody.innerHTML = '';
    sortedFiltered.forEach(trade => {
      const row = document.createElement('tr');
      row.dataset.key = generateTradeKey(trade);
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

  // --- Set initial exchange from localStorage ---
  const storedExchange = localStorage.getItem('selectedExchange');
  if (storedExchange) {
    document.getElementById('exchange-type').value = storedExchange;
    currentExchange = storedExchange;
  }

  fetchAllData(currentPeriod, currentExchange);

  // --- WebSocket Setup for Real-Time Updates ---
  const socket = io(`${window.location.protocol}//${window.location.hostname}:${window.location.port}`);

  const transformAction = (action) => {
    if (action === 'BUY') return 'Long';
    if (action === 'SELL') return 'Short';
    return action;
  };

  // --- Extract Active Trades from WebSocket Payload ---
  function extractActiveTrades(tradesArray) {
    const currentExchangeFilter = currentExchange.toLowerCase();
    
    return tradesArray
      .filter(t => t.exchange_mode && t.exchange_mode.toLowerCase() === currentExchangeFilter)
      .map(t => ({
        id: t.id,
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
      }))
      .sort((a, b) => new Date(b.time) - new Date(a.time));
  }

  // --- Handle WebSocket Payload ---
  socket.on('data_update', (payload) => {
    console.log('WebSocket data received:', payload);

    // Only process trades if they exist and are for current exchange
    if (payload.trades && Array.isArray(payload.trades)) {
      const newActiveTrades = extractActiveTrades(payload.trades);
      
      // Update active trades without duplicates
      const activeTradeKeys = new Set();
      const uniqueActiveTrades = [];
      
      newActiveTrades.forEach(trade => {
        const key = generateTradeKey(trade);
        if (!activeTradeKeys.has(key)) {
          activeTradeKeys.add(key);
          uniqueActiveTrades.push(trade);
          
          // Update in main trade map
          tradeMap.set(key, trade);
        }
      });

      // Update activeTrades array
      activeTrades = uniqueActiveTrades;
      
      // Rebuild combinedTrades from map
      combinedTrades = Array.from(tradeMap.values())
        .filter(t => t.exchange_mode && t.exchange_mode.toLowerCase() === currentExchange.toLowerCase());
      
      closedTrades = combinedTrades.filter(t => t.status?.toLowerCase() !== 'active');

      console.log(`WebSocket: ${uniqueActiveTrades.length} active trades for ${currentExchange}`);
      
      // Re-render trade book only if viewing active or all trades
      if (currentTradeFilter === 'active' || currentTradeFilter === 'all') {
        renderTradeBook();
      }
    }

    // Handle Notifications
    if (payload.notifications) {
      const notifications = payload.notifications;
      
      // Update all notifications list
      if (notifications.all_notifications && Array.isArray(notifications.all_notifications)) {
        allNotifications = notifications.all_notifications
          .filter(n => n.exchange_mode && n.exchange_mode.toLowerCase() === currentExchange.toLowerCase())
          .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        
        console.log(`Updated notifications for ${currentExchange}:`, allNotifications.length);
        renderNotifications();
      }

      // Handle latest notification popup
      if (notifications.latest_notification && notificationsEnabled) {
        const latest = notifications.latest_notification;
        
        // Check if this is a new notification and for current exchange
        if (latest.id && 
            latest.exchange_mode && 
            latest.exchange_mode.toLowerCase() === currentExchange.toLowerCase() &&
            lastNotificationId.value !== latest.id) {
          
          lastNotificationId.value = latest.id;
          
          if (!displayedNotifications.has(latest.id)) {
            displayNotificationPopup(latest);
            displayedNotifications.add(latest.id);
            console.log('Showing new notification popup:', latest.message);
          }
        }
      }
    }
  });

  const renderNotifications = () => {
    const startIdx = (currentNotificationsPage - 1) * notificationsPerPage;
    const pageData = allNotifications.slice(startIdx, startIdx + notificationsPerPage);
    const list = document.getElementById('notificationsList');
    
    if (!list) return;
    
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
    if (!notificationsEnabled) return;
    
    const container = document.getElementById('notificationContainer');
    if (!container) {
      // Create notification container if it doesn't exist
      const newContainer = document.createElement('div');
      newContainer.id = 'notificationContainer';
      newContainer.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        z-index: 10000;
        max-width: 350px;
      `;
      document.body.appendChild(newContainer);
    }
    
    const finalContainer = document.getElementById('notificationContainer');
    const type = notification.status === 'error' ? 'error'
      : notification.pnl > 0 ? 'success' : 'info';
      
    const popup = document.createElement('div');
    popup.className = `notification-popup ${type}`;
    popup.style.cssText = `
      background: ${type === 'error' ? '#dc2626' : type === 'success' ? '#059669' : '#0891b2'};
      color: white;
      padding: 16px;
      border-radius: 8px;
      margin-bottom: 10px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
      animation: slideIn 0.3s ease;
    `;
    
    popup.innerHTML = `
      <div class="notification-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
        <strong>${notification.symbol?.toUpperCase() || 'ALERT'}</strong>
        <button class="close-btn" style="background: none; border: none; color: white; font-size: 18px; cursor: pointer;">&times;</button>
      </div>
      <div class="notification-content">
        <p style="margin: 0; font-size: 14px;">${notification.message || ''}</p>
        ${notification.pnl ? `
          <div class="pnl-display" style="margin-top: 8px; font-weight: bold;">
            ${formatCurrency(notification.pnl, notification.exchange_mode)}
          </div>
        ` : ''}
      </div>
    `;
    
    popup.querySelector('.close-btn').addEventListener('click', () => {
      popup.remove();
      displayedNotifications.delete(notification.id);
    });
    
    finalContainer.prepend(popup);
    
    // Auto-remove after 8 seconds
    setTimeout(() => {
      if (popup.parentNode) {
        popup.remove();
        displayedNotifications.delete(notification.id);
      }
    }, 8000);
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
    const targetBtn = document.getElementById(id);
    if (targetBtn) targetBtn.classList.add('active');
  };

  document.getElementById('filterAllBtn')?.addEventListener('click', () => {
    currentTradeFilter = 'all';
    currentTradePage = 1;
    if (activeTradesInterval) clearInterval(activeTradesInterval);
    renderTradeBook();
    setActiveBtn('filterAllBtn');
  });

  document.getElementById('filterActiveBtn')?.addEventListener('click', () => {
    currentTradeFilter = 'active';
    currentTradePage = 1;
    if (activeTradesInterval) clearInterval(activeTradesInterval);
    renderTradeBook();
    activeTradesInterval = setInterval(() => {
      renderTradeBook();
    }, 2000);
    setActiveBtn('filterActiveBtn');
  });

  document.getElementById('filterClosedBtn')?.addEventListener('click', () => {
    currentTradeFilter = 'closed';
    currentTradePage = 1;
    if (activeTradesInterval) clearInterval(activeTradesInterval);
    renderTradeBook();
    setActiveBtn('filterClosedBtn');
  });

  document.getElementById('clearNotificationsBtn')?.addEventListener('click', () => {
    const container = document.getElementById('notificationContainer');
    if (container) container.innerHTML = '';
    displayedNotifications.clear();
  });

  // Notification toggle functionality
  const notificationToggleBtn = document.getElementById('notificationToggleBtn');
  if (notificationToggleBtn) {
    notificationToggleBtn.addEventListener('click', () => {
      notificationsEnabled = !notificationsEnabled;
      notificationToggleBtn.textContent = notificationsEnabled ? 'Disable Popups' : 'Enable Popups';
      notificationToggleBtn.style.backgroundColor = notificationsEnabled ? '#dc2626' : '#059669';
    });
  }

  // --- Modal for Popup Data ---
  const popupModal = document.getElementById('popupModal');
  const modalClose = document.getElementById('modalClose');
  const popupContent = document.getElementById('popupContent');

  if (modalClose) {
    modalClose.onclick = () => {
      if (popupModal) popupModal.style.display = "none";
    };
  }
  
  window.onclick = (event) => {
    if (event.target === popupModal) {
      if (popupModal) popupModal.style.display = "none";
    }
  };

  document.getElementById('historyBtn')?.addEventListener('click', (event) => {
    event.preventDefault();
    fetch(`/api/trades_and_historyforsidebar?exchange=${currentExchange}&period=${currentPeriod}`)
      .then(response => response.json())
      .then(result => {
        let closedTradesforhistory = result.data.filter(trade =>
          typeof trade.pnl === 'number' && !isNaN(trade.pnl) &&
          trade.exchange_mode && trade.exchange_mode.toLowerCase() === currentExchange.toLowerCase()
        );
        renderPopupData(closedTradesforhistory, 'history');
      })
      .catch(error => console.error("Error fetching history data:", error));
  });

  document.getElementById('activeBtn')?.addEventListener('click', (event) => {
    event.preventDefault();
    const inQueueTrades = activeTrades.filter(trade => 
      trade.status.toLowerCase() === 'active' &&
      trade.exchange_mode && trade.exchange_mode.toLowerCase() === currentExchange.toLowerCase()
    );
    renderPopupData(inQueueTrades, 'inQueue');
  });

  function renderPopupData(data, type) {
    if (type === 'inQueue') {
      const dealQueueWindow = window.open('', 'DealQueueWindow', 'width=800,height=1000,scrollbars=yes,resizable=yes');

      let windowHTML = `
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Deal Queue - ${currentExchange.toUpperCase()}</title>
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
            background-color: #1b5e20;
            color: #fff;
          }
          .trade-type.sell {
            background-color: #b71c1c;
            color: #fff;
          }
          .trade-status {
            font-size: 0.85em;
            color: #fff;
            background:rgba(218, 171, 33, 0.7);
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
            justify-items: center;
          }
          .trade-details div {
            background-color: #2a2a2a;
            padding: 8px;
            border-radius: 8px;
            text-align: center;
            transition: background-color 0.2s;
            width: 100%;
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
          @media (min-width: 1200px) {
            .trade-card {
              margin-right: 300px;
              margin-left: 300px;
            }
            .trade-pair {
              margin-left: 870px;
            }
          }
        </style>
        <div class="feed-container">
          <h2>Deal Queue - ${currentExchange.toUpperCase()}</h2>
      `;

      data.forEach(trade => {
        let action = trade.action !== undefined ? String(trade.action).toUpperCase() : '';
        let symbol = trade.symbol !== undefined ? String(trade.symbol).toUpperCase() : '';
        let entryPrice = trade.entry_price !== undefined ? trade.entry_price : trade.price !== undefined ? trade.price : '';
        let stopLoss = trade.stop_loss !== undefined ? trade.stop_loss : '';
        let targetPrice = trade.target_price !== undefined ? trade.target_price : '';

        let displayAction = action;
        let tradeTypeClass = '';
        if (action === 'BUY' || action === 'LONG') {
          displayAction = 'Long';
          tradeTypeClass = 'buy';
        } else if (action === 'SELL' || action === 'SHORT') {
          displayAction = 'Short';
          tradeTypeClass = 'sell';
        } else {
          tradeTypeClass = 'initiated';
        }

        const formatNumber = (value) => {
          if (value === '' || value === undefined || value === null) return 'N/A';
          const num = parseFloat(value);
          return isNaN(num) ? 'N/A' : num.toFixed(2);
        };
        
        entryPrice = formatNumber(entryPrice);
        stopLoss = formatNumber(stopLoss);
        targetPrice = formatNumber(targetPrice);

        const formattedSymbol = symbol ? symbol.replace('_', ' | ') : 'UNKNOWN_PAIR';
        const cardClass = (action === 'BUY' || action === 'SELL' || action === 'LONG' || action === 'SHORT') ? 'trade-card initiated' : 'trade-card';

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

      dealQueueWindow.document.write(windowHTML);
      dealQueueWindow.document.close();
    } else {
      let columnsToUse = tableColumns;
      if (type === 'history') {
        columnsToUse = historyTableColumns;
      } else if (type === 'notifications') {
        columnsToUse = notificationTableColumns;
      }
      
      if (popupContent) {
        popupContent.innerHTML = '';
        createTable(popupContent, data, columnsToUse);
      }
      if (popupModal) popupModal.style.display = "block";
    }
  }

  // Notifications button for popup window
  document.getElementById('notificationsBtn')?.addEventListener('click', (event) => {
    event.preventDefault();
    if (allNotifications && Array.isArray(allNotifications) && allNotifications.length > 0) {
      const popupWindow = window.open('', 'Notifications', 'width=700,height=1000,scrollbars=yes');
      if (popupWindow) {
        let popupHTML = `
          <html>
          <head>
              <title>Notifications - ${currentExchange.toUpperCase()}</title>
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
                      font-weight: bold;
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
                  .trade-header {
                      display: flex;
                      justify-content: space-between;
                      align-items: center;
                      margin-bottom: 12px;
                      font-size: 1em;
                      font-weight: bold;
                  }
                  .trade-type {
                      font-weight: 600;
                      font-size: 0.9em;
                      padding: 6px 12px;
                      border-radius: 20px;
                      letter-spacing: 0.5px;
                  }
                  .trade-type.buy {
                      background-color:#1b5e20;
                      color: #fff;
                  }
                  .trade-type.sell {
                      background-color:#b71c1c;
                      color: #fff;
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
                  }
                  h2 {
                      color: #e0e0e0;
                      font-weight: 500;
                      text-align: center;
                      margin-bottom: 20px;
                  }
                  .timestamp {
                      font-size: 0.85em;
                      font-weight: 600;
                      color: rgba(255, 255, 255, 0.6);
                      margin-top: 16px;
                      text-align: right;
                      letter-spacing: 0.3px;
                  }
                  .pnl {
                      font-weight: 600;
                      padding: 10px;
                      border-radius: 8px;
                      margin-top: 8px;
                  }
                  .pnl.positive {
                      background-color: rgba(27, 94, 31, 0.12);
                      color: #a5d6a7;
                  }
                  .pnl.negative {
                      background-color: rgba(183, 28, 28, 0.11);
                      color: #ef9a9a;
                  }
              </style>
          </head>
          <body>
              <div class="feed-container">
                  <h2>Recent Notifications - ${currentExchange.toUpperCase()}</h2>
        `;

        allNotifications.forEach(n => {
          let action = n.action !== undefined ? String(n.action).toUpperCase() : '';
          let symbol = n.symbol !== undefined ? String(n.symbol).toUpperCase() : '';
          let createdAt = n.created_at !== undefined ? n.created_at : '';
          let message = n.message !== undefined ? String(n.message) : '';

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
            tradeTypeClass = 'buy';
          } else if (action === 'SELL') {
            displayAction = 'Short';
            tradeTypeClass = 'sell';
          }

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

          const formattedTimestamp = formatDate(createdAt);
          const formattedSymbol = symbol ? symbol.replace('_', ' | ') : 'UNKNOWN_PAIR';

          popupHTML += `
            <div class="trade-card">
                <div class="trade-header">
                    <span class="trade-type ${tradeTypeClass}">${displayAction || 'NOTIFICATION'}</span>
                    <span class="trade-pair">${formattedSymbol}</span>
                </div>
                <div style="padding: 8px 0;">
                    <p style="margin: 0; font-size: 14px;">${message}</p>
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