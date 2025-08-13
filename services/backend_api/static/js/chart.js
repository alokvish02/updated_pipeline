const updateTime = () => {
  const liveTimeElement = document.getElementById('liveTime');
  if (liveTimeElement) {
    const now = Math.floor(Date.now() / 1000); // Current time in epoch seconds
    liveTimeElement.textContent = new Date(now * 1000).toLocaleString('en-IN', {
      timeZone: 'Asia/Kolkata',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    }).replace(/,/, '');
  }
};
setInterval(updateTime, 1000);
updateTime();

const calculateBollingerBands = (data, period, stdDev) => {
  if (!data || data.length < period) return [];
  const bands = [];
  for (let i = period - 1; i < data.length; i++) {
    const slice = data.slice(i - period + 1, i + 1).map(d => d.close);
    const sma = slice.reduce((sum, val) => sum + val, 0) / period;
    const variance = slice.reduce((sum, val) => sum + Math.pow(val - sma, 2), 0) / period;
    const std = Math.sqrt(variance);
    const upper = sma + stdDev * std;
    const lower = sma - stdDev * std;
    bands.push({
      time: data[i].time,
      upper,
      lower,
    });
  }
  return bands;
};

document.addEventListener('DOMContentLoaded', () => {
  const els = {
    tableSearch: document.getElementById('table-search'),
    tableList: document.getElementById('table-list'),
    spreadsSearch: document.getElementById('spreads-search'),
    spreadsList: document.getElementById('spreads-list'),
    favoritesList: document.getElementById('favorites-list'),
    currentSymbol: document.getElementById('current-symbol'),
    refreshButton: document.getElementById('refresh-chart-btn'),
    timeframeButtons: document.querySelectorAll('.timeframe-btn'),
    exchangeDropdown: document.getElementById('exchange-type'),
    chartHeader: document.getElementById('chart-header'),
  };

  let state = {
    allTables: [],
    allSpreads: [],
    currentSubscribedSymbol: '',
    favorites: JSON.parse(localStorage.getItem('favorites')) || [],
    currentTimeframe: 1,
    chart: null,
    candleSeries: null,
    bbSeriesFast: null,
    bbSeriesSlow: null,
    currentData: [],
    processingUpdate: false,
    pendingUpdates: [],
    autoScrollEnabled: false,
    loadingMoreData: false,
    earliestLoadedTime: null,
    latestLoadedTime: null,
    debounceTimer: null,
    selectedFilter: 'NSE',
    lastCrosshairPosition: null,
    bbFastVisible: false,
    bbFastPeriod: 100,
    bbFastStdDev: 4,
    bbSlowVisible: false,
    bbSlowPeriod: 100,
    bbSlowStdDev: 2,
    isDataPullEnabled: false,
    currentMinuteCandle: null,
    currentMinuteStart: null,
    minuteResetInterval: null,
  };
  const MAX_DATA_POINTS = 100000;
  const DEBOUNCE_DELAY = 100;

  const initializeMinuteReset = () => {
    if (state.minuteResetInterval) {
      clearInterval(state.minuteResetInterval);
    }
    state.minuteResetInterval = setInterval(() => {
      if (state.currentMinuteCandle) {
        state.currentData.push(state.currentMinuteCandle);
        state.candleSeries.update(state.currentMinuteCandle);
        state.currentMinuteCandle = null;
        state.currentMinuteStart = null;
      }
    }, 60000);
  };

  const fetchLatestData = async () => {
    if (!state.currentSubscribedSymbol || !state.isDataPullEnabled) return;
    try {
      showLoadingIndicator();
      const interval = getIntervalFromTimeframe(state.currentTimeframe);
      const response = await fetch(`/ohlcv?symbol=${state.currentSubscribedSymbol}&interval=${interval}&limit=100&offset=0`);
      if (!response.ok) return;
      const newData = await response.json();
      if (newData && newData.length) {
        processRealTimeUpdate(newData);
      }
    } catch (error) {
      console.log('Error fetching latest data:', error.message);
    } finally {
      hideLoadingIndicator();
    }
  };

  const startDataPull = () => {
    state.isDataPullEnabled = true;
    fetchLatestData();
  };

  const stopDataPull = () => {
    state.isDataPullEnabled = false;
  };

  const formatSymbol = (s) => {
    const l = s.toLowerCase();
    if (l.startsWith('nse_')) return `<span class="hidden-prefix">NSE_</span>${s.substring(4)}`;
    if (l.startsWith('crypto_')) return `<span class="hidden-prefix">CRYPTO_</span>${s.substring(7)}`;
    if (l.startsWith('snp_')) return `<span class="hidden-prefix">SNP_</span>${s.substring(4)}`;
    if (l.startsWith('etf_')) return `<span class="hidden-prefix">ETF_</span>${s.substring(4)}`;
    return s;
  };

  const formatSpread = (s) => {
    const l = s.toLowerCase();
    if (l.startsWith('nse_spreads_')) return `<span class="hidden-prefix">NSE_SPREADS_</span>${s.substring(12)}`;
    if (l.startsWith('nse_')) {
      const parts = s.split('_');
      if (parts.length >= 3) return `<span class="hidden-prefix">NSE_</span>${parts[2]}_${parts[1]}`;
    }
    if (l.startsWith('binance_')) return `<span class="hidden-prefix">BINANCE_</span>${s.substring(8)}`;
    if (l.startsWith('fyers_')) return `<span class="hidden-prefix">FYERS_</span>${s.substring(6)}`;
    if (l.startsWith('spreads_')) return `<span class="hidden-prefix">SPREADS_</span>${s.substring(8)}`;
    if (l.startsWith('snp_spreads_')) return `<span class="hidden-prefix">SNP_SPREADS_</span>${s.substring(12)}`;
    if (l.startsWith('etf_spreads_')) return `<span class="hidden-prefix">ETF_SPREADS_</span>${s.substring(12)}`;
    return s;
  };

  const formatHeaderSymbol = (s) => {
    const l = s.toLowerCase();
    if (l.startsWith('nse_spreads_')) return s.substring(12);
    if (l.startsWith('nse_')) return s.substring(4);
    if (l.startsWith('crypto_')) return s.substring(7);
    if (l.startsWith('binance_')) return s.substring(8);
    if (l.startsWith('spreads_')) return s.substring(8);
    if (l.startsWith('snp_spreads_')) return s.substring(12);
    if (l.startsWith('etf_spreads_')) return s.substring(12);
    if (l.startsWith('etf_')) return s.substring(4);
    return s;
  };

  const updateLists = () => {
    const tableTerm = els.tableSearch.value.trim().toUpperCase();
    const spreadsTerm = els.spreadsSearch.value.trim().toUpperCase();
    const filter = state.selectedFilter.toLowerCase();
    let filteredTables;
    let filteredSpreads;
    if (filter === 'nse') {
      filteredTables = state.allTables.filter(t => t.startsWith('NSE_') && t.includes(tableTerm));
      filteredSpreads = state.allSpreads.filter(s => (s.startsWith('NSE_SPREADS_') || s.toLowerCase().startsWith('nse_')) && s.includes(spreadsTerm));
    } else if (filter === 'snp') {
      filteredTables = state.allTables.filter(t => t.startsWith('SNP_') && t.includes(tableTerm));
      filteredSpreads = state.allSpreads.filter(s => s.startsWith('SNP_SPREADS_') && s.includes(spreadsTerm));
    } else if (filter === 'etf') {
      filteredTables = state.allTables.filter(t => t.startsWith('ETF_') && t.includes(tableTerm));
      filteredSpreads = state.allSpreads.filter(s => s.startsWith('ETF_SPREADS_') && s.includes(spreadsTerm));
    } else {
      filteredTables = state.allTables.filter(t => t.startsWith('CRYPTO_') && t.includes(tableTerm));
      filteredSpreads = state.allSpreads.filter(s => (s.startsWith('BINANCE_') || s.startsWith('FYERS_') || s.toLowerCase().startsWith('spreads_')) && s.includes(spreadsTerm));
    }
    renderList(filteredTables, els.tableList, formatSymbol);
    renderList(filteredSpreads, els.spreadsList, formatSpread);
  };

  els.exchangeDropdown.addEventListener('change', () => {
    state.selectedFilter = els.exchangeDropdown.value.toLowerCase();
    updateLists();
  });

  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const initializeChart = () => {
    if (state.chart) state.chart.remove();
    const styles = getComputedStyle(document.documentElement);
    state.chart = LightweightCharts.createChart(document.getElementById('chart-container'), {
      width: document.getElementById('chart-container').clientWidth,
      height: document.getElementById('chart-container').clientHeight,
      layout: {
        background: { color: styles.getPropertyValue('--primary-bg').trim() || '#131722' },
        textColor: styles.getPropertyValue('--text-primary').trim() || '#d1d4dc',
        fontSize: 12,
        fontFamily: 'Roboto, Arial, sans-serif',
      },
      grid: {
        vertLines: { color: 'rgba(42, 46, 57, 0.6)', style: LightweightCharts.LineStyle.Solid },
        horzLines: { color: 'rgba(42, 46, 57, 0.6)', style: LightweightCharts.LineStyle.Solid },
      },
      timeScale: {
        timeZone: 'Asia/Kolkata',
        timeVisible: true,
        secondsVisible: true,
        borderColor: 'rgba(197, 203, 206, 0.4)',
        rightOffset: 20,
        fixLeftEdge: false,
        fixRightEdge: false,
        tickMarkFormatter: (time, tickMarkType, locale) => {
          const date = new Date(time * 1000); // Convert epoch seconds to milliseconds
          if (tickMarkType === LightweightCharts.TickMarkType.Year) {
            return date.getFullYear();
          }
          if (tickMarkType === LightweightCharts.TickMarkType.Month) {
            return months[date.getMonth()];
          }
          if (tickMarkType === LightweightCharts.TickMarkType.DayOfMonth) {
            return date.getDate();
          }
          if (tickMarkType === LightweightCharts.TickMarkType.Time) {
            return date.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true, timeZone: 'Asia/Kolkata' });
          }
          return `${date.getDate()}/${months[date.getMonth()]}/${date.getFullYear()}`;
        },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
        vertLine: {
          color: 'rgba(224, 227, 235, 0.4)',
          width: 1,
          style: LightweightCharts.LineStyle.Solid,
          visible: true,
          labelVisible: true,
          labelBackgroundColor: '#0bb47a',
        },
        horzLine: {
          color: 'rgba(224, 227, 235, 0.4)',
          width: 1,
          style: LightweightCharts.LineStyle.Solid,
          visible: true,
          labelVisible: true,
          labelBackgroundColor: '#0bb47a',
        },
      },
      watermark: {
        visible: true,
        fontSize: 24,
        horzAlign: 'center',
        vertAlign: 'center',
        color: 'rgba(171, 71, 188, 0.2)',
        text: 'The One Alpha',
      },
    });

    state.candleSeries = state.chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
      priceFormat: { type: 'price', precision: 2, minMove: 0.000001 },
      priceScaleId: 'right',
      scaleMargins: { top: 0.2, bottom: 0.3 },
    });

    state.chart.applyOptions({ layout: { fontSize: 17 } });

    state.bbSeriesFast = {
      upper: state.chart.addLineSeries({
        color: '#2962FF',
        lineWidth: 1,
        title: `(Std: ${state.bbFastStdDev})`,
        priceScaleId: 'right',
        visible: state.bbFastVisible,
        scaleMargins: { top: 0.2, bottom: 0.3 },
      }),
      lower: state.chart.addLineSeries({
        color: '#2962FF',
        lineWidth: 1,
        title: `(Std: ${state.bbFastStdDev})`,
        priceScaleId: 'right',
        visible: state.bbFastVisible,
        scaleMargins: { top: 0.2, bottom: 0.3 },
      }),
    };

    state.bbSeriesSlow = {
      upper: state.chart.addLineSeries({
        color: '#E91E63',
        lineWidth: 1,
        title: `(Std: ${state.bbSlowStdDev})`,
        priceScaleId: 'right',
        visible: state.bbSlowVisible,
        scaleMargins: { top: 0.2, bottom: 0.3 },
      }),
      lower: state.chart.addLineSeries({
        color: '#E91E63',
        lineWidth: 1,
        title: `(Std: ${state.bbSlowStdDev})`,
        priceScaleId: 'right',
        visible: state.bbSlowVisible,
        scaleMargins: { top: 0.2, bottom: 0.3 },
      }),
    };

    const legend = document.createElement('div');
    legend.className = 'chart-legend';
    Object.assign(legend.style, {
      position: 'absolute',
      top: '10px',
      left: '10px',
      zIndex: '2',
      fontSize: '12px',
      padding: '5px',
      backgroundColor: 'rgba(19, 23, 34, 0.7)',
      color: '#d1d4dc',
      borderRadius: '3px',
    });
    document.getElementById('chart-container').appendChild(legend);

    window.updateLegend = (data, crosshairData = null) => {
  if (!data || !data.length) {
    legend.innerHTML = '<div>No data available</div>';
    return;
  }
  const candle = crosshairData || data[data.length - 1];
  if (!candle || !candle.time) {
    legend.innerHTML = '<div>No data available</div>';
    return;
  }
  const precision = Math.abs(candle.close) >= 1000 ? 2 : Math.abs(candle.close) >= 100 ? 3
                    : Math.abs(candle.close) >= 10 ? 4 : Math.abs(candle.close) >= 1 ? 5 : 6;
  const formatPrice = (p) => p.toFixed(precision);
  const priceChange = candle.close - candle.open;
  const percentChange = (priceChange / candle.open) * 100;
  const changeColor = priceChange >= 0 ? '#26a69a' : '#ef5350';
  const changeSign = priceChange >= 0 ? '+' : '';
  const adjustedTime = candle.time ; // Add 5.5 hours (19800 seconds)
  const formattedTime = new Date(adjustedTime * 1000).toLocaleString('en-IN', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).replace(/,/, '');

      legend.innerHTML = `
        <div style="margin-bottom: 2px; font-weight: bold;">${state.currentSubscribedSymbol ? formatHeaderSymbol(state.currentSubscribedSymbol) : 'Chart'}</div>
        
      `;
    };

 state.chart.subscribeCrosshairMove((param) => {
    if (!state.candleSeries || !param.seriesData || !param.time) {
      state.lastCrosshairPosition = null;
      if (state.currentData.length > 0) {
        const latestCandle = state.currentData[state.currentData.length - 1];
        window.updateLegend(state.currentData, latestCandle);
      } else {
        window.updateLegend([]);
      }
//      console.log('Crosshair: No valid data or series');
      return;
    }

    const candle = param.seriesData.get(state.candleSeries);
    if (!candle) {
      state.lastCrosshairPosition = null;
      if (state.currentData.length > 0) {
        const latestCandle = state.currentData[state.currentData.length - 1];
        window.updateLegend(state.currentData, latestCandle);
      } else {
        window.updateLegend([]);
      }
      console.log('Crosshair: No candle data found');
      return;
    }

    let candleData;
    if (typeof param.time === 'number') {
      candleData = state.currentData.find((d) => {
        if (typeof d.time === 'number') {
          return Math.abs(d.time - param.time) <= 60;
        }
        return false;
      });
    }

    if (!candleData) {
      const referenceTime = typeof param.time === 'number' ? param.time : Math.floor(Date.now() / 1000);
      candleData = state.currentData.reduce((closest, d) => {
        if (typeof d.time !== 'number') return closest;
        const diff = Math.abs(d.time - referenceTime);
        if (!closest || diff < closest.diff) {
          return { candle: d, diff };
        }
        return closest;
      }, null)?.candle;
    }

    if (candleData) {
      state.lastCrosshairPosition = { time: candleData.time, ...candle };
      window.updateLegend(state.currentData, { ...candle, time: candleData.time });
      console.log('Crosshair Time:', new Date(candleData.time * 1000).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
      console.log('Crosshair Data:', candle);
    } else {
      state.lastCrosshairPosition = null;
      if (state.currentData.length > 0) {
        const latestCandle = state.currentData[state.currentData.length - 1];
        window.updateLegend(state.currentData, latestCandle);
      } else {
        window.updateLegend([]);
      }
      console.log('Crosshair: No matching candle data');
    }
  });

    state.chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
      if (state.loadingMoreData || !state.currentSubscribedSymbol || state.autoScrollEnabled) return;
      if (state.debounceTimer) clearTimeout(state.debounceTimer);
      state.debounceTimer = setTimeout(() => {
        const visibleRange = state.chart.timeScale().getVisibleRange();
        if (!visibleRange) return;
        const timeDiff = visibleRange.to - visibleRange.from;
        const buffer = Math.max(600, timeDiff * 0.05);
        if (visibleRange.from <= state.earliestLoadedTime + buffer) {
          loadMoreHistoricalData();
        }
        state.debounceTimer = null;
      }, DEBOUNCE_DELAY);
    });
  };

  const popupControls = document.createElement('div');
  popupControls.style.display = 'flex';
  popupControls.style.marginLeft = '15px';

  const newWindowBtn = document.createElement('button');
  newWindowBtn.textContent = 'New Window';
  newWindowBtn.style.marginRight = '10px';
  newWindowBtn.addEventListener('click', () => {
    if (!state.currentSubscribedSymbol) {
      alert('Please subscribe to a symbol first.');
      return;
    }
    const symbol = state.currentSubscribedSymbol;
    const timeframe = state.currentTimeframe;
    const indicators = {
      bbFastVisible: state.bbFastVisible,
      bbFastPeriod: state.bbFastPeriod,
      bbFastStdDev: state.bbFastStdDev,
      bbSlowVisible: state.bbSlowVisible,
      bbSlowPeriod: state.bbSlowPeriod,
      bbSlowStdDev: state.bbSlowStdDev,
    };
    const params = new URLSearchParams({
      symbol,
      timeframe,
      indicators: JSON.stringify(indicators),
    });
    window.open(`./chart?${params.toString()}`, '_blank', 'width=800,height=600');
  });

  popupControls.appendChild(newWindowBtn);
  els.chartHeader.appendChild(popupControls);

  const hideToolsBtn = document.createElement('button');
  hideToolsBtn.textContent = 'Hide Tools';
  hideToolsBtn.style.marginLeft = '10px';
  hideToolsBtn.addEventListener('click', () => {
    const chartContainer = document.getElementById('chart-container');
    const originalParent = chartContainer.parentElement;
    hideToolsBtn.dataset.originalParentId = originalParent.id;

    const heightSlider = document.querySelector('.height-slider');
    const widthSlider = document.querySelector('.width-slider');
    if (heightSlider) heightSlider.style.display = 'none';
    if (widthSlider) widthSlider.style.display = 'none';

    document.body.style.overflow = 'hidden';

    const overlay = document.createElement('div');
    overlay.id = 'fullScreenChartOverlay';
    Object.assign(overlay.style, {
      position: 'fixed',
      top: '0',
      left: '0',
      width: '100vw',
      height: '100vh',
      backgroundColor: '#131722',
      zIndex: '10000',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    });

    overlay.appendChild(chartContainer);
    chartContainer.style.width = '100%';
    chartContainer.style.height = '100%';

    if (state.chart) {
      state.chart.resize(window.innerWidth, window.innerHeight);
    }

    const unhideBtn = document.createElement('button');
    unhideBtn.textContent = 'Unhide Tools';
    Object.assign(unhideBtn.style, {
      position: 'absolute',
      top: '10px',
      right: '10px',
      zIndex: '10001',
      padding: '8px 12px',
      fontSize: '14px',
      cursor: 'pointer'
    });
    overlay.appendChild(unhideBtn);

    document.body.appendChild(overlay);

    unhideBtn.addEventListener('click', () => {
      const parentId = hideToolsBtn.dataset.originalParentId;
      const originalWrapper = document.getElementById(parentId);
      originalWrapper.appendChild(chartContainer);

      chartContainer.style.width = '';
      chartContainer.style.height = '';
      if (state.chart) {
        state.chart.resize(originalWrapper.clientWidth, originalWrapper.clientHeight);
      }

      if (heightSlider) heightSlider.style.display = '';
      if (widthSlider) widthSlider.style.display = '';

      document.body.style.overflow = '';

      document.body.removeChild(overlay);
    });
  });

  els.chartHeader.appendChild(hideToolsBtn);

  els.chartHeader.insertAdjacentHTML('beforeend', `
    <div class="indicator-controls" style="display: flex; align-items: center; margin-left: 15px;">
      <label style="margin-right: 10px; color: #d1d4dc;">
        <input type="checkbox" id="bb-fast-toggle">
        Fast Band (Std: 4)
      </label>
      <input type="number" id="bb-fast-period" value="100" min="10" max="500" style="width: 60px; margin-right: 5px;" placeholder="Period">
      <input type="number" id="bb-fast-std" value="4" min="0.1" max="10" step="0.1" style="width: 60px; margin-right: 10px;" placeholder="Std Dev">
      <label style="margin-right: 10px; color: #d1d4dc;">
        <input type="checkbox" id="bb-slow-toggle">
        Slow Band (Std: 2)
      </label>
      <input type="number" id="bb-slow-period" value="100" min="10" max="500" style="width: 60px; margin-right: 5px;" placeholder="Period">
      <input type="number" id="bb-slow-std" value="2" min="0.1" max="10" step="0.1" style="width: 60px; margin-right: 10px;" placeholder="Std Dev">
      <label class="auto-scroll-toggle" style="display: flex; align-items: center; cursor: pointer;">
        <input type="checkbox" id="auto-scroll-checkbox" style="margin-right: 5px;" checked>
        <span style="font-size: 14px; color: #d1d4dc;">Auto-scroll</span>
      </label>
    </div>
  `);

  const indicatorControls = document.querySelector('.indicator-controls');
  const autoScrollToggle = document.querySelector('.auto-scroll-toggle');

  function toggleControlsVisibility() {
    const allControls = indicatorControls.querySelectorAll('input:not(#auto-scroll-checkbox), label:not(.auto-scroll-toggle)');
    const currentlyHidden = allControls[0].style.display === 'none';
    allControls.forEach(control => {
      control.style.display = currentlyHidden ? '' : 'none';
    });
  }

  function hideControlsByDefault() {
    const allControls = indicatorControls.querySelectorAll('input:not(#auto-scroll-checkbox), label:not(.auto-scroll-toggle)');
    allControls.forEach(control => {
      control.style.display = 'none';
    });
  }

  const toggleButton = document.createElement('button');
  toggleButton.textContent = 'ind';
  toggleButton.style.marginLeft = '15px';
  toggleButton.style.padding = '5px 10px';
  toggleButton.style.border = 'none';
  toggleButton.style.color = 'white';
  toggleButton.style.cursor = 'pointer';
  toggleButton.style.borderRadius = '5px';

  indicatorControls.appendChild(toggleButton);
  toggleButton.addEventListener('click', toggleControlsVisibility);

  autoScrollToggle.style.display = 'flex';
  hideControlsByDefault();

  document.getElementById('bb-fast-toggle').addEventListener('change', (e) => {
    state.bbFastVisible = e.target.checked;
    state.bbSeriesFast.upper.applyOptions({ visible: state.bbFastVisible });
    state.bbSeriesFast.lower.applyOptions({ visible: state.bbFastVisible });
  });

  document.getElementById('bb-slow-toggle').addEventListener('change', (e) => {
    state.bbSlowVisible = e.target.checked;
    state.bbSeriesSlow.upper.applyOptions({ visible: state.bbSlowVisible });
    state.bbSeriesSlow.lower.applyOptions({ visible: state.bbSlowVisible });
  });

  document.getElementById('bb-fast-period').addEventListener('input', (e) => {
    const value = parseInt(e.target.value);
    if (value >= 10 && value <= 500) {
      state.bbFastPeriod = value;
      updateBollingerBands();
    }
  });

  document.getElementById('bb-fast-std').addEventListener('input', (e) => {
    const value = parseFloat(e.target.value);
    if (value >= 0.1 && value <= 10) {
      state.bbFastStdDev = value;
      state.bbSeriesFast.upper.applyOptions({ title: `BB Fast Upper (Std: ${value})` });
      state.bbSeriesFast.lower.applyOptions({ title: `BB Fast Lower (Std: ${value})` });
      updateBollingerBands();
    }
  });

  document.getElementById('bb-slow-period').addEventListener('input', (e) => {
    const value = parseInt(e.target.value);
    if (value >= 10 && value <= 500) {
      state.bbSlowPeriod = value;
      updateBollingerBands();
    }
  });

  document.getElementById('bb-slow-std').addEventListener('input', (e) => {
    const value = parseFloat(e.target.value);
    if (value >= 0.1 && value <= 10) {
      state.bbSlowStdDev = value;
      state.bbSeriesSlow.upper.applyOptions({ title: `BB Slow Upper (Std: ${value})` });
      state.bbSeriesSlow.lower.applyOptions({ title: `BB Slow Lower (Std: ${value})` });
      updateBollingerBands();
    }
  });

  document.getElementById('auto-scroll-checkbox').addEventListener('change', function () {
    state.autoScrollEnabled = this.checked;
    if (state.autoScrollEnabled) state.chart.timeScale().scrollToRealTime();
  });

  window.addEventListener('resize', _.debounce(() => {
    const container = document.getElementById('chart-container');
    state.chart.resize(container.clientWidth, container.clientHeight);
  }, 200));

  const transformData = (rawData) => {
    if (!rawData || !rawData.length) {
      return [];
    }


    return rawData
      .map((item) => {
        const time = parseInt(item.timestamp); // Use epoch seconds directly
        const open = parseFloat(item.open);
        const high = parseFloat(item.high);
        const low = parseFloat(item.low);
        const close = parseFloat(item.close);
        const volume = parseFloat(item.volume) || 0;

        if (![open, high, low, close].every(Number.isFinite)) {
          console.error('Invalid OHLCV data:', item);
          return null;
        }

        const bar = { time, open, high, low, close, volume };
        return bar;
      })
      .filter(Boolean)
      .sort((a, b) => a.time - b.time);
  };

  const updateBollingerBands = _.debounce(() => {
    if (!state.currentData.length) return;
    const bbFast = calculateBollingerBands(state.currentData, state.bbFastPeriod, state.bbFastStdDev);
    const bbSlow = calculateBollingerBands(state.currentData, state.bbSlowPeriod, state.bbSlowStdDev);
    state.bbSeriesFast.upper.setData(bbFast.map(b => ({ time: b.time, value: b.upper })));
    state.bbSeriesFast.lower.setData(bbFast.map(b => ({ time: b.time, value: b.lower })));
    state.bbSeriesSlow.upper.setData(bbSlow.map(b => ({ time: b.time, value: b.upper })));
    state.bbSeriesSlow.lower.setData(bbSlow.map(b => ({ time: b.time, value: b.lower })));
  }, 200);

  const loadInitialData = async (symbol, timeframe) => {
    try {
      showLoadingIndicator();
      const interval = getIntervalFromTimeframe(timeframe);
      const response = await fetch(`/ohlcv?symbol=${symbol}&interval=${interval}&limit=1000&offset=0`);
      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }
      const data = await response.json();
      if (!data || !data.length) {
        window.updateLegend([]);
        return false;
      }

      state.currentData = [];
      const transformed = transformData(data);
      if (!transformed.length) {
        window.updateLegend([]);
        return false;
      }

      state.currentData = transformed;
      state.earliestLoadedTime = transformed[0].time;
      state.latestLoadedTime = transformed[transformed.length - 1].time;

      state.candleSeries.setData(transformed);
      updateBollingerBands();
      window.updateLegend(transformed);

      const lastBars = transformed.slice(-200);
      if (lastBars.length) {
        state.chart.timeScale().setVisibleRange({ from: lastBars[0].time, to: lastBars[lastBars.length - 1].time });
      }

      if (state.autoScrollEnabled) state.chart.timeScale().scrollToRealTime();
      return true;
    } catch (error) {
      console.log('Error loading initial data:', error.message);
      window.updateLegend([]);
      return false;
    } finally {
      hideLoadingIndicator();
    }
  };

  const loadMoreHistoricalData = async () => {
    if (state.loadingMoreData || !state.currentSubscribedSymbol) return;
    state.loadingMoreData = true;
    showLoadingIndicator();
    try {
      const offset = Math.floor(state.currentData.length / 1000) * 1000;
      const interval = getIntervalFromTimeframe(state.currentTimeframe);
      const response = await fetch(`/ohlcv?symbol=${state.currentSubscribedSymbol}&interval=${interval}&limit=1000&offset=${offset}`);
      if (!response.ok) {
        throw new Error('API error');
      }
      const newData = await response.json();
      if (!newData || !newData.length) {
        return;
      }

      const transformed = transformData(newData);
      if (!transformed.length) {
        console.log('No valid historical data after transformation');
        return;
      }

      const existingTimes = new Set(state.currentData.map(d => d.time));
      const filteredNewData = transformed.filter(d => !existingTimes.has(d.time));
      if (!filteredNewData.length) {
        console.log('No new historical data to add');
        return;
      }

      state.currentData = [...filteredNewData, ...state.currentData].sort((a, b) => a.time - b.time);
      state.candleSeries.setData(state.currentData);
      updateBollingerBands();
      window.updateLegend(state.currentData);
      state.earliestLoadedTime = state.currentData[0].time;
      state.latestLoadedTime = state.currentData[state.currentData.length - 1].time;

      if (state.currentData.length > MAX_DATA_POINTS) {
        state.currentData = state.currentData.slice(state.currentData.length - MAX_DATA_POINTS);
        state.earliestLoadedTime = state.currentData[0].time;
        state.candleSeries.setData(state.currentData);
        updateBollingerBands();
      }

      const visibleRange = state.chart.timeScale().getVisibleRange();
      if (visibleRange) {
        state.chart.timeScale().setVisibleRange(visibleRange);
      }
    } catch (error) {
      console.log('Failed to load more historical data:', error.message);
    } finally {
      state.loadingMoreData = false;
      hideLoadingIndicator();
    }
  };

  const getIntervalFromTimeframe = (tf) => {
    if (tf >= 1440) return '1d';
    if (tf >= 60) return '1h';
    if (tf >= 30) return '30m';
    if (tf >= 15) return '15m';
    if (tf >= 5) return '5m';
    return '1m';
  };
const processRealTimeUpdate = _.debounce((newData) => {
  console.log('processRealTimeUpdate triggered');

  if (state.processingUpdate) {
    state.pendingUpdates.push(newData);
    return;
  }

  state.processingUpdate = true;

  try {
    const transformed = newData
      .map((item) => {
        const time = parseInt(item.timestamp); // Use epoch seconds directly
        const open = parseFloat(item.open);
        const high = parseFloat(item.high);
        const low = parseFloat(item.low);
        const close = parseFloat(item.close);
        const volume = parseFloat(item.volume) || 0;

        if (![open, high, low, close].every(Number.isFinite)) {
          console.error('Invalid OHLCV data:', item);
          return null;
        }

        return { time, open, high, low, close, volume };
      })
      .filter(Boolean)
      .sort((a, b) => a.time - b.time);

    if (!transformed.length) {
      console.log('No valid data in processRealTimeUpdate');
      return;
    }

    const latestTime = state.currentData.length ? state.currentData[state.currentData.length - 1].time : null;

    let newCandlesAdded = false;
    transformed.forEach((candle) => {
      if (latestTime !== null && candle.time < latestTime) {
        return;
      }

      const existing = state.currentData.find((d) => d.time === candle.time);
      if (existing) {
        if (
          existing.open !== candle.open ||
          existing.high !== candle.high ||
          existing.low !== candle.low ||
          existing.close !== candle.close ||
          existing.volume !== candle.volume
        ) {
          state.currentData = state.currentData.filter((d) => d.time !== candle.time);
          state.currentData.push(candle);
          state.candleSeries.update(candle);
          newCandlesAdded = true;
        }
      } else {
        state.currentData.push(candle);
        state.candleSeries.update(candle);
        newCandlesAdded = true;
      }
    });

//    console.log('New candles added?', newCandlesAdded);
//    console.log('Socket connected?', socket?.connected);
//    console.log('Emit fields:', {
//      symbol: state.currentSubscribedSymbol,
//      timeframe: state.currentTimeframe
//    });

    if (transformed.length > 0) {
      const emitPayload = {
        message: 'DataPulled',
        symbol: state.currentSubscribedSymbol,
        timeframe: state.currentTimeframe,
        timestamp: Math.floor(Date.now() / 1000),
        sid: socket.id
      };
      console.log('Emitting client_message:', emitPayload);
      socket.emit('client_message', emitPayload);
    }


    if (newCandlesAdded) {
      const emitPayload = {
        message: 'DataPulled',
        symbol: state.currentSubscribedSymbol,
        timeframe: state.currentTimeframe,
        timestamp: Math.floor(Date.now() / 1000),
        sid: socket.id
      };
      console.log('Emitting client_message:', emitPayload);
      socket.emit('client_message', emitPayload);
    }

    state.currentData.sort((a, b) => a.time - b.time);
    if (state.currentData.length > MAX_DATA_POINTS) {
      state.currentData = state.currentData.slice(state.currentData.length - MAX_DATA_POINTS);
      state.earliestLoadedTime = state.currentData[0]?.time;
      state.candleSeries.setData(state.currentData);
    }

    updateBollingerBands();
    state.earliestLoadedTime = state.currentData[0]?.time;
    state.latestLoadedTime = state.currentData[state.currentData.length - 1]?.time;

    if (!state.lastCrosshairPosition) window.updateLegend(state.currentData);

    if (state.autoScrollEnabled && !state.lastCrosshairPosition) {
      state.chart.timeScale().scrollToRealTime();
    }

  } catch (error) {
    console.error('Error processing real-time update:', error);
  } finally {
    state.processingUpdate = false;
    if (state.pendingUpdates.length) {
      const next = state.pendingUpdates.shift();
      setTimeout(() => processRealTimeUpdate(next), 0);
    }
  }
}, 100);

  const SOCKET_URL = `${window.location.protocol}//${window.location.hostname}${window.location.port ? `:${window.location.port}` : ''}`;

  const socket = io(SOCKET_URL, {
    transports: ['websocket'],
    reconnectionAttempts: 10,
    reconnectionDelay: 1000,
    reconnectionDelayMax: 5000,
    timeout: 20000,
    pingTimeout: 10000,
    pingInterval: 25000,
    autoConnect: true,
    forceNew: true,
  });

  socket.on('connect', () => {
    console.log('WebSocket connected');
    if (state.currentSubscribedSymbol) {
      socket.emit('symbol_subscribed', {
        symbol: state.currentSubscribedSymbol,
        timestamp: Math.floor(Date.now() / 1000),
        sid: socket.id
      });
      socket.emit('subscribe', { table_name: state.currentSubscribedSymbol, timeframe: state.currentTimeframe });
    }
  });

  socket.on('disconnect', () => {
    console.log('WebSocket disconnected');
    stopDataPull();
  });

  socket.on('reconnect', () => {
    console.log('WebSocket reconnected');
    if (state.currentSubscribedSymbol) {
      socket.emit('symbol_subscribed', {
        symbol: state.currentSubscribedSymbol,
        timestamp: Math.floor(Date.now() / 1000),
        sid: socket.id
      });
      socket.emit('subscribe', { table_name: state.currentSubscribedSymbol, timeframe: state.currentTimeframe });
    }
  });

  socket.on('reconnect_error', () => {
    console.log('WebSocket reconnect error');
  });

  socket.on('reconnect_failed', () => {
    console.log('WebSocket reconnect failed');
    alert('Connection to server lost. Please refresh.');
  });

  socket.on('realtime_update', (data) => {
    if (state.currentSubscribedSymbol) {
      processRealTimeUpdate(data);
    }
  });

  socket.on('data_update', (data) => {
    if (data && data.data_pull && typeof data.data_pull.data_pull === 'boolean') {
      if (data.data_pull.data_pull === true) {
        startDataPull();
      } else {
        console.log('Data pull disabled');
        stopDataPull();
      }
    }
  });

  socket.on('error', (error) => {
    console.log('WebSocket error:', error);
  });

  socket.on('ltp_update', (data) => {
    let ltpElement = document.getElementById('ltp-display');
    if (!ltpElement) {
      ltpElement = document.createElement('div');
      ltpElement.id = 'ltp-display';
      ltpElement.style.margin = '0';
      ltpElement.style.padding = '0';
      ltpElement.style.marginLeft = '1px';
      ltpElement.style.fontWeight = 'bold';
      ltpElement.style.fontSize = '14px';
      els.chartHeader.appendChild(ltpElement);
    }
    if (data.ltp) {
      const roundedLtp = parseFloat(data.ltp).toFixed(1);
      ltpElement.textContent = `LTP: ${roundedLtp}`;
    } else if (data.error) {
      ltpElement.textContent = `LTP Error: ${data.error}`;
    }
    if (!state.currentSubscribedSymbol || state.currentSubscribedSymbol !== data.symbol) {
      return;
    }

    const currentTime = parseInt(data.timestamp) || Math.floor(Date.now() / 1000);
    const currentMinute = Math.floor(currentTime / 60) * 60;

    if (state.currentMinuteStart !== currentMinute) {
      if (state.currentMinuteCandle) {
        state.currentData.push(state.currentMinuteCandle);
        state.candleSeries.update(state.currentMinuteCandle);
      }
      state.currentMinuteStart = currentMinute;
      state.currentMinuteCandle = {
        time: currentMinute,
        open: data.ltp,
        high: data.ltp,
        low: data.ltp,
        close: data.ltp,
        volume: 0,
      };
//      console.log('New minute candle:', state.currentMinuteCandle);
    } else {
      if (state.currentMinuteCandle) {
        state.currentMinuteCandle.close = data.ltp;
        if (data.ltp > state.currentMinuteCandle.high) {
          state.currentMinuteCandle.high = data.ltp;
        }
        if (data.ltp < state.currentMinuteCandle.low) {
          state.currentMinuteCandle.low = data.ltp;
        }
      }
    }
    if (state.currentMinuteCandle) {
      state.candleSeries.update(state.currentMinuteCandle);
      window.updateLegend([...state.currentData, state.currentMinuteCandle]);
    }
  });

  const unsubscribeFromCurrentSymbol = () => {
    if (state.currentSubscribedSymbol) {
      console.log('Unsubscribing from:', state.currentSubscribedSymbol);
      socket.emit('unsubscribe', { table_name: state.currentSubscribedSymbol });
      state.currentSubscribedSymbol = '';
      stopDataPull();
      state.currentData = [];
      state.earliestLoadedTime = null;
      state.latestLoadedTime = null;
      state.lastCrosshairPosition = null;
      state.currentMinuteCandle = null;
      state.currentMinuteStart = null;

      if (state.candleSeries) state.candleSeries.setData([]);
      if (state.bbSeriesFast) {
        state.bbSeriesFast.upper.setData([]);
        state.bbSeriesFast.lower.setData([]);
      }
      if (state.bbSeriesSlow) {
        state.bbSeriesSlow.upper.setData([]);
        state.bbSeriesSlow.lower.setData([]);
      }

      els.currentSymbol.textContent = 'No Symbol Selected';
      const ltpElement = document.getElementById('ltp-display');
      if (ltpElement) {
        ltpElement.textContent = '';
      }
      window.updateLegend([]);
    }
  };

  const handleSymbolClick = async (symbol) => {
    if (state.currentSubscribedSymbol === symbol) {
      console.log('Already subscribed to:', symbol);
      return;
    }

    console.log('Switching to symbol:', symbol);
    unsubscribeFromCurrentSymbol();
    initializeMinuteReset();

    state.currentSubscribedSymbol = symbol;
    els.currentSymbol.textContent = formatHeaderSymbol(symbol);
    socket.emit('symbol_subscribed', {
      symbol: symbol,
      timestamp: Math.floor(Date.now() / 1000),
      sid: socket.id
    });

    const success = await loadInitialData(symbol, state.currentTimeframe);
    if (success) {
      socket.emit('subscribe', { table_name: symbol, timeframe: state.currentTimeframe });
      console.log('Successfully subscribed to:', symbol);
    } else {
      console.log('Failed to load data for:', symbol);
      state.currentSubscribedSymbol = '';
      els.currentSymbol.textContent = 'No Symbol Selected';
    }
  };

  const handleTimeframeChange = async (tf) => {
    state.currentTimeframe = tf;
    if (state.currentSubscribedSymbol) {
      console.log('Changing timeframe to:', tf, 'for symbol:', state.currentSubscribedSymbol);
      socket.emit('unsubscribe', { table_name: state.currentSubscribedSymbol });
      state.currentData = [];
      state.candleSeries.setData([]);
      state.currentMinuteCandle = null;
      state.currentMinuteStart = null;

      const success = await loadInitialData(state.currentSubscribedSymbol, state.currentTimeframe);
      if (success) {
        socket.emit('subscribe', { table_name: state.currentSubscribedSymbol, timeframe: tf });
      }
    }
  };

  const renderList = (items, listEl, formatter = (x) => x) => {
    listEl.innerHTML = items.map(item => `
      <li class="table-item" data-symbol="${item}">
        <span>${formatter(item)}</span>
        <button class="favorite-btn">♡</button>
      </li>
    `).join('');
    listEl.querySelectorAll('.table-item').forEach(el => {
      const symbol = el.dataset.symbol;
      el.querySelector('span').addEventListener('click', () => handleSymbolClick(symbol));
      el.querySelector('.favorite-btn').addEventListener('click', (e) => {
        e.stopPropagation();
        if (!state.favorites.includes(symbol)) {
          state.favorites.push(symbol);
          localStorage.setItem('favorites', JSON.stringify(state.favorites));
          renderFavorites();
        }
      });
    });
  };

  const renderFavorites = () => {
    els.favoritesList.innerHTML = state.favorites.map(fav => `
      <li class="favorite-item">
        <span>${formatHeaderSymbol(fav)}</span>
        <button class="remove-btn">💔</button>
      </li>
    `).join('');
    els.favoritesList.querySelectorAll('.favorite-item').forEach((item, idx) => {
      const symbol = state.favorites[idx];
      item.querySelector('span').addEventListener('click', () => handleSymbolClick(symbol));
      item.querySelector('.remove-btn').addEventListener('click', (e) => {
        e.stopPropagation();
        state.favorites = state.favorites.filter(f => f !== symbol);
        localStorage.setItem('favorites', JSON.stringify(state.favorites));
        renderFavorites();
      });
    });
  };

  const initialize = async () => {
    initializeChart();
    initializeMinuteReset();

    try {
      const [tablesResponse, spreadsResponse] = await Promise.all([fetch('/tables'), fetch('/spreads')]);
      const tablesData = await tablesResponse.json();
      const spreadsData = await spreadsResponse.json();

      state.allTables = (tablesData.tables || [])
        .filter(tbl => /^(CRYPTO|NSE|SNP|ETF)_[^_]+$/.test(tbl.toUpperCase()))
        .map(tbl => tbl.toUpperCase())
        .sort();

      state.allSpreads = (spreadsData.tables || [])
        .map(s => s.replace(/[(),]/g, '').toUpperCase())
        .sort();

      updateLists();
      renderFavorites();
      els.currentSymbol.textContent = 'No Symbol Selected';
      window.updateLegend([]);
    } catch (error) {
      console.log('Error initializing lists:', error.message);
    }
  };

  els.tableSearch.addEventListener('input', updateLists);
  els.spreadsSearch.addEventListener('input', updateLists);
  els.timeframeButtons.forEach(btn => btn.addEventListener('click', () => {
    els.timeframeButtons.forEach(x => x.classList.remove('active'));
    btn.classList.add('active');
    handleTimeframeChange(parseInt(btn.dataset.timeframe, 10));
  }));

  els.refreshButton.addEventListener('click', async () => {
    if (state.currentSubscribedSymbol) {
      console.log('Refreshing data for:', state.currentSubscribedSymbol);
      if (state.latestLoadedTime) {
        try {
          const interval = getIntervalFromTimeframe(state.currentTimeframe);
          const response = await fetch(`/ohlcv?symbol=${state.currentSubscribedSymbol}&interval=${interval}&limit=1000&offset=0`);
          if (!response.ok) {
            return;
          }
          const newData = await response.json();
          if (newData && newData.length) {
            processRealTimeUpdate(newData);
          }
        } catch (error) {
          console.log('Error during refresh fetch:', error.message);
        }
      } else {
        const success = await loadInitialData(state.currentSubscribedSymbol, state.currentTimeframe);
        if (success) {
          socket.emit('subscribe', { table_name: state.currentSubscribedSymbol, timeframe: state.currentTimeframe });
        }
      }
    } else {
      console.log('No symbol selected for refresh');
    }
  });

  initialize();

  window.addEventListener('resize', _.debounce(() => {
    const container = document.getElementById('chart-container');
    state.chart.resize(container.clientWidth, container.clientHeight);
  }, 200));
});

document.addEventListener('DOMContentLoaded', () => {
  const exchangeDropdown = document.getElementById('exchange-type');
  const storedExchange = localStorage.getItem('selectedExchange');
  if (storedExchange) {
    exchangeDropdown.value = storedExchange;
    exchangeDropdown.dispatchEvent(new Event('change'));
  }
  exchangeDropdown.addEventListener('change', (e) => localStorage.setItem('selectedExchange', e.target.value));

  const sectionButtons = document.querySelectorAll('.section-btn');
  const sections = {
    tables: document.getElementById('tables-section'),
    spreads: document.getElementById('spreads-section'),
    favorites: document.getElementById('favorites-section'),
  };

  const handleSectionToggle = (s) => {
    Object.values(sections).forEach(sec => sec.classList.remove('active'));
    sections[s].classList.add('active');
    localStorage.setItem('activeSection', s);
  };

  sectionButtons.forEach(btn => btn.addEventListener('click', (e) => {
    sectionButtons.forEach(x => x.classList.remove('active'));
    e.target.classList.add('active');
    handleSectionToggle(e.target.dataset.section);
  }));

  const storedSection = localStorage.getItem('activeSection') || 'tables';
  const activeBtn = document.querySelector(`.section-btn[data-section="${storedSection}"]`);
  if (activeBtn) activeBtn.classList.add('active');
  handleSectionToggle(storedSection);
});

const showLoadingIndicator = () => {
  const el = document.getElementById('loading-indicator');
  if (el) el.style.display = 'block';
};

const hideLoadingIndicator = () => {
  const el = document.getElementById('loading-indicator');
  if (el) el.style.display = 'none';
};