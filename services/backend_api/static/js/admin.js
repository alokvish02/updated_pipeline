function updateTime() {
    document.getElementById('liveTime').textContent = new Date().toLocaleTimeString();
}
setInterval(updateTime, 1000);
updateTime();

// const socket = io('https://f6k70dgb-5000.inc1.devtunnels.ms/');
const socket = io(`${window.location.protocol}//${window.location.hostname}:${window.location.port}`);
let performanceChart;

// Initialize Performance Chart
function initChart() {
    const ctx = document.getElementById('performanceChart').getContext('2d');
    performanceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'CPU Usage',
                data: [],
                borderColor: '#00cc66',
                tension: 0.3,
                borderWidth: 2,
                pointRadius: 0
            }, {
                label: 'Memory Usage',
                data: [],
                borderColor: '#0099ff',
                tension: 0.3,
                borderWidth: 2,
                pointRadius: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    grid: { color: '#4d4d4d' },
                    ticks: { color: '#fff' }
                },
                x: {
                    grid: { color: '#4d4d4d' },
                    ticks: { color: '#fff' }
                }
            },
            plugins: { legend: { labels: { color: '#fff' } } }
        }
    });
}

socket.on('connect', () => {
    console.log('Connected to WebSocket server');
    document.getElementById('connectionStatus').textContent = 'Connected';
});

socket.on('disconnect', () => {
    console.log('Disconnected from WebSocket server');
    document.getElementById('connectionStatus').textContent = 'Disconnected';
});

function updateChart(cpu, mem) {
    cpu = Number(cpu);
    mem = Number(mem);
    const now = new Date().toLocaleTimeString();
    if (performanceChart.data.labels.length >= 15) {
        performanceChart.data.labels.splice(0, 1);
        performanceChart.data.datasets[0].data.splice(0, 1);
        performanceChart.data.datasets[1].data.splice(0, 1);
    }
    performanceChart.data.labels.push(now);
    performanceChart.data.datasets[0].data.push(cpu);
    performanceChart.data.datasets[1].data.push(mem);
    performanceChart.update();
}

function updateUI(data) {
    const cpu = Number(data.cpu_percent) || 0;
    const mem = Number(data.memory_percent) || 0;
    const clampedCPU = Math.min(Math.max(cpu, 0), 100);
    const clampedMem = Math.min(Math.max(mem, 0), 100);
    document.getElementById('cpuProgress').style.width = `${clampedCPU}%`;
    document.getElementById('cpuProgress').textContent = `${clampedCPU.toFixed(1)}%`;
    document.getElementById('memoryProgress').style.width = `${clampedMem}%`;
    document.getElementById('memoryProgress').textContent = `${clampedMem.toFixed(1)}%`;
    document.getElementById('loadAvg').textContent = data.load_avg || '0, 0, 0';
    updateChart(clampedCPU, clampedMem);
}

function updateProcessTable(processes) {
    const tbody = document.getElementById('processTable');
    tbody.innerHTML = '';
    processes.forEach(proc => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${proc.name}</td>
            <td>${proc.status}</td>
            <td>${proc.pid}</td>
            <td>${calculateUptime(proc.last_ping)}</td>
            <td>${new Date(Number(proc.last_ping) * 1000).toLocaleTimeString()}</td>
        `;
        tbody.appendChild(row);
    });
}

function calculateUptime(lastPing) {
    const now = Math.floor(Date.now() / 1000);
    const diff = now - Number(lastPing);
    const hours = Math.floor(diff / 3600);
    const minutes = Math.floor((diff % 3600) / 60);
    return `${hours}h ${minutes}m`;
}

function controlProcess(name, action) {
    fetch('/admin/control/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, action: action })
    })
    .then(response => {
        if (!response.ok) throw new Error('Network response was not ok');
        return response.json();
    })
    .then(data => {
        if (!data.success) throw new Error('Control command failed');
        console.log(`Control command ${action} for ${name} successful`);
    })
    .catch(error => {
        console.error('Error:', error);
        alert(`Control failed: ${error.message}`);
    });
}

function controlPipeline(action) {
    const exchange = document.getElementById('exchangeSelect').value;
    const exchangeType = document.getElementById('exchange-type').value;
    const mode = document.getElementById('manualMode').checked ? 'manual' : 'automatic';

    // Validate inputs
    if (!exchangeType || !['nse', 'binance', 'snp', 'etf'].includes(exchangeType)) {
        alert('⚠️ Please select a valid exchange type (e.g., NSE, Binance, SNP, or ETF).');
        return;
    }

    if (!exchange || !['nse', 'binance', 'snp', 'etf', 'backtest_nse', 'backtest_binance', 'backtest_snp', 'backtest_etf'].includes(exchange)) {
        alert('⚠️ Please select a valid exchange option.');
        return;
    }

    if (mode === 'manual' && !userPairsSaved) {
        alert('⚠️ Please save the user pairs before running the pipeline in Manual mode.');
        return;
    }

    // Prepare payload
    const payload = {
        exchange: exchange,
        mode: mode,
    };

    fetch(`/admin/pipeline/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(response => {
        if (!response.ok) throw new Error('Failed to communicate with the pipeline server.');
        console.log(`✅ Pipeline ${action} command successfully issued for ${exchange}.`);
        updatePipelineStatus(action === 'start' ? 'running' : 'stopped');

        if (action === 'start') {
            const exchangeLabel = {
                nse: 'NSE Live Mode',
                binance: 'Binance Live Mode',
                snp: 'SNP Live Mode',
                etf: 'ETF Live Mode',
                backtest_nse: 'NSE Backtest Mode',
                backtest_binance: 'Binance Backtest Mode',
                backtest_snp: 'SNP Backtest Mode',
                backtest_etf: 'ETF Backtest Mode'
            }[exchange] || 'Unknown Mode';

            const symbolLabel = mode === 'manual' ? 'Manual Symbol Selection' : 'Automatic Symbol Selection';

            alert(
                `✅ Pipeline started successfully!\n\n` +
                `🧭 Exchange Mode: ${exchangeLabel}\n` +
                `🎯 Symbol Mode: ${symbolLabel}\n\n` +
                `The system is now actively processing data.`
            );
        }
    })
    .catch(err => {
        console.error('❌ Pipeline control failed:', err);
        alert('An error occurred while controlling the pipeline. Please check the console for details.');
    });
}

initChart();

socket.on('data_update', (data) => {
    if(data.hardware) { updateUI(data.hardware); }
    if(data.processes) { updateProcessTable(data.processes); }
});

// Load current system configuration on page load
function loadSystemConfig() {
    fetch('/admin/system/config')
        .then(res => res.json())
        .then(config => {
            document.getElementById('max_cpu_cores').value = config.max_cpu_cores;
            document.getElementById('memory_limit_gb').value = config.memory_limit_gb;
            document.getElementById('process_ttl').value = config.process_ttl;
            document.getElementById('system_monitor_interval').value = config.system_monitor_interval;
            document.getElementById('max_restart_attempts').value = config.max_restart_attempts;
        })
        .catch(err => console.error('Error loading system config:', err));
}

function saveSystemConfig() {
    const config = {
        max_cpu_cores: document.getElementById('max_cpu_cores').value,
        memory_limit_gb: document.getElementById('memory_limit_gb').value,
        process_ttl: document.getElementById('process_ttl').value,
        system_monitor_interval: document.getElementById('system_monitor_interval').value,
        max_restart_attempts: document.getElementById('max_restart_attempts').value
    };

    fetch('/admin/system/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
    })
    .then(response => response.json())
    .then(result => {
        if(result.success){
            alert('Configuration saved successfully.');
        } else {
            alert('Error saving configuration: ' + result.error);
        }
    })
    .catch(err => {
        console.error('Error saving system config:', err);
        alert('Error saving configuration.');
    });
}

document.addEventListener('DOMContentLoaded', loadSystemConfig);

function updatePipelineStatus(status) {
    const indicator = document.querySelector('.status-indicator');
    if (indicator) {
        indicator.classList.toggle('status-active', status === 'running');
        indicator.classList.toggle('status-inactive', status !== 'running');
    }
}

// Toggle sidebar visibility on mobile devices
const sidebarToggleBtn = document.getElementById('sidebarToggle');
const sidebar = document.getElementById('sidebar');
if (sidebarToggleBtn && sidebar) {
    sidebarToggleBtn.addEventListener("click", function(e) {
        e.stopPropagation();
        sidebar.classList.toggle("active");
    });

    document.addEventListener("click", function(e) {
        if (sidebar.classList.contains("active") && !sidebar.contains(e.target) && e.target !== sidebarToggleBtn) {
            sidebar.classList.remove("active");
        }
    });
}

// Fetch status immediately on load and then refresh every 1000 seconds
function fetchCompleteStatus() {
    fetch('http://192.168.10.101:5000/admin/hardware/status')
        .then(response => response.json())
        .then(data => {
            let html = '';
            const date = new Date(data.timestamp * 1000);
            html += `<h5>Timestamp: ${date.toLocaleString()}</h5>`;
            html += `<h5>System Metrics</h5>`;
            html += `<p><strong>CPU Percent:</strong> ${data.system.cpu_percent}%</p>`;
            html += `<p><strong>CPU Per Core:</strong> ${data.system.cpu_percent_per_cpu.join(', ')}%</p>`;
            html += `<p><strong>Load Average:</strong> ${data.system.load_avg}</p>`;
            html += `<p><strong>Memory:</strong> ${(data.system.memory_used / (1024 ** 3)).toFixed(2)}GB used / ${(data.system.memory_total / (1024 ** 3)).toFixed(2)}GB total</p>`;
            html += `<p><strong>Disk:</strong> ${(data.system.disk_used / (1024 ** 3)).toFixed(2)}GB used / ${(data.system.disk_total / (1024 ** 3)).toFixed(2)}GB total</p>`;
            html += `<h5>Python Process</h5>`;
            html += `<p><strong>Status:</strong> ${data.python.status}</p>`;
            html += `<p><strong>CPU Percent:</strong> ${data.python.cpu_percent}%</p>`;
            html += `<p><strong>Memory Info:</strong></p><ul>`;
            for (const key in data.python.memory_info) {
                html += `<li>${key}: ${data.python.memory_info[key]}</li>`;
            }
            html += `</ul>`;
            html += `<h5>Redis Server</h5>`;
            html += `<p><strong>Version:</strong> ${data.redis.redis_version}</p>`;
            html += `<p><strong>Connected Clients:</strong> ${data.redis.connected_clients}</p>`;
            html += `<p><strong>Used Memory:</strong> ${data.redis.used_memory_human}</p>`;
            html += `<h5>Postgres</h5>`;
            html += `<p><strong>Status:</strong> ${data.postgres.status}</p>`;
            html += `<p><strong>Version:</strong> ${data.postgres.version}</p>`;
            html += `<h5>Overall Status: ${data.status}</h5>`;
            document.getElementById('completeStatus').innerHTML = html;
        })
        .catch(err => {
            document.getElementById('completeStatus').innerHTML = `<p>Error fetching status: ${err}</p>`;
        });
}

fetchCompleteStatus();
setInterval(fetchCompleteStatus, 1000000);

function saveTotalCapital() {
    const totalCapital = document.getElementById('total_capital').value;
    const posVal = document.getElementById('pos_val').value;
    const std = document.getElementById('std').value;
    const lookback = document.getElementById('lookback').value;
    const faststd = document.getElementById('faststd').value;
    const fastlookback = document.getElementById('fastlookback').value;
    const strategyname = document.getElementById('strategyname').value;
    const windowValue = document.getElementById('window').value;
    const fromDate = document.getElementById('from_date').value;
    const toDate = document.getElementById('to_date').value;

    if (!totalCapital) {
        alert('Please enter an account capital value.');
        return;
    }

    if (fromDate && toDate && new Date(fromDate) > new Date(toDate)) {
        alert('From Date cannot be later than To Date.');
        return;
    }

    const payload = {
        total_capital: totalCapital,
        pos_val: posVal,
        std: std,
        lookback: lookback,
        fast_std: faststd,
        fast_lookback: fastlookback,
        strategy_name: strategyname,
        window: windowValue,
        from_date: fromDate,
        to_date: toDate
    };

    fetch('/admin/capital', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(response => response.json())
    .then(data => {
        if (data.message) {
            alert(data.message);
        } else if (data.error) {
            alert(`Error: ${data.error}`);
        }
    })
    .catch(error => {
        console.error('Error updating total capital:', error);
        alert('An error occurred while updating total capital.');
    });
}

// Exchange and Pipeline Control
document.addEventListener('DOMContentLoaded', function () {
    const exchangeTypeDropdown = document.getElementById('exchange-type');
    const pipelineExchangeSelect = document.getElementById('exchangeSelect');
    const pipelineControlButtons = pipelineExchangeSelect.closest('.d-flex.flex-wrap');
    const symbolTypeRadios = document.querySelectorAll('input[name="symbolType"]');

    // Function to update symbol type selection based on exchange type
    function updateSymbolTypeSelection(exchangeType) {
        symbolTypeRadios.forEach(radio => {
            radio.checked = radio.id === `${exchangeType}Symbol`;
        });
    }

    // Initialize visibility of pipeline control buttons and symbol type
    if (!['nse', 'binance', 'snp', 'etf'].includes(exchangeTypeDropdown.value)) {
        pipelineControlButtons.style.display = 'none';
    }
    updateSymbolTypeSelection(exchangeTypeDropdown.value);

    // Update exchangeSelect options and symbol type when exchange-type changes
    exchangeTypeDropdown.addEventListener('change', function () {
        const selectedExchange = this.value;

        // Show/hide pipeline control buttons
        pipelineControlButtons.style.display = ['nse', 'binance', 'snp', 'etf'].includes(selectedExchange) ? 'flex' : 'none';

        // Update exchangeSelect options
        pipelineExchangeSelect.innerHTML = '';
        if (selectedExchange === 'nse') {
            pipelineExchangeSelect.innerHTML = `
                <option value="nse">NSE Live</option>
                <option value="backtest_nse">NSE Backtest</option>
            `;
        } else if (selectedExchange === 'binance') {
            pipelineExchangeSelect.innerHTML = `
                <option value="binance">Binance Live</option>
                <option value="backtest_binance">Binance Backtest</option>
            `;
        } else if (selectedExchange === 'snp') {
            pipelineExchangeSelect.innerHTML = `
                <option value="snp">SNP Live</option>
                <option value="backtest_snp">SNP Backtest</option>
            `;
        } else if (selectedExchange === 'etf') {
            pipelineExchangeSelect.innerHTML = `
                <option value="etf">ETF Live</option>
                <option value="backtest_etf">ETF Backtest</option>
            `;
        } else {
            pipelineExchangeSelect.innerHTML = '<option value="">Select Exchange</option>';
        }

        // Set default value for exchangeSelect
        pipelineExchangeSelect.value = ['nse', 'binance', 'snp', 'etf'].includes(selectedExchange) ? selectedExchange : '';

        // Update symbol type selection
        updateSymbolTypeSelection(selectedExchange);
    });

    // Trigger change event on page load to initialize exchangeSelect and symbol type
    exchangeTypeDropdown.dispatchEvent(new Event('change'));
});

// Pipeline Control with Symbol Selection Mode
const manualMode = document.getElementById('manualMode');
const automaticMode = document.getElementById('automaticMode');
const userPairsTextarea = document.getElementById('user_pairs');
const exchangeSelectDropdown = document.getElementById('exchangeSelect');
const startPipelineBtn = document.getElementById('startPipelineBtn');
const exchangeTypeDropdown = document.getElementById('exchange-type');

let userPairsSaved = false;

function toggleStartButton() {
    if (startPipelineBtn) {
        startPipelineBtn.disabled = manualMode.checked && !userPairsSaved;
    }
}

async function saveUserPairs(event) {
    event.preventDefault();

    const symbolType = document.querySelector('input[name="symbolType"]:checked')?.id;
    const rawLines = userPairsTextarea.value.trim().split('\n');
    const user_pairs = rawLines
        .map(line => line.split(',').map(pair => pair.trim()))
        .filter(pair => pair.length === 2);

    if (user_pairs.length === 0) {
        alert('Please enter at least one valid pair.');
        return;
    }

    const data = {
        symbol_type: symbolType === 'nseSymbol' ? 'nse' : symbolType === 'binanceSymbol' ? 'binance' : symbolType === 'snpSymbol' ? 'snp' : 'etf',
        user_pairs: user_pairs,
    };

    try {
        const response = await fetch('/admin/user_pairs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });

        const result = await response.json();
        if (response.ok && (result.success || result.message)) {
            alert('✅ User pairs saved successfully!');
            userPairsSaved = true;
            toggleStartButton();
        } else {
            throw new Error(result.error || 'Unknown error saving pairs.');
        }
    } catch (error) {
        console.error('❌ Save User Pairs Error:', error);
        alert('An error occurred while saving user pairs: ' + error.message);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    manualMode.addEventListener('change', toggleStartButton);
    automaticMode.addEventListener('change', toggleStartButton);

    userPairsTextarea.addEventListener('input', () => {
        userPairsSaved = false;
        toggleStartButton();
    });

    document.getElementById('userPairsForm').addEventListener('submit', saveUserPairs);
    toggleStartButton();
});

document.addEventListener('DOMContentLoaded', async () => {
    const response = await fetch(`/admin/account_matrix_data`);
    const data = await response.json();

    document.getElementById('total_capital').value = data.total_capital || '';
    document.getElementById('pos_val').value = data.pos_val || '';
    document.getElementById('std').value = data.std || '';
    document.getElementById('lookback').value = data.lookback || '';
    document.getElementById('faststd').value = data.fast_std || '';
    document.getElementById('fastlookback').value = data.fast_lookback || '';
    document.getElementById('strategyname').value = data.strategy_name || '';
    document.getElementById('window').value = data.window || '';
    document.getElementById('from_date').value = data.from_date || '';
    document.getElementById('to_date').value = data.to_date || '';
});