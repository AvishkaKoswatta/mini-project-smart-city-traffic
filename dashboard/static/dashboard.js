// Configuration
const API_URL = 'http://localhost:5000/traffic';
const ROWS_PER_PAGE = 20;

// Global state
let allData = [];
let filteredData = [];
let currentPage = 1;
let trafficChart = null;
let distributionChart = null;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', () => {
    initEventListeners();
    loadData();
});

// Event listeners
function initEventListeners() {
    document.getElementById('refreshBtn').addEventListener('click', loadData);
    document.getElementById('searchInput').addEventListener('input', handleSearch);
    document.getElementById('columnFilter').addEventListener('change', handleColumnFilter);
    document.getElementById('prevPage').addEventListener('click', () => changePage(-1));
    document.getElementById('nextPage').addEventListener('click', () => changePage(1));
}

// Load data from API
async function loadData() {
    showLoading(true);
    hideError();

    try {
        const response = await fetch(API_URL);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        allData = await response.json();
        
        if (!allData || allData.length === 0) {
            showError('No data available. Please check if the data files exist.');
            return;
        }

        filteredData = [...allData];
        currentPage = 1;
        
        updateDashboard();
        updateLastUpdateTime();
    } catch (error) {
        console.error('Error loading data:', error);
        showError(`Failed to load data: ${error.message}`);
    } finally {
        showLoading(false);
    }
}

// Update all dashboard components
function updateDashboard() {
    updateStats();
    updateCharts();
    updateTable();
    updateColumnFilter();
}

// Update statistics cards
function updateStats() {
    const numericColumns = getNumericColumns(allData);
    
    document.getElementById('totalRecords').textContent = allData.length.toLocaleString();
    document.getElementById('dataPoints').textContent = (allData.length * Object.keys(allData[0] || {}).length).toLocaleString();
    
    if (numericColumns.length > 0) {
        const firstNumericCol = numericColumns[0];
        const values = allData.map(row => parseFloat(row[firstNumericCol])).filter(v => !isNaN(v));
        
        if (values.length > 0) {
            const avg = values.reduce((a, b) => a + b, 0) / values.length;
            const max = Math.max(...values);
            
            document.getElementById('avgTraffic').textContent = avg.toFixed(2);
            document.getElementById('peakTraffic').textContent = max.toFixed(2);
        }
    } else {
        document.getElementById('avgTraffic').textContent = 'N/A';
        document.getElementById('peakTraffic').textContent = 'N/A';
    }
}

// Update charts
function updateCharts() {
    if (allData.length === 0) return;

    const numericColumns = getNumericColumns(allData);
    const dateColumns = getDateColumns(allData);
    
    // Traffic Over Time Chart
    if (dateColumns.length > 0 && numericColumns.length > 0) {
        updateTimeSeriesChart(dateColumns[0], numericColumns[0]);
    }
    
    // Distribution Chart
    if (numericColumns.length > 0) {
        updateDistributionChart(numericColumns[0]);
    }
}

// Update time series chart
function updateTimeSeriesChart(dateCol, valueCol) {
    const ctx = document.getElementById('trafficChart');
    
    // Sort data by date
    const sortedData = [...allData].sort((a, b) => {
        const dateA = new Date(a[dateCol]);
        const dateB = new Date(b[dateCol]);
        return dateA - dateB;
    });
    
    const labels = sortedData.map(row => {
        const date = new Date(row[dateCol]);
        return date.toLocaleDateString();
    }).slice(0, 100); // Limit to 100 points for performance
    
    const values = sortedData.map(row => parseFloat(row[valueCol])).slice(0, 100);
    
    if (trafficChart) {
        trafficChart.destroy();
    }
    
    trafficChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: valueCol,
                data: values,
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Update distribution chart
function updateDistributionChart(valueCol) {
    const ctx = document.getElementById('distributionChart');
    
    const values = allData.map(row => parseFloat(row[valueCol])).filter(v => !isNaN(v));
    
    // Create histogram bins
    const bins = 10;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const binSize = (max - min) / bins;
    
    const histogram = new Array(bins).fill(0);
    const labels = [];
    
    for (let i = 0; i < bins; i++) {
        const binStart = min + (i * binSize);
        const binEnd = binStart + binSize;
        labels.push(`${binStart.toFixed(1)}-${binEnd.toFixed(1)}`);
    }
    
    values.forEach(value => {
        const binIndex = Math.min(Math.floor((value - min) / binSize), bins - 1);
        histogram[binIndex]++;
    });
    
    if (distributionChart) {
        distributionChart.destroy();
    }
    
    distributionChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Frequency',
                data: histogram,
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgb(54, 162, 235)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Update data table
function updateTable() {
    if (filteredData.length === 0) return;
    
    const columns = Object.keys(filteredData[0]);
    const thead = document.getElementById('tableHead');
    const tbody = document.getElementById('tableBody');
    
    // Create header
    thead.innerHTML = '<tr>' + columns.map(col => `<th>${col}</th>`).join('') + '</tr>';
    
    // Calculate pagination
    const startIdx = (currentPage - 1) * ROWS_PER_PAGE;
    const endIdx = startIdx + ROWS_PER_PAGE;
    const pageData = filteredData.slice(startIdx, endIdx);
    
    // Create rows
    tbody.innerHTML = pageData.map(row => {
        return '<tr>' + columns.map(col => `<td>${formatCellValue(row[col])}</td>`).join('') + '</tr>';
    }).join('');
    
    // Update pagination info
    const totalPages = Math.ceil(filteredData.length / ROWS_PER_PAGE);
    document.getElementById('pageInfo').textContent = `Page ${currentPage} of ${totalPages}`;
    document.getElementById('prevPage').disabled = currentPage === 1;
    document.getElementById('nextPage').disabled = currentPage === totalPages;
}

// Handle search
function handleSearch(e) {
    const searchTerm = e.target.value.toLowerCase();
    
    if (searchTerm === '') {
        filteredData = [...allData];
    } else {
        filteredData = allData.filter(row => {
            return Object.values(row).some(value => 
                String(value).toLowerCase().includes(searchTerm)
            );
        });
    }
    
    currentPage = 1;
    updateTable();
}

// Handle column filter
function handleColumnFilter(e) {
    const column = e.target.value;
    
    if (column === '') {
        updateCharts();
    } else if (allData.length > 0) {
        const numericColumns = getNumericColumns(allData);
        if (numericColumns.includes(column)) {
            updateDistributionChart(column);
        }
    }
}

// Update column filter dropdown
function updateColumnFilter() {
    if (allData.length === 0) return;
    
    const select = document.getElementById('columnFilter');
    const numericColumns = getNumericColumns(allData);
    
    select.innerHTML = '<option value="">All Columns</option>' +
        numericColumns.map(col => `<option value="${col}">${col}</option>`).join('');
}

// Change page
function changePage(direction) {
    const totalPages = Math.ceil(filteredData.length / ROWS_PER_PAGE);
    currentPage = Math.max(1, Math.min(currentPage + direction, totalPages));
    updateTable();
}

// Utility functions
function getNumericColumns(data) {
    if (data.length === 0) return [];
    const firstRow = data[0];
    return Object.keys(firstRow).filter(key => {
        const value = firstRow[key];
        return !isNaN(parseFloat(value)) && isFinite(value);
    });
}

function getDateColumns(data) {
    if (data.length === 0) return [];
    const firstRow = data[0];
    return Object.keys(firstRow).filter(key => {
        const value = firstRow[key];
        return !isNaN(Date.parse(value));
    });
}

function formatCellValue(value) {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'number') return value.toFixed(2);
    if (!isNaN(Date.parse(value)) && value.includes('-')) {
        return new Date(value).toLocaleString();
    }
    return value;
}

function showLoading(show) {
    document.getElementById('loadingOverlay').style.display = show ? 'flex' : 'none';
}

function showError(message) {
    const errorEl = document.getElementById('errorMessage');
    errorEl.textContent = message;
    errorEl.style.display = 'block';
}

function hideError() {
    document.getElementById('errorMessage').style.display = 'none';
}

function updateLastUpdateTime() {
    const now = new Date();
    document.getElementById('lastUpdate').textContent = 
        `Last updated: ${now.toLocaleTimeString()}`;
}