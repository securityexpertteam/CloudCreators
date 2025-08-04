import React, { useEffect, useState } from "react";
import axios from "axios";
import "./Dashboard.css";
import { Bar, Pie } from "react-chartjs-2";
import { Chart, BarElement, CategoryScale, LinearScale, Tooltip, Legend, ArcElement } from "chart.js";
Chart.register(BarElement, CategoryScale, LinearScale, Tooltip, Legend, ArcElement);

// Utility for CSV download
function downloadCSV(data, filename = 'resources.csv') {
  if (!data || !data.length) return;
  const header = Object.keys(data[0]);
  const csvRows = [header.join(','), ...data.map(row => header.map(field => '"' + (row[field] ?? '') + '"').join(','))];
  const csvContent = csvRows.join('\n');
  const blob = new Blob([csvContent], { type: 'text/csv' });
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.setAttribute('hidden', '');
  a.setAttribute('href', url);
  a.setAttribute('download', filename);
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}
function Dashboard() {
  const [resources, setResources] = useState([]);
  const [filteredResources, setFilteredResources] = useState([]);
  const [cioCostSummary, setCioCostSummary] = useState([]);
  const [entityCostSummary, setEntityCostSummary] = useState([]);
  const [regionCostSummary, setRegionCostSummary] = useState([]);
  const [resourceTypeSummary, setResourceTypeSummary] = useState([]);
  const [filters, setFilters] = useState({
    CIO: "",
    ResourceType: "",
    Region: "",
    TotalCost: "",
  });
  const [cloudProviderSummary, setCloudProviderSummary] = useState([]);
  const [environmentSummary, setEnvironmentSummary] = useState([]);
  const [showFilter, setShowFilter] = useState(false);
  const [loading, setLoading] = useState(true);
  const [costCenterSummary, setCostCenterSummary] = useState([]);
  const [applicationCodeSummary, setApplicationCodeSummary] = useState([]);

 const user = JSON.parse(localStorage.getItem("user") || "{}");

  useEffect(() => {
    setLoading(true);
    axios.get(`http://localhost:8000/api/resources?email=${user.email}`)
      .then((res) => {
        // Sort resources by TotalCost descending for table
        const sortedResources = [...res.data].sort((a, b) => (parseFloat(b.TotalCost) || 0) - (parseFloat(a.TotalCost) || 0));
        setResources(sortedResources);
        setFilteredResources(sortedResources);

        // Aggregate cost by CIO
        const cioSummary = res.data.reduce((acc, curr) => {
          if (!curr.CIO || !curr.TotalCost) return acc;
          acc[curr.CIO] = (acc[curr.CIO] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const cioSummaryArray = Object.entries(cioSummary)
          .map(([cio, cost]) => ({ cio, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setCioCostSummary(cioSummaryArray);

        // Aggregate cost by Entity
        const entitySummary = res.data.reduce((acc, curr) => {
          if (!curr.Entity || !curr.TotalCost) return acc;
          acc[curr.Entity] = (acc[curr.Entity] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        // Sort descending by cost
        const entitySummaryArray = Object.entries(entitySummary)
          .map(([entity, cost]) => ({ entity, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setEntityCostSummary(entitySummaryArray);

        // Aggregate cost by Region
        const regionSummary = res.data.reduce((acc, curr) => {
          if (!curr.Region || !curr.TotalCost) return acc;
          acc[curr.Region] = (acc[curr.Region] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const regionSummaryArray = Object.entries(regionSummary)
          .map(([region, cost]) => ({ region, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setRegionCostSummary(regionSummaryArray);

        // Aggregate cost by Resource Type
        const resourceTypeSummary = res.data.reduce((acc, curr) => {
          if (!curr.ResourceType || !curr.TotalCost) return acc;
          acc[curr.ResourceType] = (acc[curr.ResourceType] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const resourceTypeSummaryArray = Object.entries(resourceTypeSummary)
          .map(([type, cost]) => ({ type, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setResourceTypeSummary(resourceTypeSummaryArray);

        // Aggregate cost by Cloud Provider
        const cloudProviderSummary = res.data.reduce((acc, curr) => {
          if (!curr.CloudProvider || !curr.TotalCost) return acc;
          acc[curr.CloudProvider] = (acc[curr.CloudProvider] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const cloudProviderSummaryArray = Object.entries(cloudProviderSummary)
          .map(([provider, cost]) => ({ provider, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setCloudProviderSummary(cloudProviderSummaryArray);

        // Aggregate cost by Environment
        const environmentSummary = res.data.reduce((acc, curr) => {
          if (!curr.Environment || !curr.TotalCost) return acc;
          acc[curr.Environment] = (acc[curr.Environment] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const environmentSummaryArray = Object.entries(environmentSummary)
          .map(([env, cost]) => ({ env, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setEnvironmentSummary(environmentSummaryArray);

        // Aggregate cost by Cost Center
        const costCenterSummary = res.data.reduce((acc, curr) => {
          if (!curr.CostCenter || !curr.TotalCost) return acc;
          acc[curr.CostCenter] = (acc[curr.CostCenter] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const costCenterSummaryArray = Object.entries(costCenterSummary)
          .map(([costCenter, cost]) => ({ costCenter, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setCostCenterSummary(costCenterSummaryArray);

        // Aggregate cost by Application Code
        const applicationCodeSummary = res.data.reduce((acc, curr) => {
          if (!curr.ApplicationCode || !curr.TotalCost) return acc;
          acc[curr.ApplicationCode] = (acc[curr.ApplicationCode] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const applicationCodeSummaryArray = Object.entries(applicationCodeSummary)
          .map(([appCode, cost]) => ({ applicationCode: appCode, cost: cost }))
          .sort((a, b) => b.cost - a.cost);
        setApplicationCodeSummary(applicationCodeSummaryArray);

        setLoading(false);
      })
      .catch((err) => { console.error(err); setLoading(false); });
  }, [user.email]);

  const handleInputChange = (e) => {
    setFilters({
      ...filters,
      [e.target.name]: e.target.value,
    });
  };

  const applyFilter = () => {
    const filtered = resources.filter((item) => {
      return (
        (filters.CloudProvider === "" || item.CloudProvider === filters.CloudProvider) &&
        (filters.Entity === "" || item.Entity === filters.Entity) &&
        (filters.CIO === "" || item.CIO === filters.CIO) &&
        (filters.ResourceType === "" || item.ResourceType === filters.ResourceType) &&
        (filters.Region === "" || item.Region === filters.Region) &&
        (filters.TotalCost === "" || item.TotalCost <= parseFloat(filters.TotalCost))
      );
    });

    // Update summaries based on filtered resources
    const updatedCloudProviderSummary = filtered.reduce((acc, curr) => {
      if (!curr.CloudProvider || !curr.TotalCost) return acc;
      acc[curr.CloudProvider] = (acc[curr.CloudProvider] || 0) + Number(curr.TotalCost);
      return acc;
    }, {});
    setCloudProviderSummary(Object.entries(updatedCloudProviderSummary).map(([provider, cost]) => ({ provider, cost })));

    const updatedEntitySummary = filtered.reduce((acc, curr) => {
      if (!curr.Entity || !curr.TotalCost) return acc;
      acc[curr.Entity] = (acc[curr.Entity] || 0) + Number(curr.TotalCost);
      return acc;
    }, {});
    setEntityCostSummary(Object.entries(updatedEntitySummary).map(([entity, cost]) => ({ entity, cost })));

    setFilteredResources(filtered.sort((a, b) => (parseFloat(b.TotalCost) || 0) - (parseFloat(a.TotalCost) || 0)));
  };

  const clearFilters = () => {
    setFilters({
      CIO: "",
      ResourceType: "",
      Region: "",
      TotalCost: "",
      CloudProvider: "",
      Entity: "",
    });
    setFilteredResources(resources);

    // Reset summaries to original state
    const originalCloudProviderSummary = resources.reduce((acc, curr) => {
      if (!curr.CloudProvider || !curr.TotalCost) return acc;
      acc[curr.CloudProvider] = (acc[curr.CloudProvider] || 0) + Number(curr.TotalCost);
      return acc;
    }, {});
    setCloudProviderSummary(Object.entries(originalCloudProviderSummary).map(([provider, cost]) => ({ provider, cost })));

    const originalEntitySummary = resources.reduce((acc, curr) => {
      if (!curr.Entity || !curr.TotalCost) return acc;
      acc[curr.Entity] = (acc[curr.Entity] || 0) + Number(curr.TotalCost);
      return acc;
    }, {});
    setEntityCostSummary(Object.entries(originalEntitySummary).map(([entity, cost]) => ({ entity, cost })));

    // Add similar resets for other summaries if needed
  };

  // Card summary data
  const totalCost = resources.reduce((sum, r) => sum + (parseFloat(r.TotalCost) || 0), 0);
  const totalResources = resources.length;
  const uniqueCIOs = new Set(resources.map(r => r.CIO)).size;
  const uniqueCloudProviders = new Set(resources.map(r => r.CloudProvider)).size;
  const uniqueEntities = new Set(resources.map(r => r.Entity)).size;

  // Color palettes
  const palette = [
    '#4287f5', '#f59e42', '#42f5b3', '#f542a7', '#a742f5', '#f5e642', '#42f5e6', '#f54242', '#7a42f5', '#42f57b'
  ];

  const downloadCSV = () => {
    const csvContent = [
      ["Entity", "Cloud Provider", "Environment", "CIO", "Resource Name", "Resource Type", "Region", "Total Cost", "Owner", "Current Size", "Finding", "Recommendation", "Status"],
      ...filteredResources.map(r => [
        r.Entity,
        r.CloudProvider,
        r.Environment,
        r.CIO,
        r.ResourceName,
        r.ResourceType,
        r.Region,
        r.TotalCost,
        r.Owner,
        r.Current_Size,
        r.Finding,
        r.Recommendation,
        r.Status
      ])
    ]
      .map(e => e.join(","))
      .join("\n");

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = "filtered_data.csv";
    link.style.display = "none";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="App finops-dashboard glassy-bg" style={{
      minHeight: '100vh',
      background: 'linear-gradient(120deg, #232526 0%, #414345 100%)',
      paddingBottom: 40,
      color: '#f5f5f7',
      position: 'relative',
      overflow: 'hidden',
    }}>
      {/* Glassy animated background */}
      <div className="glassy-anim-bg" style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: '100vw',
        height: '100vh',
        zIndex: 0,
        pointerEvents: 'none',
        overflow: 'hidden',
      }}>
        <svg width="100%" height="100%" style={{position:'absolute',top:0,left:0}}>
          <defs>
            <radialGradient id="g1" cx="50%" cy="50%" r="80%">
              <stop offset="0%" stopColor="#4fc3f7" stopOpacity="0.18" />
              <stop offset="100%" stopColor="#232526" stopOpacity="0" />
            </radialGradient>
            <radialGradient id="g2" cx="80%" cy="20%" r="60%">
              <stop offset="0%" stopColor="#a742f5" stopOpacity="0.13" />
              <stop offset="100%" stopColor="#232526" stopOpacity="0" />
            </radialGradient>
            <radialGradient id="g3" cx="20%" cy="80%" r="60%">
              <stop offset="0%" stopColor="#42f5b3" stopOpacity="0.13" />
              <stop offset="100%" stopColor="#232526" stopOpacity="0" />
            </radialGradient>
          </defs>
          <circle cx="50%" cy="50%" r="600" fill="url(#g1)">
            <animate attributeName="r" values="600;700;600" dur="8s" repeatCount="indefinite" />
          </circle>
          <circle cx="80%" cy="20%" r="350" fill="url(#g2)">
            <animate attributeName="r" values="350;420;350" dur="10s" repeatCount="indefinite" />
          </circle>
          <circle cx="20%" cy="80%" r="300" fill="url(#g3)">
            <animate attributeName="r" values="300;380;300" dur="12s" repeatCount="indefinite" />
          </circle>
        </svg>
      </div>
      {/* Header */}
      <header style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 24,
        padding: '32px 32px 16px 32px',
        borderBottom: '1px solid #333',
        margin: '0 auto 32px auto',
        background: 'rgba(30,30,35,0.7)',
        backdropFilter: 'blur(12px)',
        boxShadow: '0 4px 32px rgba(0,0,0,0.25)',
        borderRadius: 24,
        maxWidth: 900,
        width: '95%',
        minWidth: 320,
        position: 'relative',
        zIndex: 2,
      }}>
        <div style={{width: 64, height: 64, background: 'rgba(40,40,50,0.8)', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 36, fontWeight: 700, boxShadow: '0 2px 12px #111'}}>💸</div>
        <div style={{display: 'flex', flexDirection: 'column', alignItems: 'flex-start', justifyContent: 'center'}}>
          <h1 style={{margin: 0, fontSize: '2.2rem', letterSpacing: 1, color: '#fff', fontWeight: 800, textShadow: '0 2px 8px #111', lineHeight: 1.15, wordBreak: 'break-word'}}>FinOps Cloud Cost Dashboard</h1>
          <div style={{fontSize: 17, color: '#bbb', marginTop: 6, fontWeight: 500, lineHeight: 1.2}}>Cloud Spend, Utilization & Optimization Insights</div>
        </div>
      </header>

      {/* Card Summaries */}
      <div className="dashboard-cards glassy-cards" style={{
        display: 'flex', justifyContent: 'center', gap: 32, marginBottom: 32, flexWrap: 'wrap',
        zIndex: 1, position: 'relative',
      }}>
        {/* Total Cloud Cost Card */}
        <div className="card glassy-card" style={{
          background: 'rgba(30,30,35,0.55)',
          borderRadius: 18,
          padding: 24,
          minWidth: 180,
          boxShadow: '0 8px 32px 0 rgba(31,38,135,0.37)',
          display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative',
          border: '1.5px solid rgba(255,255,255,0.18)',
          backdropFilter: 'blur(16px)',
          WebkitBackdropFilter: 'blur(16px)',
          transition: 'transform 0.3s cubic-bezier(.4,2,.6,1), box-shadow 0.3s',
        }}
        onMouseEnter={e => e.currentTarget.style.transform = 'scale(1.04)'}
        onMouseLeave={e => e.currentTarget.style.transform = 'scale(1)'}
        >
          <span style={{position: 'absolute', top: 12, right: 12, fontSize: 20, color: '#4fc3f7'}}>💰</span>
          <div style={{fontSize: 14, color: '#bbb'}}>Total Cloud Cost</div>
          <div style={{fontSize: 28, fontWeight: 700, color: '#4fc3f7', marginTop: 4}}>${totalCost.toLocaleString(undefined, {maximumFractionDigits: 2})}</div>
        </div>
        {/* Total Cloud Providers Card */}
        <div className="card" style={{
          background: 'rgba(30,30,35,0.7)',
          borderRadius: 18,
          padding: 24,
          minWidth: 180,
          boxShadow: '0 4px 24px rgba(0,0,0,0.35)',
          display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative',
          border: '1.5px solid #333',
          backdropFilter: 'blur(8px)'
        }}>
          <span style={{position: 'absolute', top: 12, right: 12, fontSize: 20, color: '#42f5b3'}}>☁️</span>
          <div style={{fontSize: 14, color: '#bbb'}}>Total Cloud Providers</div>
          <div style={{fontSize: 28, fontWeight: 700, color: '#42f5b3', marginTop: 4}}>{uniqueCloudProviders}</div>
        </div>
        {/* Unique CIOs Card */}
        <div className="card" style={{
          background: 'rgba(30,30,35,0.7)',
          borderRadius: 18,
          padding: 24,
          minWidth: 180,
          boxShadow: '0 4px 24px rgba(0,0,0,0.35)',
          display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative',
          border: '1.5px solid #333',
          backdropFilter: 'blur(8px)'
        }}>
          <span style={{position: 'absolute', top: 12, right: 12, fontSize: 20, color: '#f59e42'}}>👤</span>
          <div style={{fontSize: 14, color: '#bbb'}}>Unique CIOs</div>
          <div style={{fontSize: 28, fontWeight: 700, color: '#f59e42', marginTop: 4}}>{uniqueCIOs}</div>
        </div>
        {/* Unique Entities Card */}
        <div className="card" style={{
          background: 'rgba(30,30,35,0.7)',
          borderRadius: 18,
          padding: 24,
          minWidth: 180,
          boxShadow: '0 4px 24px rgba(0,0,0,0.35)',
          display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative',
          border: '1.5px solid #333',
          backdropFilter: 'blur(8px)'
        }}>
          <span style={{position: 'absolute', top: 12, right: 12, fontSize: 20, color: '#a742f5'}}>🏢</span>
          <div style={{fontSize: 14, color: '#bbb'}}>Unique Entities</div>
          <div style={{fontSize: 28, fontWeight: 700, color: '#a742f5', marginTop: 4}}>{uniqueEntities}</div>
        </div>
        {/* Total Resources Card */}
        <div className="card" style={{
          background: 'rgba(30,30,35,0.7)',
          borderRadius: 18,
          padding: 24,
          minWidth: 180,
          boxShadow: '0 4px 24px rgba(0,0,0,0.35)',
          display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative',
          border: '1.5px solid #333',
          backdropFilter: 'blur(8px)'
        }}>
          <span style={{position: 'absolute', top: 12, right: 12, fontSize: 20, color: '#81c784'}}>🗂️</span>
          <div style={{fontSize: 14, color: '#bbb'}}>Total Resources</div>
          <div style={{fontSize: 28, fontWeight: 700, color: '#81c784', marginTop: 4}}>{totalResources}</div>
        </div>
      </div>

      {/* Filter Toggle Button */}
      <div style={{display: 'flex', justifyContent: 'center', marginBottom: 12, zIndex: 1, position: 'relative'}}>
        <button
          onClick={() => setShowFilter((v) => !v)}
          style={{
            background: showFilter ? '#4287f5' : '#fff',
            color: showFilter ? '#fff' : '#4287f5',
            border: '1.5px solid #4287f5',
            borderRadius: 8,
            padding: '8px 24px',
            fontWeight: 600,
            fontSize: 15,
            cursor: 'pointer',
            boxShadow: showFilter ? '0 2px 8px #c0d0f0' : 'none',
            transition: 'all 0.2s'
          }}
        >
          {showFilter ? 'Hide Filters' : 'Show Filters'}
        </button>
      </div>

      {/* Filters */}
      {showFilter && (
        <div className="filter-form glassy-filter" style={{
          display: 'flex',
          gap: 16,
          justifyContent: 'center',
          alignItems: 'center',
          marginBottom: 24,
          background: 'rgba(255,255,255,0.18)',
          borderRadius: 12,
          boxShadow: '0 2px 16px #b0b0b0',
          padding: 20,
          backdropFilter: 'blur(8px)',
          WebkitBackdropFilter: 'blur(8px)',
        }}>
          <select
            name="CloudProvider"
            value={filters.CloudProvider}
            onChange={handleInputChange}
            style={{padding: 8, borderRadius: 6, border: '1px solid #ccc', minWidth: 120}}
          >
            <option value="">Select Cloud Provider</option>
            {cloudProviderSummary.map((provider) => (
              <option key={provider.provider} value={provider.provider}>{provider.provider}</option>
            ))}
          </select>
          <select
            name="Entity"
            value={filters.Entity}
            onChange={handleInputChange}
            style={{padding: 8, borderRadius: 6, border: '1px solid #ccc', minWidth: 120}}
          >
            <option value="">Select Entity</option>
            {entityCostSummary.map((entity) => (
              <option key={entity.entity} value={entity.entity}>{entity.entity}</option>
            ))}
          </select>
          <input
            type="text"
            name="CIO"
            value={filters.CIO}
            onChange={handleInputChange}
            placeholder="CIO"
            style={{padding: 8, borderRadius: 6, border: '1px solid #ccc', minWidth: 120}}
          />
          <input
            type="text"
            name="ResourceType"
            value={filters.ResourceType}
            onChange={handleInputChange}
            placeholder="Resource Type"
            style={{padding: 8, borderRadius: 6, border: '1px solid #ccc', minWidth: 120}}
          />
          <input
            type="text"
            name="Region"
            value={filters.Region}
            onChange={handleInputChange}
            placeholder="Region"
            style={{padding: 8, borderRadius: 6, border: '1px solid #ccc', minWidth: 120}}
          />
          <input
            type="number"
            name="TotalCost"
            value={filters.TotalCost}
            onChange={handleInputChange}
            placeholder="Max Total Cost"
            style={{padding: 8, borderRadius: 6, border: '1px solid #ccc', minWidth: 120}}
          />
          <button onClick={applyFilter} style={{background: '#4287f5', color: '#fff', border: 'none', borderRadius: 8, padding: '8px 18px', fontWeight: 600, fontSize: 15, cursor: 'pointer'}}>Apply</button>
          <button onClick={clearFilters} style={{background: '#f5f7fa', color: '#4287f5', border: '1.5px solid #4287f5', borderRadius: 8, padding: '8px 18px', fontWeight: 600, fontSize: 15, cursor: 'pointer'}}>Clear</button>
        </div>
      )}

      {/* Loading Spinner */}
      {loading ? (
        <div style={{display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 300, zIndex: 2, position: 'relative'}}>
          <div style={{border: '6px solid #e0e0e0', borderTop: '6px solid #4287f5', borderRadius: '50%', width: 48, height: 48, animation: 'spin 1s linear infinite', boxShadow: '0 4px 24px #4287f7'}} />
          <style>{`@keyframes spin { 0% { transform: rotate(0deg);} 100% { transform: rotate(360deg);} }`}</style>
        </div>
      ) : (
        <>
          {/* Charts Section */}
          <div className="dashboard-charts glassy-charts" style={{
            display: 'flex', flexWrap: 'wrap', gap: 40, justifyContent: 'center', marginBottom: 40,
            zIndex: 1, position: 'relative',
          }}>
            {/* Cloud Provider-wise Bar Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Cloud Provider-wise Total Cost</h3>
              <Bar
                data={{
                  labels: cloudProviderSummary.map((p) => p.provider),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: cloudProviderSummary.map((p) => p.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* Environment-wise Bar Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Environment-wise Total Cost</h3>
              <Bar
                data={{
                  labels: environmentSummary.map((e) => e.env),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: environmentSummary.map((e) => e.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* Entity-wise Bar Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Entity-wise Total Cost</h3>
              <Bar
                data={{
                  labels: entityCostSummary.map((e) => e.entity),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: entityCostSummary.map((e) => e.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* CIO-wise Bar Chart */}
            <div style={{background: 'rgba(255,255,255,0.85)', borderRadius: 16, boxShadow: '0 8px 32px #a742f5', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center', transition: 'box-shadow 0.3s'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>CIO-wise Cost Distribution</h3>
              <Bar
                data={{
                  labels: cioCostSummary.map((c) => c.cio),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: cioCostSummary.map((c) => c.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* Region-wise Bar Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Region-wise Total Cost</h3>
              <Bar
                data={{
                  labels: regionCostSummary.map((r) => r.region),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: regionCostSummary.map((r) => r.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* Resource Type Pie Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Resource Type Cost Distribution</h3>
              <Pie
                data={{
                  labels: resourceTypeSummary.map((r) => r.type),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: resourceTypeSummary.map((r) => r.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: true, position: 'bottom' },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* Cost Center-wise Bar Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Cost Center-wise Total Cost</h3>
              <Bar
                data={{
                  labels: costCenterSummary.map((c) => c.costCenter),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: costCenterSummary.map((c) => c.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>

            {/* Application Code-wise Bar Chart */}
            <div style={{background: '#fff', borderRadius: 16, boxShadow: '0 4px 16px #e0e0e0', padding: 24, minWidth: 350, maxWidth: 500, height: 300, display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h3 style={{textAlign: 'center', marginBottom: 16, color: '#222'}}>Application Code-wise Total Cost</h3>
              <Bar
                data={{
                  labels: applicationCodeSummary.map((a) => a.applicationCode),
                  datasets: [
                    {
                      label: "Total Cost (USD)",
                      data: applicationCodeSummary.map((a) => a.cost),
                      backgroundColor: palette,
                    },
                  ],
                }}
                options={{
                  plugins: {
                    legend: { display: false },
                    tooltip: { enabled: true },
                    title: { display: false }
                  },
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    y: { beginAtZero: true, grid: { display: false } },
                    x: { grid: { display: false } }
                  },
                  layout: { padding: 10 },
                }}
              />
            </div>
          </div>

          {/* Resource Table */}
          <h2 style={{marginTop: 32, textAlign: 'center', color: '#fff', letterSpacing: 1, fontWeight: 700, textShadow: '0 2px 8px #111'}}>Resource Details</h2>
          <div style={{overflowX: 'auto', margin: '0 auto', maxWidth: 1400, zIndex: 1, position: 'relative'}}>
            <table className="resource-table glassy-table" style={{
              width: '100vw',
              minWidth: 900,
              maxWidth: '100%',
              tableLayout: 'auto',
              borderCollapse: 'separate',
              borderSpacing: 0,
              background: 'rgba(255,255,255,0.13)',
              borderRadius: 18,
              boxShadow: '0 8px 32px 0 rgba(31,38,135,0.37)',
              fontSize: 15,
              margin: '0 auto',
              color: '#111',
              overflow: 'hidden',
              backdropFilter: 'blur(12px)',
              WebkitBackdropFilter: 'blur(12px)',
              border: '1.5px solid rgba(255,255,255,0.18)',
              transition: 'box-shadow 0.3s',
            }}>
              <thead style={{background: 'rgba(40,40,50,0.85)', color: '#111', fontWeight: 700, letterSpacing: 1}}>
                <tr>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Entity</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Cloud Provider</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Environment</th> 
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>CIO</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Resource Name</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Resource Type</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Region</th>
                 
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Total Cost</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Owner</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Current Size</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Finding</th>
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Recommendation</th>
                  
                  <th style={{padding: 12, borderBottom: '2px solid #333'}}>Status</th>
                </tr>
              </thead>
              <tbody>
                {filteredResources.map((r, i) => (
                  <tr
                    key={i}
                    style={{
                      background: 'rgba(255,255,255,0.85)',
                      transition: 'background 0.2s, box-shadow 0.2s',
                      cursor: 'pointer',
                      color: '#111',
                      borderBottom: '1px solid #333',
                      boxShadow: '0 2px 8px #e0e0e0',
                    }}
                    onMouseEnter={e => e.currentTarget.style.background = 'rgba(66,135,245,0.13)'}
                    onMouseLeave={e => e.currentTarget.style.background = 'rgba(255,255,255,0.85)'}
                  >
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111'}}>{r.Entity}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.CloudProvider}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Environment}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.CIO}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.ResourceName}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.ResourceType}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Region}</td>

                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>${parseFloat(r.TotalCost).toLocaleString(undefined, {maximumFractionDigits: 2})}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Owner}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Current_Size}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Finding}</td>
                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Recommendation}</td>

                    <td style={{padding: 10, borderBottom: '1px solid #333', color: '#111', wordBreak: 'break-word', whiteSpace: 'pre-wrap'}}>{r.Status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Download CSV Button */}
          <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 20 }}>
            <button
              onClick={downloadCSV}
              style={{
                background: '#4287f5',
                color: '#fff',
                border: 'none',
                borderRadius: 8,
                padding: '10px 20px',
                fontWeight: 600,
                fontSize: 15,
                cursor: 'pointer',
                boxShadow: '0 2px 8px rgba(0, 0, 0, 0.2)',
                transition: 'all 0.2s',
              }}
            >
              Download CSV
            </button>
          </div>
        </>
      )}

      {/* Footer removed as requested */}
    </div>
  );
}

export default Dashboard;

// Stunning Glassy/Neumorphic CSS for extra polish
const style = document.createElement('style');
style.innerHTML = `
body, .App {
  background: linear-gradient(135deg, #232526 0%, #414345 100%) !important;
}
.glassy-bg, .glassy-cards, .glassy-card, .glassy-filter, .glassy-charts, .glassy-table {
  animation: glassyFadeIn 1.2s cubic-bezier(.4,2,.6,1);
  box-shadow: 0 8px 32px 0 rgba(31,38,135,0.25), 0 1.5px 8px 0 rgba(0,0,0,0.10);
  border-radius: 24px;
  backdrop-filter: blur(18px) saturate(1.2);
  -webkit-backdrop-filter: blur(18px) saturate(1.2);
}
.glassy-card, .card {
  transition: transform 0.35s cubic-bezier(.4,2,.6,1), box-shadow 0.35s;
  box-shadow: 0 8px 32px 0 rgba(31,38,135,0.25), 0 1.5px 8px 0 rgba(0,0,0,0.10);
  border-radius: 22px;
  background: rgba(255,255,255,0.18) !important;
  border: 1.5px solid rgba(255,255,255,0.22) !important;
}
.glassy-card:hover, .card:hover {
  transform: translateY(-8px) scale(1.045) rotate(-1deg);
  box-shadow: 0 16px 48px 0 rgba(66,135,245,0.18), 0 2px 16px 0 rgba(167,66,245,0.10);
  z-index: 3;
}
.dashboard-cards {
  gap: 40px !important;
}
.dashboard-cards .card {
  min-width: 200px !important;
  font-size: 1.1rem;
}
.resource-table {
  border-radius: 22px !important;
  overflow: hidden;
  box-shadow: 0 8px 32px 0 rgba(31,38,135,0.18), 0 1.5px 8px 0 rgba(0,0,0,0.10);
  background: rgba(255,255,255,0.18) !important;
}
.resource-table thead tr {
  background: linear-gradient(90deg, #4287f5 0%, #a742f5 100%) !important;
  color: #fff !important;
  text-shadow: 0 2px 8px #23252644;
}
.resource-table tbody tr {
  transition: background 0.22s, box-shadow 0.22s;
  background: rgba(255,255,255,0.92) !important;
}
.resource-table tbody tr:hover {
  background: linear-gradient(90deg, #e3f0ff 0%, #f3e3ff 100%) !important;
  box-shadow: 0 4px 16px #a742f522;
}
.resource-table td, .resource-table th {
  border-bottom: 1.5px solid #e0e0e0 !important;
  padding: 12px 10px !important;
}
.filter-form {
  box-shadow: 0 2px 16px #b0b0b0;
  background: rgba(255,255,255,0.22) !important;
  border-radius: 16px !important;
}
.filter-form input, .filter-form button {
  font-size: 1rem;
  border-radius: 8px !important;
  border: 1.5px solid #4287f5 !important;
  background: rgba(255,255,255,0.85) !important;
  margin-right: 2px;
  transition: box-shadow 0.2s, border 0.2s;
}
.filter-form input:focus {
  outline: none;
  border: 2px solid #a742f5 !important;
  box-shadow: 0 0 0 2px #a742f555;
}
.filter-form button {
  background: linear-gradient(90deg, #4287f5 0%, #a742f5 100%) !important;
  color: #fff !important;
  font-weight: 700;
  border: none !important;
  box-shadow: 0 2px 8px #a742f522;
  margin-left: 2px;
}
.filter-form button:hover {
  background: linear-gradient(90deg, #42f5b3 0%, #4287f5 100%) !important;
  color: #232526 !important;
  box-shadow: 0 4px 16px #42f5b344;
}
.dashboard-charts > div {
  box-shadow: 0 8px 32px 0 rgba(31,38,135,0.13), 0 1.5px 8px 0 rgba(167,66,245,0.10);
  border-radius: 22px !important;
  background: rgba(255,255,255,0.96) !important;
  transition: box-shadow 0.3s, transform 0.3s;
}
.dashboard-charts > div:hover {
  box-shadow: 0 16px 48px 0 rgba(66,135,245,0.18), 0 2px 16px 0 rgba(167,66,245,0.10);
  transform: translateY(-6px) scale(1.025);
  z-index: 2;
}
@keyframes glassyFadeIn {
  0% { opacity: 0; transform: translateY(40px) scale(0.98); }
  100% { opacity: 1; transform: translateY(0) scale(1); }
}
/* Animated floating gradient background */
.glassy-anim-bg {
  position: fixed !important;
  top: 0; left: 0; width: 100vw; height: 100vh;
  z-index: 0;
  pointer-events: none;
  overflow: hidden;
  background: linear-gradient(120deg, #232526 0%, #414345 100%);
}
.glassy-anim-bg svg {
  filter: blur(8px) saturate(1.2);
  opacity: 0.95;
}
::-webkit-scrollbar {
  width: 10px;
  background: #232526;
}
::-webkit-scrollbar-thumb {
  background: linear-gradient(120deg, #4287f5 0%, #a742f5 100%);
  border-radius: 8px;
}
`;
if (!document.head.querySelector('style[data-glassy]')) {
  style.setAttribute('data-glassy', '');
  document.head.appendChild(style);
}




