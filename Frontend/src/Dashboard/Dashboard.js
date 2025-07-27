import React, { useEffect, useState } from "react";
import axios from "axios";
import "./Dashboard.css";
import { Bar } from "react-chartjs-2";
import { Chart, BarElement, CategoryScale, LinearScale, Tooltip, Legend } from "chart.js";
Chart.register(BarElement, CategoryScale, LinearScale, Tooltip, Legend);

function Dashboard() {
  const [resources, setResources] = useState([]);
  const [filteredResources, setFilteredResources] = useState([]);
  const [cioCostSummary, setCioCostSummary] = useState([]);
  const [entityCostSummary, setEntityCostSummary] = useState([]);
  const [filters, setFilters] = useState({
    CIO: "",
    ResourceType: "",
    Region: "",
    TotalCost: "",
  });
  const [showFilter] = useState(false);

  useEffect(() => {
    axios.get("http://localhost:8000/api/resources")
      .then((res) => {
        setResources(res.data);
        setFilteredResources(res.data);

        // Aggregate cost by CIO
        const cioSummary = res.data.reduce((acc, curr) => {
          if (!curr.CIO || !curr.TotalCost) return acc;
          acc[curr.CIO] = (acc[curr.CIO] || 0) + Number(curr.TotalCost);
          return acc;
        }, {});
        const cioSummaryArray = Object.entries(cioSummary).map(([cio, cost]) => ({
          cio,
          cost: cost.toFixed(6),
        }));
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
      })
      .catch((err) => console.error(err));
  }, []);

  const handleInputChange = (e) => {
    setFilters({
      ...filters,
      [e.target.name]: e.target.value,
    });
  };

  const applyFilter = () => {
    const filtered = resources.filter((item) => {
      return (
        (filters.CIO === "" || item.CIO === filters.CIO) &&
        (filters.ResourceType === "" || item.ResourceType === filters.ResourceType) &&
        (filters.Region === "" || item.Region === filters.Region) &&
        (filters.TotalCost === "" || item.TotalCost <= parseFloat(filters.TotalCost))
      );
    });
    setFilteredResources(filtered);
  };

  const clearFilters = () => {
    setFilters({
      CIO: "",
      ResourceType: "",
      Region: "",
      TotalCost: "",
    });
    setFilteredResources(resources);
  };

  return (
    <div className="App">
      {showFilter && (
        <div className="filter-form">
          <input
            type="text"
            name="CIO"
            value={filters.CIO}
            onChange={handleInputChange}
            placeholder="CIO"
          />
          <input
            type="text"
            name="ResourceType"
            value={filters.ResourceType}
            onChange={handleInputChange}
            placeholder="Resource Type"
          />
          <input
            type="text"
            name="Region"
            value={filters.Region}
            onChange={handleInputChange}
            placeholder="Region"
          />
          <input
            type="number"
            name="TotalCost"
            value={filters.TotalCost}
            onChange={handleInputChange}
            placeholder="Max Total Cost"
          />
          <button onClick={applyFilter}>Apply</button>
          <button onClick={clearFilters}>Clear</button>
        </div>
      )}

      <h2>CIO Operational Cost Summary</h2>
      <table>
        <thead>
          <tr>
            <th>CIO</th>
            <th>Total Cost (USD)</th>
          </tr>
        </thead>
        <tbody>
          {cioCostSummary.map((row, idx) => (
            <tr key={idx}>
              <td>{row.cio}</td>
              <td>{row.cost}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h2>Entity-wise Total Cost (Descending)</h2>
      <div style={{ maxWidth: "600px", margin: "0 auto" }}>
        <Bar
          data={{
            labels: entityCostSummary.map((e) => e.entity),
            datasets: [
              {
                label: "Total Cost (USD)",
                data: entityCostSummary.map((e) => e.cost),
                backgroundColor: "#4287f5",
              },
            ],
          }}
          options={{
            plugins: {
              legend: { display: false },
              tooltip: { enabled: true },
            },
            scales: {
              y: { beginAtZero: true },
            },
          }}
        />
      </div>

      <h2>Resource Details</h2>
      <table>
        <thead>
          <tr>
            <th>Resource Name</th>
            <th>CIO</th>
            <th>Resource Type</th>
            <th>Region</th>
            <th>Total Cost</th>
            <th>Owner</th>
       
            <th>Current Size</th>
            <th>Finding</th>
            <th>Recommendation</th>
            <th>Entity</th>
            <th>Status</th>
            {/* Add more columns as needed */}
          </tr>
        </thead>
        <tbody>
          {filteredResources.map((r, i) => (
            <tr key={i}>
              <td>{r.ResourceName}</td>
              <td>{r.CIO}</td>
              <td>{r.ResourceType}</td>
              <td>{r.Region}</td>
              <td>{r.TotalCost}</td>
              <td>{r.Owner}</td>
              <td>{r.Current_Size}</td>
              <td>{r.Finding}</td>
              <td>{r.Recommendation}</td>
              <td>{r.Entity}</td>
              <td>{r.Status}</td>
              {/* Add more fields as needed */}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default Dashboard;




