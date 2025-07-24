
import React, { useEffect, useState } from "react";
import axios from "axios";
import "./Dashboard.css";

function Dashboard() {
  const [resources, setResources] = useState([]);
  const [filteredResources, setFilteredResources] = useState([]);
  const [filters, setFilters] = useState({
    cpu_usage: "",
    memory_usage: "",
    network_usage: "",
    orphaned_vms: "",
  });
  const [showFilter, setShowFilter] = useState(false);

  useEffect(() => {
    axios.get("http://localhost:8000/api/resources")
      .then((res) => {
        setResources(res.data);
        setFilteredResources(res.data);
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
        (filters.cpu_usage === "" || item.cpu_usage <= parseInt(filters.cpu_usage)) &&
        (filters.memory_usage === "" || item.memory_usage <= parseInt(filters.memory_usage)) &&
        (filters.network_usage === "" || item.network_usage <= parseInt(filters.network_usage)) &&
        (filters.orphaned_vms === "" || item.orphaned_vms <= parseInt(filters.orphaned_vms))
      );
    });
    setFilteredResources(filtered);
  };

  const clearFilters = () => {
    setFilters({
      cpu_usage: "",
      memory_usage: "",
      network_usage: "",
      orphaned_vms: "",
    });
    setFilteredResources(resources);
  };

  return (
    <div className="App">
      <div>
        <h1>Cloud Resource Dashboard</h1>
      </div>
      <button className="filter-btn" onClick={() => setShowFilter(!showFilter)}>
          {showFilter ? "Hide Filter" : "Filter"}
        </button>

      {showFilter && (
        <div className="filter-form">
          <input
            type="number"
            name="cpu_usage"
            value={filters.cpu_usage}
            onChange={handleInputChange}
            placeholder="CPU Usage"
          />
          <input
            type="number"
            name="memory_usage"
            value={filters.memory_usage}
            onChange={handleInputChange}
            placeholder="Memory Usage"
          />
          <input
            type="number"
            name="network_usage"
            value={filters.network_usage}
            onChange={handleInputChange}
            placeholder="Network Usage"
          />
          <input
            type="number"
            name="orphaned_vms"
            value={filters.orphaned_vms}
            onChange={handleInputChange}
            placeholder="Orphaned VMs"
          />
          <button onClick={applyFilter}>Apply</button>
          <button onClick={clearFilters}>Clear</button>
        </div>
      )}

      <table>
        <thead>
          <tr>
            <th>Resource ID</th>
            <th>Provider</th>
            <th>Type</th>
            <th>CPU (%)</th>
            <th>Memory (%)</th>
            <th>Network (MB)</th>
            <th>Scale Down</th>
            <th>Untagged</th>
            <th>Orphaned VMs</th>
          </tr>
        </thead>
        <tbody>
          {filteredResources.map((r, i) => (
            <tr key={i}>
              <td>{r.resource_id}</td>
              <td>{r.provider}</td>
              <td>{r.resource_type}</td>
              <td>{r.cpu_usage}</td>
              <td>{r.memory_usage}</td>
              <td>{r.network_usage}</td>
              <td>{r.scale_down_recommendation}</td>
              <td>{r.untagged_instances}</td>
              <td>{r.orphaned_vms}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default Dashboard;




