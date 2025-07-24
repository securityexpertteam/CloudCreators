import React, { useState, useEffect } from "react";
import axios from "axios";
import "./App.css";

function StandardConfigForm() {
  const [cpu, setCpu] = useState("");
  const [memory, setMemory] = useState("");
  const [network, setNetwork] = useState("");
  const [untagged, setUntagged] = useState(false);
  const [orphaned, setOrphaned] = useState(false);
  const [message, setMessage] = useState("");
  const [configData, setConfigData] = useState(null);

  useEffect(() => {
    handleShowData();
  }, []);

  const handleInput = (value, setter) => {
    const num = parseInt(value, 10);
    if (!value) {
      setter(""); // allow clearing
    } else if (!isNaN(num) && num >= 1 && num <= 100) {
      setter(num);
    }
  };

  const handleSubmit = async () => {
    if (
      cpu === "" || memory === "" || network === "" ||
      cpu < 1 || cpu > 100 ||
      memory < 1 || memory > 100 ||
      network < 1 || network > 100
    ) {
      setMessage("❗ CPU, Memory, and Network usage must be between 1 and 100.");
      return;
    }

    try {
      const payload = {
        cpu_usage: parseInt(cpu),
        memory_usage: parseInt(memory),
        network_usage: parseInt(network),
        untagged,
        orphaned,
      };

      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("✅ Standard configuration data updated successfully");
      handleShowData();
    } catch (err) {
      console.error("Submission error:", err);
      setMessage("❌ Failed to submit configuration.");
    }
  };

  const handleShowData = async () => {
    try {
      const res = await axios.get("http://localhost:8000/api/config/latest");
      setConfigData(res.data);
      setCpu(res.data.cpu_usage || "");
      setMemory(res.data.memory_usage || "");
      setNetwork(res.data.network_usage || "");
      setUntagged(res.data.untagged || false);
      setOrphaned(res.data.orphaned || false);
    } catch (err) {
      console.error("Error fetching config data:", err);
      setMessage("❌ Failed to fetch configuration data.");
    }
  };

  const displayValue = (value) => {
    if (value === null || value === undefined || value === "") {
      return "None";
    }
    return typeof value === "boolean" ? (value ? "Yes" : "No") : value;
  };

  return (
    <div className="App">
      <div className="header">
        <div className="logo">🛠️ Config Portal</div>
        <div className="profile">
          <img
            src="https://i.pravatar.cc/36?img=3"
            alt="Profile"
            className="avatar"
          />
        </div>
      </div>

      <h1>Standard Configuration</h1>

      <div className="config-card">
        <div className="input-row">
          <div className="input-field">
            <label>CPU Usage (%)</label>
            <input
              type="number"
              value={cpu}
              onChange={(e) => handleInput(e.target.value, setCpu)}
              min="1"
              max="100"
              placeholder="Enter CPU %"
            />
          </div>
          <div className="input-field">
            <label>Memory Usage (%)</label>
            <input
              type="number"
              value={memory}
              onChange={(e) => handleInput(e.target.value, setMemory)}
              min="1"
              max="100"
              placeholder="Enter Memory %"
            />
          </div>
          <div className="input-field">
            <label>Network Usage (%)</label>
            <input
              type="number"
              value={network}
              onChange={(e) => handleInput(e.target.value, setNetwork)}
              min="1"
              max="100"
              placeholder="Enter Network %"
            />
          </div>
        </div>

        <div className="toggle-options">
          <label>
            <input
              type="checkbox"
              checked={untagged}
              onChange={(e) => setUntagged(e.target.checked)}
            />
            Include Untagged Resources
          </label>

          <label>
            <input
              type="checkbox"
              checked={orphaned}
              onChange={(e) => setOrphaned(e.target.checked)}
            />
            Include Orphaned Resources
          </label>
        </div>

        <div className="button-group">
          <button className="secondary-btn" onClick={handleShowData}>
            Show Data
          </button>
          <button className="primary-btn" onClick={handleSubmit}>
            Update
          </button>
        </div>

        {message && <p>{message}</p>}
      </div>

      <div style={{ width: "100%", maxWidth: "900px", marginTop: "3rem" }}>
        {configData && (
          <table className="config-table">
            <thead>
              <tr>
                <th>CPU (%)</th>
                <th>Memory (%)</th>
                <th>Network (%)</th>
                <th>Untagged</th>
                <th>Orphaned</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{displayValue(configData.cpu_usage)}</td>
                <td>{displayValue(configData.memory_usage)}</td>
                <td>{displayValue(configData.network_usage)}</td>
                <td>{displayValue(configData.untagged)}</td>
                <td>{displayValue(configData.orphaned)}</td>
              </tr>
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

export default StandardConfigForm;
