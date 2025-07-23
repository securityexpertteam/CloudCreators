import React, { useState } from "react";
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

  const handleSubmit = async () => {
    try {
      const payload = {
        cpu_usage: cpu ? parseInt(cpu) : null,
        memory_usage: memory ? parseInt(memory) : null,
        network_usage: network ? parseInt(network) : null,
        untagged,
        orphaned,
      };

      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("Standard configuration data updated successfully");
      setConfigData(null); // Clear previous data if any
    } catch (err) {
      console.error("Submission error:", err);
      setMessage("Failed to submit configuration.");
    }
  };

  const handleShowData = async () => {
    try {
      const res = await axios.get("http://localhost:8000/api/config/latest");
      setConfigData(res.data);
    } catch (err) {
      console.error("Error fetching config data:", err);
      setMessage("Failed to fetch configuration data.");
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
      <h1>Standard Configuration</h1>

      <div className="vertical-form">
        <label>
          CPU (%):
          <input
            type="number"
            min="1"
            max="100"
            value={cpu}
            onChange={(e) => setCpu(e.target.value)}
            placeholder="Enter CPU %"
            required
          />
        </label>

        <label>
          Memory (%):
          <input
            type="number"
            min="1"
            max="100"
            value={memory}
            onChange={(e) => setMemory(e.target.value)}
            placeholder="Enter Memory %"
            required
          />
        </label>

        <label>
          Network (%):
          <input
            type="number"
            min="1"
            max="100"
            value={network}
            onChange={(e) => setNetwork(e.target.value)}
            placeholder="Enter Network %"
            required
          />
        </label>

        <label>
          Untagged:
          <input
            type="checkbox"
            checked={untagged}
            onChange={(e) => setUntagged(e.target.checked)}
          />
        </label>

        <label>
          Orphaned:
          <input
            type="checkbox"
            checked={orphaned}
            onChange={(e) => setOrphaned(e.target.checked)}
          />
        </label>
      </div>

      <div className="bottom-submit">
        <button onClick={handleSubmit}>Update</button>
        <button onClick={handleShowData} style={{ marginLeft: "10px" }}>Show Data</button>
      </div>

      {message && <p>{message}</p>}

      {configData && (
        <table>
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
  );
}

export default StandardConfigForm;
