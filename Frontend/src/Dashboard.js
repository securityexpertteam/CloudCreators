import React, { useEffect, useState } from "react";
import axios from "axios";
import "./App.css";

function Dashboard() {
  const [resources, setResources] = useState([]);
  const [configs, setConfigs] = useState([]);
  const [cpu, setCpu] = useState("");
  const [memory, setMemory] = useState("");
  const [network, setNetwork] = useState("");
  const [untagged, setUntagged] = useState(false);
  const [orphaned, setOrphaned] = useState(false);
  const [message, setMessage] = useState("");
  const [activeTab, setActiveTab] = useState("resources");

  useEffect(() => {
    axios.get("http://localhost:8000/api/resources")
      .then(res => setResources(res.data))
      .catch(err => console.error(err));

    axios.get("http://localhost:8000/api/configs")
      .then(res => setConfigs(res.data))
      .catch(err => console.error(err));
  }, [message]);

  const handleSubmit = async () => {
    const payload = {
      cpu_usage: cpu ? parseInt(cpu) : null,
      memory_usage: memory ? parseInt(memory) : null,
      network_usage: network ? parseInt(network) : null,
      untagged: untagged,
      orphaned: orphaned
    };

    try {
      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("Standard configuration data updated successfully");
      setCpu("");
      setMemory("");
      setNetwork("");
      setUntagged(false);
      setOrphaned(false);
      setActiveTab("configs");
    } catch (err) {
      console.error(err);
      setMessage("Error submitting configuration");
    }
  };

  return (
    <div className="App">
      <h1>Cloud Resource Dashboard</h1>
      <div>
        <button onClick={() => setActiveTab("resources")}>Resource Table</button>
        <button onClick={() => setActiveTab("configs")}>Standard Configs</button>
      </div>

      {activeTab === "resources" && (
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
            {resources.map((r, i) => (
              <tr key={i}>
                <td>{r.resource_id}</td>
                <td>{r.provider}</td>
                <td>{r.resource_type}</td>
                <td>{r.cpu_usage}%</td>
                <td>{r.memory_usage}%</td>
                <td>{r.network_usage}</td>
                <td>{r.scale_down_recommendation}</td>
                <td>{r.untagged_instances}</td>
                <td>{r.orphaned_vms}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {activeTab === "configs" && (
        <div>
          <h2>Submit Standard Configuration</h2>
          <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem', justifyContent: 'center' }}>
            <div>
              <label>CPU:</label><br />
              <input
                type="number"
                value={cpu}
                min="1"
                max="100"
                onChange={(e) => setCpu(e.target.value)}
                placeholder="%"
              />
            </div>
            <div>
              <label>Memory:</label><br />
              <input
                type="number"
                value={memory}
                min="1"
                max="100"
                onChange={(e) => setMemory(e.target.value)}
                placeholder="%"
              />
            </div>
            <div>
              <label>Network:</label><br />
              <input
                type="number"
                value={network}
                min="1"
                max="100"
                onChange={(e) => setNetwork(e.target.value)}
                placeholder="%"
              />
            </div>
            <div>
              <label>Untagged:</label><br />
              <input
                type="checkbox"
                checked={untagged}
                onChange={() => setUntagged(!untagged)}
              />
            </div>
            <div>
              <label>Orphaned:</label><br />
              <input
                type="checkbox"
                checked={orphaned}
                onChange={() => setOrphaned(!orphaned)}
              />
            </div>
          </div>
          <button onClick={handleSubmit}>Submit</button>
          {message && <p>{message}</p>}

          <h2>Submitted Configs</h2>
          <table>
            <thead>
              <tr>
                <th>CPU</th>
                <th>Memory</th>
                <th>Network</th>
                <th>Untagged</th>
                <th>Orphaned</th>
              </tr>
            </thead>
            <tbody>
              {configs.map((c, i) => (
                <tr key={i}>
                  <td>{c.cpu_usage ?? "None"}</td>
                  <td>{c.memory_usage ?? "None"}</td>
                  <td>{c.network_usage ?? "None"}</td>
                  <td>{c.untagged ? "Yes" : "No"}</td>
                  <td>{c.orphaned ? "Yes" : "No"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default Dashboard;
