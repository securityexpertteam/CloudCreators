import React, { useEffect, useState } from "react";
import axios from "axios";
import { useNavigate } from "react-router-dom"; // ✅ import navigate
import "./App.css";

function StandardConfigSummary() {
  const [data, setData] = useState(null);
  const navigate = useNavigate(); // ✅ for navigation

  useEffect(() => {
    axios
      .get("http://localhost:8000/api/config/latest")
      .then((res) => setData(res.data))
      .catch((err) => console.error("Failed to fetch config summary:", err));
  }, []);

  const displayValue = (value) => {
    if (value === null || value === undefined || value === "") {
      return "None";
    }
    return typeof value === "boolean" ? (value ? "Yes" : "No") : value;
  };

  return (
    <div className="App">
      <h1>Standard Configuration Summary</h1>
      {data ? (
        <table>
          <tbody>
            <tr>
              <th>CPU (%)</th>
              <td>{displayValue(data.cpu_usage)}</td>
            </tr>
            <tr>
              <th>Memory (%)</th>
              <td>{displayValue(data.memory_usage)}</td>
            </tr>
            <tr>
              <th>Network (%)</th>
              <td>{displayValue(data.network_usage)}</td>
            </tr>
            <tr>
              <th>Untagged</th>
              <td>{displayValue(data.untagged)}</td>
            </tr>
            <tr>
              <th>Orphaned</th>
              <td>{displayValue(data.orphaned)}</td>
            </tr>
          </tbody>
        </table>
      ) : (
        <p>Loading latest configuration...</p>
      )}

      {/* ✅ Back button */}
      <button style={{ marginTop: "1rem" }} onClick={() => navigate("/config")}>
        ← Back to Config Form
      </button>
    </div>
  );
}

export default StandardConfigSummary;
