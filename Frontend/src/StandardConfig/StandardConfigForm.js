import React, { useState, useEffect } from "react";
import axios from "axios";
import "./StandardConfigForm.css";

const CONFIG_TYPES = [
  {
    key: "compute_engine",
    label: "Compute Engine",
    fields: [
      { name: "cpu_usage", label: "CPU Usage (%)", type: "percentage" },
      { name: "memory_usage", label: "Memory Usage (%)", type: "percentage" },
      { name: "network_usage", label: "Network Usage (%)", type: "percentage" },
    ],
  },
  {
    key: "kubernetes",
    label: "Kubernetes",
    fields: [
      { name: "node_cpu_percentage", label: "Node CPU Usage (%)", type: "percentage" },
      { name: "node_memory_percentage", label: "Node Memory Usage (%)", type: "percentage" },
      { name: "node_count", label: "Number of Nodes", type: "number" },
      { name: "volume_percentage", label: "Persistent Volume Usage (%)", type: "percentage" },
    ],
  },
  {
    key: "cloud_storage",
    label: "Cloud Storage",
    fields: [
      { name: "storage_size", label: "Total Storage Size (GB/TB)", type: "number" },
      { name: "access_frequency", label: "Access Frequency", type: "dropdown", options: ["Hot", "Cold"] },
      { name: "network_egress", label: "Network Egress (GB)", type: "number" },
      { name: "lifecycle_enabled", label: "Lifecycle Policies Enabled?", type: "checkbox" },
    ],
  },
  {
    key: "general",
    label: "General Configuration",
    fields: [
      { name: "untagged", label: "Include Untagged Resources", type: "checkbox" },
      { name: "orphaned", label: "Include Orphaned Resources", type: "checkbox" },
    ],
  },
];

function StandardConfigForm() {
  const [selectedType, setSelectedType] = useState(CONFIG_TYPES[0].key);
  const [formValues, setFormValues] = useState({});
  const [message, setMessage] = useState("");
  const [configData, setConfigData] = useState(null);

  const currentTypeObj = CONFIG_TYPES.find((ct) => ct.key === selectedType);

  // Get user email from localStorage with key 'email'
  const email = localStorage.getItem("email");

  useEffect(() => {
    setFormValues({});
    setMessage("");
    fetchLatestConfig();
    // eslint-disable-next-line
  }, [selectedType]);

  const fetchLatestConfig = async () => {
    try {
      // Send both type and email as query params!
      const res = await axios.get(
        `http://localhost:8000/api/config/latest?type=${selectedType}&email=${encodeURIComponent(email)}`
      );
      setConfigData(res.data || {});
      const initialValues = {};
      currentTypeObj.fields.forEach((field) => {
        initialValues[field.name] = res.data?.[field.name] ?? (field.type === "checkbox" ? false : "");
      });
      setFormValues(initialValues);
    } catch (err) {
      setConfigData(null);
      setMessage("❌ Failed to fetch configuration data.");
    }
  };

  const validateInput = (field, value) => {
    if (field.type === "percentage") {
      if (!value) return "";
      const num = parseInt(value, 10);
      if (!isNaN(num) && num >= 1 && num <= 100) return num;
      return formValues[field.name] || "";
    }
    if (field.type === "number") {
      if (!value) return "";
      const num = parseInt(value, 10);
      return !isNaN(num) && num >= 0 ? num : formValues[field.name] || "";
    }
    return value;
  };

  const handleInputChange = (field, value) => {
    setFormValues((prev) => ({
      ...prev,
      [field.name]: validateInput(field, value),
    }));
  };

  const handleSubmit = async () => {
    let invalid = false;
    currentTypeObj.fields.forEach((field) => {
      if (field.type === "percentage") {
        const val = formValues[field.name];
        if (val === "" || val > 100 || val < 1) {
          invalid = true;
        }
      }
    });
    if (invalid) {
      setMessage("❗ Percentage fields must be between 1 and 100.");
      return;
    }

    const payload = { 
      type: selectedType,
      email: email // use lowercase 'email' from localStorage!
    };
    currentTypeObj.fields.forEach((field) => {
      payload[field.name] = formValues[field.name];
    });

    try {
      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("✅ Configuration updated successfully");
      fetchLatestConfig(); // Load fresh config for user/type
    } catch (err) {
      setMessage("❌ Failed to submit configuration.");
    }
  };

  const displayValue = (value, type) => {
    if (value === null || value === undefined || value === "") return "None";
    if (type === "checkbox") return value ? "Yes" : "No";
    return value;
  };

  // Only show toggle frame for Cloud Storage or General Configuration
  const showToggleOptions =
    selectedType === "cloud_storage" || selectedType === "general";

  return (
    <div className="App">
      <h1>Standard Configuration</h1>
      <div className="button-group" style={{ justifyContent: "center", marginBottom: "2rem" }}>
        {CONFIG_TYPES.map((ct) => (
          <button
            key={ct.key}
            className={selectedType === ct.key ? "primary-btn" : "secondary-btn"}
            onClick={() => setSelectedType(ct.key)}
            style={{ minWidth: "170px" }}
          >
            {ct.label}
          </button>
        ))}
      </div>

      <div className="config-card">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleSubmit();
          }}
        >
          <div className="input-row" style={{ flexWrap: "wrap" }}>
            {currentTypeObj.fields.map((field) =>
              field.type === "checkbox" ? null : (
                <div className="input-field" key={field.name}>
                  <label>{field.label}</label>
                  {field.type === "dropdown" ? (
                    <select
                      value={formValues[field.name] || ""}
                      onChange={(e) =>
                        handleInputChange(field, e.target.value)
                      }
                      style={{ padding: "10px 12px", borderRadius: "10px", border: "1px solid #d0d5dd", backgroundColor: "#f9fafb" }}
                    >
                      <option value="">Select</option>
                      {field.options.map((opt) => (
                        <option key={opt} value={opt}>
                          {opt}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type="number"
                      value={formValues[field.name] ?? ""}
                      onChange={(e) =>
                        handleInputChange(field, e.target.value)
                      }
                      min={field.type === "percentage" ? 1 : 0}
                      max={field.type === "percentage" ? 100 : undefined}
                      placeholder={`Enter ${field.label}`}
                    />
                  )}
                </div>
              )
            )}
          </div>

          {showToggleOptions && (
            <div className="toggle-options">
              {currentTypeObj.fields
                .filter((field) => field.type === "checkbox")
                .map((field) => (
                  <label className="toggle-label" key={field.name}>
                    <input
                      type="checkbox"
                      checked={!!formValues[field.name]}
                      onChange={(e) =>
                        handleInputChange(field, e.target.checked)
                      }
                    />
                    <span>{field.label}</span>
                  </label>
                ))}
            </div>
          )}

          <div className="button-group">
            <button
              type="button"
              className="secondary-btn"
              onClick={fetchLatestConfig}
            >
              Show Data
            </button>
            <button type="submit" className="primary-btn">
              Update
            </button>
          </div>
        </form>
        {message && <p>{message}</p>}
      </div>

      {/* Latest config data table */}
      <div style={{ width: "100%", maxWidth: "900px", marginTop: "3rem" }}>
        {configData && (
          <table className="config-table">
            <thead>
              <tr>
                {currentTypeObj.fields.map((field) => (
                  <th key={field.name}>{field.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              <tr>
                {currentTypeObj.fields.map((field) => (
                  <td key={field.name}>
                    {displayValue(configData[field.name], field.type)}
                  </td>
                ))}
              </tr>
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

export default StandardConfigForm;