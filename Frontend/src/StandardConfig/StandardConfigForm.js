import React, { useEffect, useState } from "react";
import axios from "axios";
import './StandardConfigForm.css';

const CONFIG_TYPES = [
  {
    key: "general",
    label: "General Configuration",
    fields: [
      { name: "gen_untagged", label: "Include Untagged Resources", type: "checkbox" },
      { name: "gen_orphaned", label: "Include Orphaned Resources", type: "checkbox" },
    ],
  },
  {
    key: "compute_engine",
    label: "Compute Engine",
    fields: [
      { name: "cmp_cpu_usage", label: "CPU Usage (%)", type: "percentage" },
      { name: "cmp_memory_usage", label: "Memory Usage (%)", type: "percentage" },
      { name: "cmp_network_usage", label: "Network Usage (%)", type: "percentage" },
    ],
  },
  {
    key: "kubernetes",
    label: "Kubernetes",
    fields: [
      { name: "k8s_node_cpu_percentage", label: "Node CPU Usage (%)", type: "percentage" },
      { name: "k8s_node_memory_percentage", label:"Node Memory Usage(%)", type: "percentage" },
      { name: "k8s_node_count", label: "Number of Nodes", type: "number" },
      { name: "k8s_volume_percentage", label:"Persistent Volume Usage (%)", type: "percentage" },
    ],
  },
  {
    key: "cloud_storage",
    label: "Cloud Storage",
    fields: [
      { name: "stor_size", label: "Total Storage Size (GB/TB)", type: "number" },
      { name: "stor_access_frequency", label: "Access Frequency", type: "dropdown", options: ["Hot", "Cold"] },
      { name: "stor_nw_egress", label: "Network Egress (GB)", type: "number" },
      { name: "stor_lifecycle_enabled", label: "Lifecycle Policies Enabled?", type: "checkbox" },
    ],
  },
  {
    key: "database",
    label: "Database",
    fields: [
      { name: "db_size", label: "Database Size", type: "number" },
      { name: "dtu_percentage", label: "DTU (%)", type: "percentage" }
    ],
  },
];

function StandardConfigForm() {
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const email = user.email || "";

  // Store all panes' values
  const [allFormValues, setAllFormValues] = useState({});
  const [selectedType, setSelectedType] = useState("");
  const [loadedConfig, setLoadedConfig] = useState({});
  const [message, setMessage] = useState("");
  const [showConfig, setShowConfig] = useState(false);

  // On mount, load config and initialize allFormValues
  useEffect(() => {
    setMessage("");
    const fetchConfig = async () => {
      try {
        const res = await axios.get(
          `http://localhost:8000/api/config/latest?email=${encodeURIComponent(email)}`
        );
        const config = res.data || {};
        setLoadedConfig(config);

        // Initialize allFormValues with loaded config or empty (checkboxes default to false)
        let initial = {};
        CONFIG_TYPES.forEach(type => {
          type.fields.forEach(field => {
            if (field.type === "checkbox") {
              initial[field.name] = config[field.name] === true;
            } else {
              initial[field.name] = config[field.name] !== undefined ? config[field.name] : "";
            }
          });
        });
        setAllFormValues(initial);

        // Auto-select the first pane with config, or default to first pane
        const firstPaneWithConfig = CONFIG_TYPES.find(type =>
          type.fields.some(field =>
            config[field.name] !== undefined &&
            config[field.name] !== "" &&
            config[field.name] !== false
          )
        );
        setSelectedType(firstPaneWithConfig ? firstPaneWithConfig.key : CONFIG_TYPES[0].key);
      } catch {
        // On error, initialize allFormValues as empty (checkboxes default to false)
        let initial = {};
        CONFIG_TYPES.forEach(type => {
          type.fields.forEach(field => {
            initial[field.name] = field.type === "checkbox" ? false : "";
          });
        });
        setAllFormValues(initial);
        setLoadedConfig({});
        setSelectedType(CONFIG_TYPES[0].key);
      }
    };
    if (email) fetchConfig();
  }, [email]);

  // When switching panes, keep allFormValues, just change visible pane
  const handleTypeChange = (typeKey) => {
    setSelectedType(typeKey);
  };

  // Update only the field in allFormValues for the current pane
  const handleInputChange = (fieldName, value, fieldType) => {
    setAllFormValues((prev) => ({
      ...prev,
      [fieldName]: fieldType === "checkbox" ? !!value : value,
    }));
  };

  // Save all panes' data at once, set null for untouched fields
  const handleSubmit = async (e) => {
    e.preventDefault();
    setMessage("");
    let hasError = false;

    // Validate all percentage fields in all panes
    for (const typeObj of CONFIG_TYPES) {
      for (const field of typeObj.fields) {
        if (field.type === "percentage") {
          const val = Number(allFormValues[field.name]);
          if (
            allFormValues[field.name] !== "" && // Only validate if user entered something
            (isNaN(val) || val < 1 || val > 100)
          ) {
            setMessage(
              `❗ ${field.label} in ${typeObj.label} must be between 1 and 100.`
            );
            hasError = true;
            break;
          }
        }
      }
      if (hasError) break;
    }
    if (hasError) return;

    // Prepare payload: all fields, set to null if empty string
    let payload = { email };
    CONFIG_TYPES.forEach(type => {
      type.fields.forEach(field => {
        let val = allFormValues[field.name];
        payload[field.name] = val === "" ? null : val;
      });
    });

    try {
      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("✅ Configuration saved successfully!");
      // Reload config from backend
      const res = await axios.get(
        `http://localhost:8000/api/config/latest?email=${encodeURIComponent(email)}`
      );
      const config = res.data || {};
      setLoadedConfig(config);

      // After save, update allFormValues with the latest config from backend
      let updated = {};
      CONFIG_TYPES.forEach(type => {
        type.fields.forEach(field => {
          if (field.type === "checkbox") {
            updated[field.name] = config[field.name] === true;
          } else {
            updated[field.name] = config[field.name] !== undefined ? config[field.name] : "";
          }
        });
      });
      setAllFormValues(updated);
    } catch (err) {
      setMessage("❌ Failed to save configuration.");
    }
  };

  // Show configuration table (each config type in a separate table)
  const renderConfigTables = () => {
    if (!loadedConfig || Object.keys(loadedConfig).length === 0) {
      return <div style={{ marginTop: "16px" }}>No previous configuration found.</div>;
    }
    return (
      <div style={{ marginTop: "32px" }}>
        <h3 style={{ marginBottom: "18px" }}>Previous Configuration</h3>
        {CONFIG_TYPES.map(type => (
          <div
            key={type.key}
            style={{
              width: "100%",
              marginBottom: "28px",
              background: "#fff",
              borderRadius: "8px",
              boxShadow: "0 2px 8px rgba(0,0,0,0.04)",
              padding: "18px 18px 12px 18px"
            }}
          >
            <div style={{ fontWeight: 700, fontSize: "1.08em", marginBottom: "10px", color: "#232526" }}>
              {type.label}
            </div>
            <table className="config-table" style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: "6px", borderBottom: "1px solid #ccc" }}>Field</th>
                  <th style={{ textAlign: "left", padding: "6px", borderBottom: "1px solid #ccc" }}>Value</th>
                </tr>
              </thead>
              <tbody>
                {type.fields.map(field => (
                  <tr key={field.name}>
                    <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>{field.label}</td>
                    <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>
                      {typeof loadedConfig[field.name] === "boolean"
                        ? loadedConfig[field.name] ? "Yes" : "No"
                        : loadedConfig[field.name] !== undefined && loadedConfig[field.name] !== null && loadedConfig[field.name] !== ""
                          ? loadedConfig[field.name]
                          : <span style={{ color: "#aaa" }}>-</span>
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ))}
      </div>
    );
  };

  return (
    <div className="signin-bg">
      <div className="signin-center">
        <div className="signin-card">
          <div className="signin-card-content">
            <h2 className="signin-title">Standard Configuration</h2>
            <form onSubmit={handleSubmit}>
              <div className="signin-section">
                <label>
                  <b>Select Configuration Type:</b>
                </label>
                <div style={{ height: 16 }} />
                <div
                  className="signin-type-buttons"
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: "10px",
                    justifyContent: "flex-start",
                    marginBottom: "10px"
                  }}
                >
                  {CONFIG_TYPES.map((type) => (
                    <button
                      key={type.key}
                      type="button"
                      className={`signin-type-btn${selectedType === type.key ? " selected" : ""}`}
                      onClick={() => handleTypeChange(type.key)}
                      style={{
                        marginRight: 0,
                        marginBottom: 0,
                        padding: "8px 18px",
                        borderRadius: 8,
                        border: selectedType === type.key ? "2px solid #4287f5" : "1.5px solid #bbb",
                        background: selectedType === type.key
                          ? "linear-gradient(90deg, #4287f5 0%, #a742f5 100%)"
                          : "#fff",
                        color: selectedType === type.key ? "#fff" : "#232526",
                        fontWeight: 600,
                        cursor: "pointer",
                        transition: "all 0.2s"
                      }}
                    >
                      {type.label}
                    </button>
                  ))}
                </div>
              </div>
              <hr />
              {selectedType && (() => {
                const typeObj = CONFIG_TYPES.find((t) => t.key === selectedType);
                return (
                  <div key={selectedType} className="signin-config-section">
                    <div className="signin-fields-grid">
                      {typeObj.fields.map((field) => {
                        const isSpecialCheckbox =
                          ["stor_lifecycle_enabled", "gen_untagged", "gen_orphaned"].includes(field.name);

                        return (
                          <div className="signin-field" key={field.name}>
                            {field.type === "checkbox" && isSpecialCheckbox ? (
                              <>
                                <input
                                  type="checkbox"
                                  checked={allFormValues[field.name] === true}
                                  onChange={(e) =>
                                    handleInputChange(field.name, e.target.checked, field.type)
                                  }
                                  id={field.name}
                                  className="signin-checkbox"
                                  style={{ marginRight: "8px" }}
                                />
                                <label htmlFor={field.name} className="signin-field-label" style={{ marginBottom: 0 }}>
                                  {field.label}
                                </label>
                              </>
                            ) : (
                              <>
                                <label htmlFor={field.name} className="signin-field-label">
                                  {field.label}
                                </label>
                                {field.type === "percentage" ? (
                                  <>
                                    <input
                                      type="text"
                                      inputMode="numeric"
                                      pattern="[0-9]*"
                                      value={allFormValues[field.name] !== undefined ? allFormValues[field.name] : ""}
                                      placeholder={
                                        loadedConfig[field.name] !== undefined && loadedConfig[field.name] !== ""
                                          ? loadedConfig[field.name]
                                          : ""
                                      }
                                      min={field.type === "percentage" ? 1 : undefined}
                                      max={field.type === "percentage" ? 100 : undefined}
                                      onChange={(e) => {
                                        let val = e.target.value.replace(/[^0-9]/g, "");
                                        if (field.type === "percentage") {
                                          if (val.length > 3) val = val.slice(0, 3);
                                          if (val !== "" && Number(val) > 100) val = val.slice(0, val.length - 1);
                                        }
                                        setAllFormValues((prev) => ({
                                          ...prev,
                                          [field.name]: val,
                                        }));
                                      }}
                                      onKeyDown={(e) => {
                                        if (
                                          ["e", "E", "+", "-", ".", ",", " "].includes(e.key) ||
                                          (e.key.length === 1 && e.key.match(/[a-zA-Z]/))
                                        ) {
                                          e.preventDefault();
                                        }
                                        if (
                                          field.type === "percentage" &&
                                          e.target.value.length >= 3 &&
                                          !["Backspace", "Delete", "ArrowLeft", "ArrowRight", "Tab"].includes(e.key)
                                        ) {
                                          e.preventDefault();
                                        }
                                      }}
                                      className="signin-input"
                                    />
                                  </>
                                ) : field.type === "checkbox" ? (
                                  <input
                                    type="checkbox"
                                    checked={allFormValues[field.name] === true}
                                    onChange={(e) =>
                                      handleInputChange(field.name, e.target.checked, field.type)
                                    }
                                    id={field.name}
                                    className="signin-checkbox"
                                  />
                                ) : field.type === "dropdown" ? (
                                  <select
                                    value={allFormValues[field.name] !== undefined ? allFormValues[field.name] : ""}
                                    onChange={(e) =>
                                      handleInputChange(field.name, e.target.value, field.type)
                                    }
                                    className="signin-select"
                                  >
                                    <option value="">
                                      {loadedConfig[field.name] !== undefined && loadedConfig[field.name] !== ""
                                        ? `Previous: ${loadedConfig[field.name]}`
                                        : "Select"}
                                    </option>
                                    {field.options.map((opt) => (
                                      <option key={opt} value={opt}>
                                        {opt}
                                      </option>
                                    ))}
                                  </select>
                                ) : (
                                  <input
                                    type="text"
                                    inputMode="numeric"
                                    pattern="[0-9]*"
                                    value={allFormValues[field.name] !== undefined ? allFormValues[field.name] : ""}
                                    placeholder={
                                      loadedConfig[field.name] !== undefined && loadedConfig[field.name] !== ""
                                        ? loadedConfig[field.name]
                                        : ""
                                    }
                                    min={field.type === "percentage" ? 1 : undefined}
                                    max={field.type === "percentage" ? 100 : undefined}
                                    onChange={(e) => {
                                      let val = e.target.value.replace(/[^0-9]/g, "");
                                      if (field.type === "percentage") {
                                        if (val.length > 3) val = val.slice(0, 3);
                                        if (val !== "" && Number(val) > 100) val = val.slice(0, val.length - 1);
                                      }
                                      setAllFormValues((prev) => ({
                                        ...prev,
                                        [field.name]: val,
                                      }));
                                    }}
                                    onKeyDown={(e) => {
                                      if (
                                        ["e", "E", "+", "-", ".", ",", " "].includes(e.key) ||
                                        (e.key.length === 1 && e.key.match(/[a-zA-Z]/))
                                      ) {
                                        e.preventDefault();
                                      }
                                      if (
                                        field.type === "percentage" &&
                                        e.target.value.length >= 3 &&
                                        !["Backspace", "Delete", "ArrowLeft", "ArrowRight", "Tab"].includes(e.key)
                                      ) {
                                        e.preventDefault();
                                      }
                                    }}
                                    className="signin-input"
                                  />
                                )}
                              </>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })()}
              <div style={{ display: "flex", gap: "12px", marginTop: "18px" }}>
                <button type="submit" className="signin-save-btn">
                  Save Configuration
                </button>
                <button
                  type="button"
                  className="signin-save-btn"
                  style={{
                    background: showConfig ? "#a742f5" : "#4287f5",
                    color: "#fff"
                  }}
                  onClick={() => setShowConfig((prev) => !prev)}
                >
                  {showConfig ? "Hide Configuration" : "Show Configuration"}
                </button>
              </div>
              {message && (
                <div
                  style={{
                    marginTop: "16px",
                    color: message.startsWith("✅") ? "green" : "red",
                  }}
                >
                  {message}
                </div>
              )}
            </form>
            {showConfig && renderConfigTables()}
          </div>
        </div>
      </div>
    </div>
  );
}

export default StandardConfigForm;

