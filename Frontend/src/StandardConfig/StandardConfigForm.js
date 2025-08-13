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
      { name: "db_type", label: "DB Type", type: "dropdown", options: ["sql","mysql","postgresql","mariadb","cosmos","redis","mongodb","synapse"] },
    ],
  },
];

function StandardConfigForm() {
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const email = user.email || "";

  const [allFormValues, setAllFormValues] = useState({});
  const [selectedType, setSelectedType] = useState("");
  const [loadedConfig, setLoadedConfig] = useState({});
  const [message, setMessage] = useState("");
  const [showConfig, setShowConfig] = useState(false);

  useEffect(() => {
    setMessage("");
    const fetchConfig = async () => {
      try {
        const res = await axios.get(
          `http://localhost:8000/api/config/latest?email=${encodeURIComponent(email)}`
        );
        const config = res.data || {};
        setLoadedConfig(config);

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
        const dbTypes = CONFIG_TYPES.find(t => t.key === "database").fields[0].options;
        dbTypes.forEach(db => {
          const key = `${db}_db_size`;
          initial[key] = config[key] !== undefined ? config[key] : "";
        });
        setAllFormValues(initial);

        const firstPaneWithConfig = CONFIG_TYPES.find(type =>
          type.fields.some(field =>
            config[field.name] !== undefined &&
            config[field.name] !== "" &&
            config[field.name] !== false
          )
        );
        setSelectedType(firstPaneWithConfig ? firstPaneWithConfig.key : CONFIG_TYPES[0].key);
      } catch {
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

  const handleTypeChange = (typeKey) => {
    setSelectedType(typeKey);
  };

  const handleInputChange = (fieldName, value, fieldType) => {
    setAllFormValues((prev) => ({
      ...prev,
      [fieldName]: fieldType === "checkbox" ? !!value : value,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setMessage("");
    let hasError = false;

    for (const typeObj of CONFIG_TYPES) {
      for (const field of typeObj.fields) {
        if (typeObj.key === "database" && field.name === "db_type") {
          const dbType = allFormValues.db_type;
          if (dbType) {
            const dbSizeVal = allFormValues[`${dbType}_db_size`];
            if (dbSizeVal !== undefined && dbSizeVal !== "") {
              const val = Number(dbSizeVal);
              if (isNaN(val) || val < 1 || val > 100) {
                setMessage(`❗ DB Size (%) for ${dbType} must be between 1 and 100.`);
                hasError = true;
                break;
              }
            }
          }
        } else if (field.type === "percentage") {
          const raw = allFormValues[field.name];
          if (raw === undefined || raw === null || raw === "") continue;
          const val = Number(raw);
          if (isNaN(val) || val < 1 || val > 100) {
            setMessage(`❗ ${field.label} in ${typeObj.label} must be between 1 and 100.`);
            hasError = true;
            break;
          }
        }
      }
      if (hasError) break;
    }
    if (hasError) return;

    let payload = { email };
    CONFIG_TYPES.forEach(type => {
      type.fields.forEach(field => {
        if (type.key === "database" && field.name === "db_type") {
          payload.db_type = allFormValues.db_type || null;
          if (allFormValues.db_type) {
            const dbSizeKey = `${allFormValues.db_type}_db_size`;
            payload[dbSizeKey] = allFormValues[dbSizeKey] !== "" ? allFormValues[dbSizeKey] : null;
          }
        } else if (type.key !== "database") {
          const v = allFormValues[field.name];
          payload[field.name] = (v === undefined || v === "") ? null : v;
        }
      });
    });

    try {
      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("✅ Configuration saved successfully!");
      const res = await axios.get(
        `http://localhost:8000/api/config/latest?email=${encodeURIComponent(email)}`
      );
      const config = res.data || {};
      setLoadedConfig(config);

      let updated = {};
      CONFIG_TYPES.forEach(type => {
        type.fields.forEach(field => {
          if (field.name === "db_type") return;
          updated[field.name] = field.type === "checkbox"
            ? config[field.name] === true
            : (config[field.name] !== undefined ? config[field.name] : "");
        });
      });
      updated.db_type = allFormValues.db_type || "";
      const dbTypes = CONFIG_TYPES.find(t => t.key === "database").fields[0].options;
      dbTypes.forEach(db => {
        const key = `${db}_db_size`;
        updated[key] = config[key] !== undefined ? config[key] : "";
      });
      setAllFormValues(updated);
    } catch {
      setMessage("❌ Failed to save configuration.");
    }
  };

  const renderConfigTables = () => {
    if (!loadedConfig || Object.keys(loadedConfig).length === 0) {
      return <div style={{ marginTop: "16px" }}>No previous configuration found.</div>;
    }

    return (
      <div style={{ marginTop: "32px" }}>
        {CONFIG_TYPES.filter(t => t.key !== "database").map(type => (
          <div key={type.key} style={{
              width: "100%", marginBottom: "28px", background: "#fff",
              borderRadius: "8px", boxShadow: "0 2px 8px rgba(0,0,0,0.04)",
              padding: "18px 18px 12px 18px"
          }}>
            <div style={{ fontWeight: 700, fontSize: "1.08em", marginBottom: "10px", color: "#232526" }}>
              {type.label}
            </div>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: "6px", borderBottom: "1px solid #ccc" }}>Field</th>
                  <th style={{ textAlign: "left", padding: "6px", borderBottom: "1px solid #ccc" }}>Value</th>
                </tr>
              </thead>
              <tbody>
                {type.fields
                  .filter(f => {
                    const val = loadedConfig[f.name];
                    return val !== undefined && val !== null && val !== "";
                  })
                  .map(f => (
                    <tr key={f.name}>
                      <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>{f.label}</td>
                      <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>
                        {typeof loadedConfig[f.name] === "boolean"
                          ? (loadedConfig[f.name] ? "Yes" : "No")
                          : loadedConfig[f.name]}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        ))}

        <div style={{
          width: "100%", marginBottom: "28px", background: "#fff",
          borderRadius: "8px", boxShadow: "0 2px 8px rgba(0,0,0,0.04)",
          padding: "18px 18px 12px 18px"
        }}>
          <div style={{ fontWeight: 700, fontSize: "1.08em", marginBottom: "10px", color: "#232526" }}>
            Database
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse", marginBottom: "16px" }}>
            <thead>
              <tr>
                <th style={{ textAlign: "left", padding: "6px", borderBottom: "1px solid #ccc" }}>DB Type</th>
                <th style={{ textAlign: "left", padding: "6px", borderBottom: "1px solid #ccc" }}>DB Size (%)</th>
              </tr>
            </thead>
            <tbody>
              {CONFIG_TYPES.find(t => t.key === "database").fields[0].options
                .filter(db => {
                  const key = `${db}_db_size`;
                  // Show only if db_size is filled (not empty, undefined, or null)
                  return loadedConfig[key] !== undefined && loadedConfig[key] !== null && loadedConfig[key] !== "";
                })
                .map(db => {
                  const key = `${db}_db_size`;
                  return (
                    <tr key={db}>
                      <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>
                        {db.charAt(0).toUpperCase() + db.slice(1)}
                      </td>
                      <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>
                        {loadedConfig[key]}
                      </td>
                    </tr>
                  );
                })}
              {/* If none are filled, show a placeholder row */}
              {CONFIG_TYPES.find(t => t.key === "database").fields[0].options.every(db => {
                const key = `${db}_db_size`;
                return loadedConfig[key] === undefined || loadedConfig[key] === null || loadedConfig[key] === "";
              }) && (
                <tr>
                  <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>-</td>
                  <td style={{ padding: "6px", borderBottom: "1px solid #eee" }}>-</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div className="signin-bg">
      <div className="signin-center">
        <div className="signin-card" style={{ maxWidth: 820, minWidth: 320, width: "100%" }}>
          <div className="signin-card-content" style={{ padding: "18px 18px 10px 18px" }}>
            <h2 className="signin-title" style={{ fontSize: "1.25rem" }}>Standard Configuration</h2>
            <form onSubmit={handleSubmit}>
              <div className="signin-section">
                <label>
                  <b>Select Configuration Type:</b>
                </label>
                <div style={{ height: 12 }} />
                <div>
                  <div
                    className="signin-type-buttons"
                    style={{
                      display: "flex",
                      flexWrap: "nowrap",
                      gap: "8px",
                      justifyContent: "flex-start",
                      marginBottom: "8px"
                    }}
                  >
                    {CONFIG_TYPES.slice(0, 4).map((type, idx) => (
                      <button
                        key={type.key}
                        type="button"
                        className={`signin-type-btn${selectedType === type.key ? " selected" : ""}`}
                        onClick={() => handleTypeChange(type.key)}
                        style={{
                          marginRight: 0,
                          marginBottom: 0,
                          padding: "7px 10px",
                          borderRadius: 8,
                          border: selectedType === type.key ? "2px solid #4287f5" : "1.5px solid #bbb",
                          background: selectedType === type.key
                            ? "linear-gradient(90deg, #4287f5 0%, #a742f5 100%)"
                            : "#fff",
                          color: selectedType === type.key ? "#fff" : "#232526",
                          fontWeight: 600,
                          cursor: "pointer",
                          transition: "all 0.2s",
                          fontSize: "0.98rem",
                          minWidth: 0,
                          flex: 1
                        }}
                        id={type.key === "general" ? "general-btn" : undefined}
                      >
                        {type.label}
                      </button>
                    ))}
                  </div>
                  <div
                    className="signin-type-buttons"
                    style={{
                      display: "flex",
                      justifyContent: "flex-start",
                      marginBottom: "8px"
                    }}
                  >
                    <button
                      key={CONFIG_TYPES[4].key}
                      type="button"
                      className={`signin-type-btn${selectedType === CONFIG_TYPES[4].key ? " selected" : ""}`}
                      onClick={() => handleTypeChange(CONFIG_TYPES[4].key)}
                      style={{
                        marginRight: 0,
                        marginBottom: 0,
                        padding: "7px 10px",
                        borderRadius: 8,
                        border: selectedType === CONFIG_TYPES[4].key ? "2px solid #4287f5" : "1.5px solid #bbb",
                        background: selectedType === CONFIG_TYPES[4].key
                          ? "linear-gradient(90deg, #4287f5 0%, #a742f5 100%)"
                          : "#fff",
                        color: selectedType === CONFIG_TYPES[4].key ? "#fff" : "#232526",
                        fontWeight: 600,
                        cursor: "pointer",
                        transition: "all 0.2s",
                        fontSize: "0.98rem",
                        minWidth: 0,
                        flex: "1 1 0",
                        maxWidth: "25%"
                      }}
                    >
                      {CONFIG_TYPES[4].label}
                    </button>
                  </div>
                </div>
              </div>
              <hr style={{ margin: "10px 0" }} />
              {selectedType && (() => {
                const typeObj = CONFIG_TYPES.find((t) => t.key === selectedType);
                const isDb = typeObj.key === "database";
                return (
                  <div key={selectedType} className={`signin-config-section${isDb ? " db-sec" : ""}`}>
                    <div
                      className="signin-fields-grid"
                      style={
                        isDb
                          ? {
                              gridTemplateColumns: "1fr 1fr",
                              gap: "8px",
                              alignItems: "end",
                              width: "100%",
                              maxWidth: "100%",
                              margin: "0 auto",
                            }
                          : undefined
                      }
                    >
                      {(() => {
                        let visibleFields = typeObj.fields;
                        if (isDb) {
                          const order = { db_type: 0 };
                          visibleFields = visibleFields.sort((a, b) => (order[a.name] ?? 2) - (order[b.name] ?? 2));
                        }
                        return visibleFields.map((field) => {
                          const isSpecialCheckbox = ["stor_lifecycle_enabled", "gen_untagged", "gen_orphaned"].includes(field.name);
                          const inputSizeStyle = isDb
                            ? {
                                height: 36,
                                boxSizing: "border-box",
                                padding: "8px 10px",
                                fontSize: "0.98rem",
                                borderRadius: 8,
                                width: "100%",
                                marginTop: 0
                              }
                            : undefined;
                          const fieldContainerStyle = isDb
                            ? (field.name === "db_type"
                                ? { gridColumn: "1 / span 1", gridRow: 1, marginRight: "12px" }
                                : { gridColumn: "2 / span 1", gridRow: 1 })
                            : undefined;
                          if (isDb && field.name === "db_type") {
                            return (
                              <React.Fragment key="db_type">
                                <div
                                  className="signin-field"
                                  style={{ ...fieldContainerStyle }}
                                >
                                  <label
                                    htmlFor="db_type"
                                    className="signin-field-label"
                                    style={{ marginBottom: 0, lineHeight: 1.1, fontSize: "0.98rem" }}
                                  >
                                    DB Type
                                  </label>
                                  <select
                                    value={allFormValues.db_type || ""}
                                    onChange={e => handleInputChange("db_type", e.target.value, "dropdown")}
                                    className="signin-select"
                                    style={inputSizeStyle}
                                  >
                                    <option value="">Select</option>
                                    {field.options.map(opt => (
                                      <option key={opt} value={opt}>
                                        {opt}
                                      </option>
                                    ))}
                                  </select>
                                </div>
                                {allFormValues.db_type && (
                                  <div
                                    className="signin-field"
                                    style={{ gridColumn: "2 / span 1", gridRow: 1 }}
                                  >
                                    <label
                                      htmlFor="db_size"
                                      className="signin-field-label"
                                      style={{ marginBottom: 0, lineHeight: 1.1, fontSize: "0.98rem" }}
                                    >
                                      DB Size (%)
                                    </label>
                                    <input
                                      type="text"
                                      inputMode="numeric"
                                      pattern="[0-9]*"
                                      value={allFormValues[`${allFormValues.db_type}_db_size`] || ""}
                                      placeholder={
                                        loadedConfig[`${allFormValues.db_type}_db_size`] !== undefined &&
                                        loadedConfig[`${allFormValues.db_type}_db_size`] !== ""
                                          ? loadedConfig[`${allFormValues.db_type}_db_size`]
                                          : ""
                                      }
                                      min={1}
                                      max={100}
                                      onChange={e => {
                                        let val = e.target.value.replace(/[^0-9]/g, "");
                                        if (val.length > 3) val = val.slice(0, 3);
                                        if (val !== "" && Number(val) > 100) val = val.slice(0, val.length - 1);
                                        setAllFormValues(prev => ({
                                          ...prev,
                                          [`${allFormValues.db_type}_db_size`]: val
                                        }));
                                      }}
                                      onKeyDown={e => {
                                        if (
                                          ["e", "E", "+", "-", ".", ",", " "].includes(e.key) ||
                                          (e.key.length === 1 && e.key.match(/[a-zA-Z]/))
                                        ) {
                                          e.preventDefault();
                                        }
                                        if (
                                          e.target.value.length >= 3 &&
                                          !["Backspace", "Delete", "ArrowLeft", "ArrowRight", "Tab"].includes(e.key)
                                        ) {
                                          e.preventDefault();
                                        }
                                      }}
                                      className="signin-input"
                                      style={inputSizeStyle}
                                    />
                                  </div>
                                )}
                              </React.Fragment>
                            );
                          }
                          return (
                            <div
                              className="signin-field"
                              key={field.name}
                              style={fieldContainerStyle}
                            >
                              {field.type === "checkbox" && isSpecialCheckbox ? (
                                <>
                                  <input
                                    type="checkbox"
                                    checked={allFormValues[field.name] === true}
                                    onChange={e => handleInputChange(field.name, e.target.checked, field.type)}
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
                                  <label
                                    htmlFor={field.name}
                                    className="signin-field-label"
                                    style={isDb ? { marginBottom: 0, lineHeight: 1.1, fontSize: "0.98rem" } : undefined}
                                  >
                                    {field.label}
                                  </label>
                                  {field.type === "percentage" ? (
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
                                      min={1}
                                      max={100}
                                      onChange={e => {
                                        let val = e.target.value.replace(/[^0-9]/g, "");
                                        if (val.length > 3) val = val.slice(0, 3);
                                        if (val !== "" && Number(val) > 100) val = val.slice(0, val.length - 1);
                                        setAllFormValues(prev => ({ ...prev, [field.name]: val }));
                                      }}
                                      onKeyDown={e => {
                                        if (
                                          ["e", "E", "+", "-", ".", ",", " "].includes(e.key) ||
                                          (e.key.length === 1 && e.key.match(/[a-zA-Z]/))
                                        ) {
                                          e.preventDefault();
                                        }
                                        if (
                                          e.target.value.length >= 3 &&
                                          !["Backspace", "Delete", "ArrowLeft", "ArrowRight", "Tab"].includes(e.key)
                                        ) {
                                          e.preventDefault();
                                        }
                                      }}
                                      className="signin-input"
                                      style={inputSizeStyle}
                                    />
                                  ) : field.type === "checkbox" ? (
                                    <input
                                      type="checkbox"
                                      checked={allFormValues[field.name] === true}
                                      onChange={e => handleInputChange(field.name, e.target.checked, field.type)}
                                      id={field.name}
                                      className="signin-checkbox"
                                    />
                                  ) : field.type === "dropdown" ? (
                                    <select
                                      value={allFormValues[field.name] !== undefined ? allFormValues[field.name] : ""}
                                      onChange={e => handleInputChange(field.name, e.target.value, field.type)}
                                      className="signin-select"
                                      style={inputSizeStyle}
                                    >
                                      <option value="">
                                        {loadedConfig[field.name] !== undefined && loadedConfig[field.name] !== ""
                                          ? `Previous: ${loadedConfig[field.name]}`
                                          : "Select"}
                                      </option>
                                      {field.options.map(opt => (
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
                                      onChange={e => {
                                        let val = e.target.value.replace(/[^0-9]/g, "");
                                        setAllFormValues(prev => ({ ...prev, [field.name]: val }));
                                      }}
                                      onKeyDown={e => {
                                        if (
                                          ["e", "E", "+", "-", ".", ",", " "].includes(e.key) ||
                                          (e.key.length === 1 && e.key.match(/[a-zA-Z]/))
                                        ) {
                                          e.preventDefault();
                                        }
                                      }}
                                      className="signin-input"
                                      style={inputSizeStyle}
                                    />
                                  )}
                                </>
                              )}
                            </div>
                          );
                        });
                      })()}
                    </div>
                  </div>
                );
              })()}
              <div style={{ display: "flex", gap: "10px", marginTop: "14px" }}>
                <button type="submit" className="signin-save-btn" style={{ fontSize: "1rem", padding: "8px 18px" }}>
                  Save Configuration
                </button>
                <button
                  type="button"
                  className="signin-save-btn"
                  style={{
                    background: showConfig ? "#a742f5" : "#4287f5",
                    color: "#fff",
                    fontSize: "1rem",
                    padding: "8px 18px"
                  }}
                  onClick={() => setShowConfig(prev => !prev)}
                >
                  {showConfig ? "Hide Previous Configuration" : "Show Previous Configuration"}
                </button>
              </div>
              {message && <div className="signin-message">{message}</div>}
            </form>
            {showConfig && renderConfigTables()}
          </div>
        </div>
      </div>
    </div>
  );
}

export default StandardConfigForm;
