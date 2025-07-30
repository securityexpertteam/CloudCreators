import React, { useEffect, useState } from "react";
import axios from "axios";
import './StandardConfigForm.css';

const CONFIG_TYPES = [
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
      { name: "k8s_node_memory_percentage", label: "Node Memory Usage (%)", type: "percentage" },
      { name: "k8s_node_count", label: "Number of Nodes", type: "number" },
      { name: "k8s_volume_percentage", label: "Persistent Volume Usage (%)", type: "percentage" },
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
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const email = user.email || "";

  const [selectedTypes, setSelectedTypes] = useState([]);
  const [formValues, setFormValues] = useState({});
  const [loadedConfig, setLoadedConfig] = useState({});
  const [message, setMessage] = useState("");

  useEffect(() => {
    setMessage("");
    const fetchConfig = async () => {
      try {
        const res = await axios.get(
          `http://localhost:8000/api/config/latest?email=${encodeURIComponent(email)}`
        );
        const config = res.data || {};
        setLoadedConfig(config);
        setFormValues({});
        // Auto-select types with any previous value
        setSelectedTypes(
          CONFIG_TYPES.filter(type =>
            type.fields.some(field =>
              config[field.name] !== undefined &&
              config[field.name] !== "" &&
              config[field.name] !== false
            )
          ).map(type => type.key)
        );
      } catch {
        setFormValues({});
        setLoadedConfig({});
        setSelectedTypes([]);
      }
    };
    if (email) fetchConfig();
  }, [email]);

  const handleTypeChange = (typeKey) => {
    setSelectedTypes((prev) =>
      prev.includes(typeKey)
        ? prev.filter((t) => t !== typeKey)
        : [...prev, typeKey]
    );
  };

  const handleInputChange = (fieldName, value, fieldType) => {
    setFormValues((prev) => ({
      ...prev,
      [fieldName]: fieldType === "checkbox" ? !!value : value,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setMessage("");
    let hasError = false;

    for (const typeKey of selectedTypes) {
      const typeObj = CONFIG_TYPES.find((t) => t.key === typeKey);
      for (const field of typeObj.fields) {
        if (field.type === "percentage") {
          const val = Number(formValues[field.name]);
          if (
            isNaN(val) ||
            val < 1 ||
            val > 100
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

    let payload = { email };
    selectedTypes.forEach(typeKey => {
      const typeObj = CONFIG_TYPES.find(t => t.key === typeKey);
      typeObj.fields.forEach(field => {
        payload[field.name] = formValues[field.name];
      });
    });

    try {
      await axios.post("http://localhost:8000/api/configs", payload);
      setMessage("✅ Configuration saved successfully!");
      setFormValues({});
      // Optionally, reload config to update placeholders
      const res = await axios.get(
        `http://localhost:8000/api/config/latest?email=${encodeURIComponent(email)}`
      );
      setLoadedConfig(res.data || {});
    } catch (err) {
      setMessage("❌ Failed to save configuration.");
    }
  };

  return (
    <div className="standard-config-container">
      <h2>Standard Configuration</h2>
      <form onSubmit={handleSubmit}>
        <div>
          <label>
            <b>Select Configuration Types:</b>
          </label>
          <div className="type-checkboxes">
            {CONFIG_TYPES.map((type) => (
              <label key={type.key} className="type-checkbox-label">
                <input
                  type="checkbox"
                  checked={selectedTypes.includes(type.key)}
                  onChange={() => handleTypeChange(type.key)}
                />
                {type.label}
              </label>
            ))}
          </div>
        </div>
        <hr />
        {selectedTypes.map((typeKey) => {
          const typeObj = CONFIG_TYPES.find((t) => t.key === typeKey);
          return (
            <div key={typeKey} className="config-section">
              <h3>{typeObj.label}</h3>
              {typeObj.fields.map((field) => (
                <div className="field" key={field.name}>
                  <label>
                    {field.label}
                    {field.type === "percentage" && " (1-100)"}
                  </label>
                  {field.type === "checkbox" ? (
                    <div className="checkbox-field">
                      <input
                        type="checkbox"
                        checked={
                          formValues[field.name] !== undefined
                            ? formValues[field.name]
                            : !!loadedConfig[field.name]
                        }
                        onChange={(e) =>
                          handleInputChange(
                            field.name,
                            e.target.checked,
                            field.type
                          )
                        }
                        id={field.name}
                      />
                      <label htmlFor={field.name}>{field.label}</label>
                      {loadedConfig[field.name] !== undefined && (
                        <span className="previous-value">
                          Previous: {loadedConfig[field.name] ? "Enabled" : "Disabled"}
                        </span>
                      )}
                    </div>
                  ) : field.type === "dropdown" ? (
                    <select
                      value={formValues[field.name] !== undefined ? formValues[field.name] : ""}
                      onChange={(e) =>
                        handleInputChange(
                          field.name,
                          e.target.value,
                          field.type
                        )
                      }
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
                      type={field.type === "number" ? "number" : "text"}
                      value={formValues[field.name] !== undefined ? formValues[field.name] : ""}
                      placeholder={
                        loadedConfig[field.name] !== undefined && loadedConfig[field.name] !== ""
                          ? loadedConfig[field.name]
                          : ""
                      }
                      min={field.type === "percentage" ? 1 : undefined}
                      max={field.type === "percentage" ? 100 : undefined}
                      onChange={(e) =>
                        handleInputChange(
                          field.name,
                          field.type === "number" || field.type === "percentage"
                            ? Number(e.target.value)
                            : e.target.value,
                          field.type
                        )
                      }
                    />
                  )}
                </div>
              ))}
            </div>
          );
        })}
        <button type="submit" className="standard-config-save-btn">
          Save Configuration
        </button>
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
    </div>
  );
}

export default StandardConfigForm;