import React, { useState, useEffect } from "react";
import "./userOnboarding.css";

const UserOnboarding = () => {
  const [envEntries, setEnvEntries] = useState([]);
  const email = JSON.parse(localStorage.getItem("user")).email;

  useEffect(() => {
    fetch(`http://localhost:8000/environments/${email}`)
      .then(res => res.json())
      .then(data => setEnvEntries(data.data || []));
  }, [email]);

  const [user, setUser] = useState({
    cloudName: "",
    environment: "",
    rootId: "",
    managementUnitId: "",
    srvaccntName: "",
    srvacctPass: "",
    gcpJsonFile: null
  });

  const [usersList, setUsersList] = useState([]);

  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (event) => {
        setUser({ ...user, gcpJsonFile: event.target.result });
      };
      reader.readAsDataURL(file);
    }
  };

  const handleAddUser = () => {
    // Validation
    if (
      !user.cloudName ||
      !user.environment ||
      !user.rootId ||
      !user.managementUnitId ||
      (user.cloudName === "GCP"
        ? !user.gcpJsonFile
        : !user.srvaccntName || !user.srvacctPass)
    ) {
      alert("Please fill all fields");
      return;
    }
    setUsersList([...usersList, user]);
    setUser({
      cloudName: "",
      environment: "",
      rootId: "",
      managementUnitId: "",
      srvaccntName: "",
      srvacctPass: "",
      gcpJsonFile: null
    });
  };

  const handleEditUser = (index) => {
    setUser(usersList[index]);
    setUsersList(usersList.filter((_, i) => i !== index));
  };

  const handleDeleteUser = (index) => {
    setUsersList(usersList.filter((_, i) => i !== index));
  };

  const handleSubmit = async () => {
    const payload = {
      users: usersList,
      email: email,
    };

    try {
      const response = await fetch("http://localhost:8000/environment_onboarding", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await response.json();

      if (!response.ok) {
        alert(data.detail || "Error occurred during submission");
        return;
      }

      alert(data.message);
      setUsersList([]);
      fetch(`http://localhost:8000/environments/${email}`)
        .then(res => res.json())
        .then(data => setEnvEntries(data.data || []));
    } catch (error) {
      alert("Error submitting data");
      console.error("Submission Error:", error);
    }
  };

  return (
    <div className="container">
      <h2>Environment Onboarding</h2>
      <div className="form-row">
        <div className="input-group">
          <label>Cloud Provider</label>
          <select
            value={user.cloudName}
            onChange={(e) =>
              setUser({
                ...user,
                cloudName: e.target.value,
                srvaccntName: "",
                srvacctPass: "",
                gcpJsonFile: null
              })
            }
          >
            <option value="">Select</option>
            <option value="AWS">AWS</option>
            <option value="Azure">Azure</option>
            <option value="GCP">GCP</option>
          </select>
        </div>
        <div className="input-group">
          <label>Environment</label>
          <select
            value={user.environment}
            onChange={(e) => setUser({ ...user, environment: e.target.value })}
          >
            <option value="">Select</option>
            <option value="Production">Production</option>
            <option value="Integration">Integration</option>
            <option value="Build">Build</option>
            <option value="Test">Test</option>
          </select>
        </div>
        <div className="input-group">
          <label>RootId</label>
          <input
            type="text"
            value={user.rootId}
            placeholder="Enter RootId"
            onChange={(e) =>
              setUser({ ...user, rootId: e.target.value })
            }
          />
        </div>
        <div className="input-group">
          <label>ManagementUnit_ID</label>
          <input
            type="text"
            value={user.managementUnitId}
            placeholder="Enter ManagementUnit_ID"
            onChange={(e) =>
              setUser({ ...user, managementUnitId: e.target.value })
            }
          />
        </div>
        {user.cloudName === "GCP" ? (
          <div className="input-group">
            <label>Upload GCP Service Account JSON</label>
            <input
              type="file"
              accept=".json"
              onChange={handleFileChange}
            />
            {user.gcpJsonFile && <span>File uploaded</span>}
          </div>
        ) : (
          <>
            <div className="input-group">
              <label>Service Account Name</label>
              <input
                type="text"
                value={user.srvaccntName}
                placeholder="Enter Service Account Name"
                onChange={(e) =>
                  setUser({ ...user, srvaccntName: e.target.value })
                }
              />
            </div>
            <div className="input-group">
              <label>Service Account Password</label>
              <input
                type="password"
                value={user.srvacctPass}
                placeholder="Enter Service Account Password"
                onChange={(e) =>
                  setUser({ ...user, srvacctPass: e.target.value })
                }
              />
            </div>
          </>
        )}
        <button className="add-btn" onClick={handleAddUser}>
          Add
        </button>
      </div>

      {usersList.length > 0 && (
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Cloud</th>
                <th>Environment</th>
                <th>RootId</th>
                <th>ManagementUnit_ID</th>
                <th>ServiceAccountName</th>
                <th>ServiceAccountPass</th>
                <th>GCP JSON</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {usersList.map((u, idx) => (
                <tr key={idx}>
                  <td>{u.cloudName}</td>
                  <td>{u.environment}</td>
                  <td>{u.rootId}</td>
                  <td>{u.managementUnitId}</td>
                  <td>{u.srvaccntName}</td>
                  <td>{u.cloudName === "GCP" ? "-" : "*".repeat(u.srvacctPass.length)}</td>
                  <td>{u.cloudName === "GCP" && u.gcpJsonFile ? "Uploaded" : "-"}</td>
                  <td>
                    <button
                      className="edit-btn"
                      onClick={() => handleEditUser(idx)}
                    >
                      Edit
                    </button>
                    <button
                      className="delete-btn"
                      onClick={() => handleDeleteUser(idx)}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <button className="submit-btn" onClick={handleSubmit}>
            Submit
          </button>
        </div>
      )}

      <h3>All Environments Added By You</h3>
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Cloud</th>
              <th>Environment</th>
              <th>RootId</th>
              <th>ManagementUnit_ID</th>
              <th>ServiceAccountName</th>
              <th>GCP JSON</th>
            </tr>
          </thead>
          <tbody>
  {envEntries.map((entry, idx) => (
    <tr key={entry._id || idx}>
      <td>{entry.cloudName}</td>
      <td>{entry.environment}</td>
      <td>{entry.rootId}</td>
      <td>{entry.managementUnitId}</td>
      <td>{entry.cloudName === "GCP" ? "N/A" : entry.srvaccntName}</td>
      <td>{entry.gcpJsonFile ? "Credentials Uploaded" : "N/A"}</td>
    </tr>
  ))}
</tbody>
        </table>
      </div>
    </div>
  );
};

export default UserOnboarding;