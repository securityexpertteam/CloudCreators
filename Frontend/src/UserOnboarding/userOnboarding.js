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
    vaultname: "",
    secretname: "",
    srvaccntName: "",
    srvacctPass: ""
  });

  const [usersList, setUsersList] = useState([]);

  const handleAddUser = () => {
    const requiredFields = ["cloudName", "environment", "rootId", "managementUnitId", "vaultname", "secretname", "srvaccntName", "srvacctPass"];
    for (let field of requiredFields) {
      if (!user[field]) {
        alert(`Please fill the ${field} field`);
        return;
      }
    }

    setUsersList([...usersList, user]);
    setUser({
      cloudName: "",
      environment: "",
      rootId: "",
      managementUnitId: "",
      vaultname: "",
      secretname: "",
      srvaccntName: "",
      srvacctPass: ""
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

  const handleDeleteEnvEntry = async (envId) => {
  if (!window.confirm("Are you sure you want to delete this environment?")) return;

  try {
    const res = await fetch(`http://localhost:8000/delete_environment/${envId}`, {
      method: "DELETE",
    });

    const result = await res.json();
    if (!res.ok) {
      alert(result.detail || "Delete failed");
      return;
    }

    alert("Environment deleted successfully");

    // Refresh the environment list
    setEnvEntries(envEntries.filter(entry => entry._id !== envId));
  } catch (error) {
    console.error("Delete Error:", error);
    alert("Error deleting environment");
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
                cloudName: e.target.value
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
          <label>Root ID</label>
          <input
            type="text"
            placeholder="Enter RootId"
            value={user.rootId}
            onChange={(e) => setUser({ ...user, rootId: e.target.value })}
          />
        </div>

        <div className="input-group">
          <label>Management Unit ID</label>
          <input
            type="text"
            placeholder="Enter ManagementUnitId"
            value={user.managementUnitId}
            onChange={(e) => setUser({ ...user, managementUnitId: e.target.value })}
          />
        </div>

        <div className="input-group">
          <label>Vault Name</label>
          <input
            type="text"
            placeholder="Enter Vault Name"
            value={user.vaultname}
            onChange={(e) => setUser({ ...user, vaultname: e.target.value })}
          />
        </div>

        <div className="input-group">
          <label>Secret Name</label>
          <input
            type="text"
            placeholder="Enter Secret Name"
            value={user.secretname}
            onChange={(e) => setUser({ ...user, secretname: e.target.value })}
          />
        </div>

        <div className="input-group">
          <label>Client ID -svc name</label>
          <input
            type="text"
            placeholder="Enter Client ID"
            value={user.srvaccntName}
            onChange={(e) => setUser({ ...user, srvaccntName: e.target.value })}
          />
        </div>

        <div className="input-group">
          <label>Client Secret-svcpass</label>
          <input
            type="password"
            placeholder="Enter Client Secret"
            value={user.srvacctPass}
            onChange={(e) => setUser({ ...user, srvacctPass: e.target.value })}
          />
        </div>

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
                <th>MgmtUnitId</th>
                <th>Vault</th>
                <th>Secret</th>
                <th>ClientId-svcName</th>
                <th>ClientSecret-svc-password</th>
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
                  <td>{u.vaultname}</td>
                  <td>{u.secretname}</td>
                  <td>{u.srvaccntName}</td>
                  <td>{"*".repeat(u.srvacctPass.length)}</td>
                  <td>
                    <button className="edit-btn" onClick={() => handleEditUser(idx)}>Edit</button>
                    <button className="delete-btn" onClick={() => handleDeleteUser(idx)}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <button className="submit-btn" onClick={handleSubmit}>Submit</button>
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
              <th>MgmtUnitId</th>
              <th>Vault Name</th>
              <th>Secret Name</th>
              <th>Delete</th>
            </tr>
          </thead>
          <tbody>
  {envEntries.map((entry, idx) => (
    <tr key={entry._id || idx}>
      <td>{entry.cloudName}</td>
      <td>{entry.environment}</td>
      <td>{entry.rootId}</td>
      <td>{entry.managementUnitId}</td>
      <td>{entry.vaultname || "N/A"}</td>
      <td>{entry.secretname || "N/A"}</td>
      <td>
        <button
          className="delete-btn"
          onClick={() => handleDeleteEnvEntry(entry._id)}
        >
          Delete
        </button>
      </td>
    </tr>
  ))}
</tbody>
        </table>
      </div>
    </div>
  );
};

export default UserOnboarding;
