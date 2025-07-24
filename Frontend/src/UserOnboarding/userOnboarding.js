import React, { useState } from "react";
import "./userOnboarding.css";

const UserOnboarding = () => {
  const [user, setUser] = useState({
    cloudName: "",
    project: "",
    environment: "",
    username: "",
    password: "",
  });

  const [usersList, setUsersList] = useState([]);

  const handleAddUser = () => {
    if (Object.values(user).some((val) => !val.trim())) {
      alert("Please fill all fields");
      return;
    }

    setUsersList([...usersList, user]);
    setUser({ cloudName: "", project: "", environment: "", username: "", password: "" });
  };

  const handleEditUser = (index) => {
    const editingUser = usersList[index];
    setUser(editingUser);
    setUsersList(usersList.filter((_, i) => i !== index));
  };

  const handleDeleteUser = (index) => {
    setUsersList(usersList.filter((_, i) => i !== index));
  };

//   const handleSubmit = async () => {
//     try {
//       const response = await fetch("http://localhost:8000/bulk_signup", {
//         method: "POST",
//         headers: { "Content-Type": "application/json" },
//         body: JSON.stringify(usersList),
//       });
//       const data = await response.json();
//       alert(data.message);
//       setUsersList([]);
//     } catch (error) {
//       alert("Error submitting data");
//       console.error(error);
//     }
//   };

const handleSubmit = async () => {
  try {
    const response = await fetch("http://localhost:8000/bulk_signup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(usersList),
    });

    const data = await response.json();

    if (!response.ok) {
      // Server returned an error message
      alert(data.detail || "Error occurred during submission");
      return;
    }

    alert(data.message); // This will now only show if response is OK
    setUsersList([]);
  } catch (error) {
    alert("Error submitting data");
    console.error("Submission Error:", error);
  }
};


  return (
    <div className="container">
      <h2>User Onboarding To Cloud Environment</h2>

      <div className="form-row">
        <div className="input-group">
          <label>Cloud Provider</label>
          <select
            value={user.cloudName}
            onChange={(e) => setUser({ ...user, cloudName: e.target.value })}
          >
            <option value="">Select</option>
            <option value="AWS">AWS</option>
            <option value="Azure">Azure</option>
            <option value="GCP">GCP</option>
          </select>
        </div>

        <div className="input-group">
          <label>Management Unit</label>
          <input
            type="text"
            value={user.project}
            placeholder="Enter project"
            onChange={(e) => setUser({ ...user, project: e.target.value })}
          />
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
          <label>Username</label>
          <input
            type="text"
            placeholder="Enter username"
            value={user.username}
            onChange={(e) => setUser({ ...user, username: e.target.value })}
          />
        </div>

        <div className="input-group">
          <label>Password</label>
          <input
            type="password"
            placeholder="Enter password"
            value={user.password}
            onChange={(e) => setUser({ ...user, password: e.target.value })}
          />
        </div>

        <button className="add-btn" onClick={handleAddUser}>Add</button>
      </div>

      {/* User List */}
      {usersList.length > 0 && (
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Cloud</th>
                <th>Management Unit</th>
                <th>Environment</th>
                <th>Username</th>
                <th>Password</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {usersList.map((u, idx) => (
                <tr key={idx}>
                  <td>{u.cloudName}</td>
                  <td>{u.project}</td>
                  <td>{u.environment}</td>
                  <td>{u.username}</td>
                  <td>{u.password}</td>
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
    </div>
  );
};

export default UserOnboarding;
