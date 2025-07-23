import './Signup.css';
import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

function Signup() {
  const [formData, setFormData] = useState({
    username: "",
    password: "",
    cloudName: "",
    project: "",
    environment: "",
  });

  const navigate = useNavigate();

  const handleChange = (e) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    try {
      const res = await fetch("http://localhost:8000/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      if (res.ok) {
        alert("Signup successful!");
        navigate("/config");  // ✅ redirect to Standard Config page
      } else {
        const data = await res.json();
        alert(data.detail || "Signup failed");
      }
    } catch (error) {
      alert("Network error. Try again.");
      console.error(error);
    }
  };

  return (
    <div className="signup-container">
      <h2 className="signup-title">Enter Details</h2>
      <form onSubmit={handleSubmit} className="signup-form">
        <input name="username" placeholder="Username" onChange={handleChange} required />
        <input name="password" type="password" placeholder="Password" onChange={handleChange} required />
        <input name="cloudName" placeholder="Cloud Name" onChange={handleChange} required />
        <input name="project" placeholder="Project" onChange={handleChange} required />
        <input name="environment" placeholder="Environment" onChange={handleChange} required />
        <button type="submit">Submit</button>
      </form>
    </div>
  );
}

export default Signup;
