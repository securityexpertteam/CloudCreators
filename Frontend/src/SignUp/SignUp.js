import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "./SignUp.css";

const SignUp = () => {
  const [form, setForm] = useState({
    firstname: "",
    lastname: "",
    email: "",
    password: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  const handleSignup = async (e) => {
    e.preventDefault();
    setError("");
    setSubmitting(true);

    try {
      const response = await fetch("http://localhost:8000/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      const result = await response.json();
      if (response.ok) {
        alert("Signup successful! Please sign in.");
        navigate("/signin");
      } else {
        setError(result.detail || result.message || "Signup failed");
      }
    } catch (err) {
      setError("Unable to reach the server. Please try again.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="signin-container">
      <form className="signin-form" onSubmit={handleSignup}>
        <h2>Create Account</h2>
        {error && <div className="signin-error">{error}</div>}
        <div className="field">
          <label htmlFor="firstname"><b>First Name</b></label>
          <input
            id="firstname"
            name="firstname"
            type="text"
            placeholder="Enter your first name"
            value={form.firstname}
            required
            onChange={handleChange}
          />
        </div>
        <div className="field">
          <label htmlFor="lastname"><b>Last Name</b></label>
          <input
            id="lastname"
            name="lastname"
            type="text"
            placeholder="Enter your last name"
            value={form.lastname}
            required
            onChange={handleChange}
          />
        </div>
        <div className="field">
          <label htmlFor="email"><b>Email Address</b></label>
          <input
            id="email"
            name="email"
            type="email"
            placeholder="Enter your email"
            value={form.email}
            required
            onChange={handleChange}
          />
        </div>
        <div className="field">
          <label htmlFor="password"><b>Password</b></label>
          <input
            id="password"
            name="password"
            type="password"
            placeholder="Enter your password"
            value={form.password}
            required
            onChange={handleChange}
          />
        </div>
        <button type="submit" className="signin-button" disabled={submitting}>
          {submitting ? "Signing Up..." : "Sign Up"}
        </button>
        <div className="signup-redirect">
          <button
            type="button"
            className="signup-link"
            onClick={() => navigate("/signin")}
            style={{ marginTop: "16px", background: "none", border: "none", color: "#007bff", cursor: "pointer", textDecoration: "underline" }}
          >
            Sign Up?
          </button>
        </div>
      </form>
    </div>
  );
};

export default SignUp;