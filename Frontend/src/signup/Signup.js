import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "./Signup.css";

const SignIn = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();
 const [formData, setFormData] = useState({
    firstname: "",
    lastname: "",
    email: "",
    username: "",
    password: "",
    profilePic: null,
  });

  
  const handleChange = (e) => {
    const { name, value, files } = e.target;
    if (name === "profilePic") {
      setFormData({ ...formData, [name]: files[0] });
    } else {
      setFormData({ ...formData, [name]: value });
    }
  };

  const handleSignin = async (e) => {
    e.preventDefault();
    setError("");
    setSubmitting(true);

    try {
      const response = await fetch("http://localhost:8000/signin", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });

      const result = await response.json();

      if (response.ok) {
        alert(result.message);
        navigate("/userOnboarding");
      } else {
        setError(result.detail || result.message || "Login failed");
      }
    } catch (err) {
      console.error("Signin error:", err);
      setError("Unable to reach the server. Please try again.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="signin-container">
      <form className="signin-form" onSubmit={handleSignin}>
        <div className="signin-header">
           <div className="profile-upload">
          <input type="file" id="profilePic" name="profilePic" onChange={handleChange} hidden />
          <label htmlFor="profilePic" className="upload-label" style={{ fontSize: "40px", cursor: "pointer" }}>
  ☁️
            
          </label>
        </div>

          <h2>Welcome Back</h2>
          <p className="subtitle">Sign in to your account to continue</p>
        </div>

        {error && <div className="signin-error">{error}</div>}

        <div className="field">
          <label htmlFor="email"><b>Email Address</b></label>
          <input
            id="email"
            type="email"
            placeholder="Enter your email"
            value={email}
            required
            onChange={(e) => setEmail(e.target.value)}
          />
        </div>

        <div className="field">
          <label htmlFor="password"><b>Password</b></label>
          <input
            id="password"
            type="password"
            placeholder="Enter your password"
            value={password}
            required
            onChange={(e) => setPassword(e.target.value)}
          />
        </div>

        <div className="forgot-password">
          <button
          type="button"
          className="forgot-password-button"
          onClick={() => alert("Forgot Password feature coming soon!")}
          >
            Forgot your password?
          </button>
        </div>

        <button type="submit" className="signin-button" disabled={submitting}>
          {submitting ? "Signing In..." : "Sign In"}
        </button>
      </form>
    </div>
  );
};

export default SignIn;

