import React, { useState } from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import './Header.css';
import { FaRocket, FaUserCircle } from 'react-icons/fa';

const Header = () => {
  const navigate = useNavigate();
  const [showProfile, setShowProfile] = useState(false);

  // Get user details from localStorage
  const user = JSON.parse(localStorage.getItem("user"));

  return (
    <header className="header">
      <div className="logo-section" onClick={() => navigate('/')}>
        <FaRocket className="logo-icon" />
        <span className="app-name">Cloud Cost Optimization</span>
      </div>

      <div className="right-section">
        <nav className="nav-links">
          <NavLink to="/UserOnboarding" className={({ isActive }) => isActive ? "active" : ""}>
            User Onboarding
          </NavLink>
          <NavLink to="/config" className={({ isActive }) => isActive ? "active" : ""}>
            Standard Config
          </NavLink>
          <NavLink to="/dashboard" className={({ isActive }) => isActive ? "active" : ""}>
            Dashboard
          </NavLink>
        </nav>

        <div className="user-icon" onClick={() => setShowProfile(!showProfile)} style={{ cursor: "pointer" }}>
          <FaUserCircle size={24} />
        </div>
        {showProfile && user && (
  <div className="profile-modal">
    <p><b>First Name:</b> {user.firstname}</p>
    <p><b>Last Name:</b> {user.lastname}</p>
    <p><b>Email:</b> {user.email}</p>
    <div style={{ display: "flex", gap: "10px" }}>
      <button onClick={() => setShowProfile(false)}>Close</button>
      <button
        onClick={() => {
          localStorage.removeItem("user");
          setShowProfile(false);
          navigate("/signin");
        }}
      >
        Logout
      </button>
    </div>
  </div>
)}
      </div>
    </header>
  );
};

export default Header;