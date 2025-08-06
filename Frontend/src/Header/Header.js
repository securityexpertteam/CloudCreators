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
          <NavLink to="/environmentOnboarding" className={({ isActive }) => isActive ? "active" : ""}>
            Environment Onboarding
          </NavLink>
          <NavLink to="/config" className={({ isActive }) => isActive ? "active" : ""}>
            Standard Config
          </NavLink>
          <NavLink to="/schedulescan" className={({ isActive }) => isActive ? "active" : ""}>
            Schedule Scan
          </NavLink>
          <NavLink to="/dashboard" className={({ isActive }) => isActive ? "active" : ""}>
            Dashboard
          </NavLink>
        </nav>

        <div
          className="user-icon"
          onMouseEnter={() => setShowProfile(true)}
          onMouseLeave={() => setShowProfile(false)}
          style={{ cursor: "pointer", position: "relative" }}
        >
          <FaUserCircle size={24} />
          {showProfile && user && (
            <div
              className="profile-modal"
              onMouseEnter={() => setShowProfile(true)}
              onMouseLeave={() => setShowProfile(false)}
            >
              <p><b>Email:</b> {user.email}</p>
              <div style={{ width: "100%" }}>
                <button
                  className="logout-btn"
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
      </div>
    </header>
  );
};

export default Header;
