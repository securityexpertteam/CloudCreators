
import React from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import './Header.css';
import { FaRocket, FaUserCircle } from 'react-icons/fa';

const Header = () => {
  const navigate = useNavigate();

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

        <div className="user-icon">
          <FaUserCircle size={24} />
        </div>
      </div>
    </header>
  );
};

export default Header;
