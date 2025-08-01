// PrivateRoute.js
import React from "react";
import { Navigate } from "react-router-dom";

const PrivateRoute = ({ children }) => {
  const isAuthenticated = !!localStorage.getItem("user"); // or sessionStorage, depending on your app
  return isAuthenticated ? children : <Navigate to="/signin" />;
};

export default PrivateRoute;
