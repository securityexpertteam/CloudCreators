// App.js
import { Routes, Route, BrowserRouter, useLocation } from "react-router-dom";
import SignIn from './SignIn/SignIn';
import SignUp from './SignUp/SignUp';
import StandardConfigForm from './StandardConfig/StandardConfigForm';
import EnvironmentOnboarding from "./EnvironmentOnboarding/EnvironmentOnboarding";
import Header from "./Header/Header";
import Footer from "./Footer/Footer";
import Dashboard from "./Dashboard/Dashboard";
import ScheduleScan from "./ScheduleScan/ScheduleScan";
import PrivateRoute from "./PrivateRoute"; // Import the new PrivateRoute
import { useEffect } from "react";

function AppContent() {
  const location = useLocation();
  const isSignInPage = location.pathname === "/" || location.pathname === "/signin" || location.pathname === "/signup";

  useEffect(() => {
    window.scrollTo(0, 0);
  }, [location]);

  return (
    <>
      {!isSignInPage && <Header />}
      <main style={{ minHeight: '80vh', padding: '20px' }}>
        <Routes>
          {/* Public Routes */}
          <Route path="/" element={<SignIn />} />
          <Route path="/signin" element={<SignIn />} />
          <Route path="/signup" element={<SignUp />} />

          {/* Protected Routes */}
          <Route
            path="/config"
            element={
              <PrivateRoute>
                <StandardConfigForm />
              </PrivateRoute>
            }
          />
          <Route
            path="/environmentOnboarding"
            element={
              <PrivateRoute>
                <EnvironmentOnboarding />
              </PrivateRoute>
            }
          />
          <Route
            path="/dashboard"
            element={
              <PrivateRoute>
                <Dashboard />
              </PrivateRoute>
            }
          />
          <Route
            path="/schedulescan"
            element={
              <PrivateRoute>
                <ScheduleScan />
              </PrivateRoute>
            }
          />
        </Routes>
      </main>
      {!isSignInPage && <Footer />}
    </>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AppContent />
    </BrowserRouter>
  );
}

export default App;













