// App.js
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Signup from './signup/Signup';
import StandardConfigForm from './StandardConfigForm';
import StandardConfigSummary from './StandardConfigSummary';
import UserOnboarding from "./UserOnboarding/userOnboarding";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Signup />} />
        <Route path="/config" element={<StandardConfigForm />} />
        <Route path="/config/summary" element={<StandardConfigSummary />} />
        <Route path="/userOnboarding" element={<UserOnboarding />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
