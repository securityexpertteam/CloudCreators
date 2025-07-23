// App.js
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Signup from './signup/Signup';
import StandardConfigForm from './StandardConfigForm';
import StandardConfigSummary from './StandardConfigSummary';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Signup />} />
        <Route path="/config" element={<StandardConfigForm />} />
        <Route path="/config/summary" element={<StandardConfigSummary />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
