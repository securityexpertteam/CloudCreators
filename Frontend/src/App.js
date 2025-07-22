// App.js
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Signup from './signup/Signup';



function Dashboard() {
  return <h1>Welcome to Cloud Dashboard!</h1>;
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Signup />} />
        <Route path="/dashboard" element={<Dashboard />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
