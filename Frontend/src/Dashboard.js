

import React, { useEffect, useState } from "react";
import axios from "axios";
import "./App.css";

function App() {
  const [resources, setResources] = useState([]);

  useEffect(() => {
    axios.get("http://localhost:8000/api/resources")
      .then((res) => setResources(res.data))
      .catch((err) => console.error(err));
  }, []);

  return (
    <div className="App">
      <h1>Cloud Resource Dashboard</h1>
      <table>
        <thead>
          <tr>
            <th>Resource ID</th>
            <th>Provider</th>
            <th>Type</th>
            <th>CPU (%)</th>
            <th>Memory (%)</th>
            <th>Network (MB)</th>
            <th>Scale Down</th>
            <th>Untagged</th>
            <th>Orphaned VMs</th>
          </tr>
        </thead>
        <tbody>
          {resources.map((r, i) => (
            <tr key={i}>
              <td>{r.resource_id}</td>
              <td>{r.provider}</td>
              <td>{r.resource_type}</td>
              <td>{r.cpu_usage}</td>
              <td>{r.memory_usage}</td>
              <td>{r.network_usage}</td>
              <td>{r.scale_down_recommendation}</td>
              <td>{r.untagged_instances}</td>
              <td>{r.orphaned_vms}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;



// import React, { useEffect, useState } from "react";
// import axios from "axios";
// import "./App.css";

// function App() {
//   const [resources, setResources] = useState([]);

//   useEffect(() => {
//     axios.get("http://localhost:8000/api/resources")
//       .then((res) => setResources(res.data))
//       .catch((err) => console.error(err));
//   }, []);

//   return (
//     <div className="App">
//       <h1>Cloud Resource Dashboard</h1>
//       <table>
//         <thead>
//           <tr>
//             <th>Resource Type</th>
//             <th>Sub Resource Type</th>
//             <th>Resource Name</th>
//             <th>Region</th>
//             <th>Total Cost</th>
//             <th>Currency</th>
//             <th>Finding</th>
//             <th>Recommendation</th>
//             <th>Environment</th>
//             <th>Timestamp</th>
//             <th>Tags</th>
//             <th>Confidence Score</th>
//             <th>Status</th>
//             <th>Entity</th>
//           </tr>
//         </thead>
//         <tbody>
//           {resources.map((r, i) => (
//             <tr key={i}>
//               <td>{r.ResourceType}</td>
//               <td>{r.SubResourceType}</td>
//               <td>{r.ResourceName}</td>
//               <td>{r.Region}</td>
//               <td>{r.TotalCost}</td>
//               <td>{r.Currency}</td>
//               <td>{r.Finding}</td>
//               <td>{r.Recommendation}</td>
//               <td>{r.Environment}</td>
//               <td>{new Date(r.Timestamp).toLocaleString()}</td>
//               <td>{r.Tags.join(", ")}</td>
//               <td>{r.ConfidenceScore}</td>
//               <td>{r.Status}</td>
//               <td>{r.Entity}</td>
//             </tr>
//           ))}
//         </tbody>
//       </table>
//     </div>
//   );
// }

// export default App;

