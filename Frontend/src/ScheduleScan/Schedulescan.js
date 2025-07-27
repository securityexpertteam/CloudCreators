import React, { useState, useEffect, useRef } from 'react';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { DateTimePicker } from '@mui/x-date-pickers/DateTimePicker';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { TextField, Button, Box, Typography } from '@mui/material';
import dayjs from 'dayjs';
import axios from 'axios';
import { useNavigate } from "react-router-dom";

const ScheduleScan = () => {
  const [selectedDateTime, setSelectedDateTime] = useState(dayjs());
  const [countdownMsg, setCountdownMsg] = useState('');
  const [countdownActive, setCountdownActive] = useState(false);
  const intervalRef = useRef(null);
  const navigate = useNavigate();

  // Check if user is logged in
  useEffect(() => {
    const user = JSON.parse(localStorage.getItem("user"));
    if (!user) {
      alert("Please login first!");
      navigate("/signin");
    }
  }, [navigate]);

  function formatCountdown(ms) {
    if (ms <= 0) return "now!";
    const totalSeconds = Math.floor(ms / 1000);
    const days = Math.floor(totalSeconds / (3600 * 24));
    const hours = Math.floor((totalSeconds % (3600 * 24)) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${days}d ${hours}h ${minutes}m ${seconds}s`;
  }

  // Live countdown effect
  useEffect(() => {
    if (countdownActive) {
      intervalRef.current = setInterval(() => {
        const now = new Date();
        const scheduled = new Date(selectedDateTime.toISOString());
        const diff = scheduled - now;
        setCountdownMsg(`Your scan starts in ${formatCountdown(diff)}`);
        if (diff <= 0) {
          clearInterval(intervalRef.current);
          setCountdownActive(false);
        }
      }, 1000);
      return () => clearInterval(intervalRef.current);
    }
  }, [countdownActive, selectedDateTime]);

  const handleSubmit = async () => {
    try {
      const user = JSON.parse(localStorage.getItem("user"));
      if (!user || !user.email) {
        alert("User not logged in!");
        return;
      }

      const payload = {
        email: user.email,
        ScheduledTimeStamp: selectedDateTime.toISOString()
      };

      const response = await axios.post("http://localhost:8000/triggers", payload);

      setCountdownActive(true); // Start live countdown

      alert(response.data.message);
    } catch (error) {
      console.error("Error while scheduling:", error);
      alert(error.response?.data?.detail || "Failed to save schedule");
    }
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, width: 300, margin: '50px auto' }}>
        <Typography variant="h5">Schedule Scan</Typography>
        <DateTimePicker
          label="Select Date & Time"
          value={selectedDateTime}
          onChange={(newValue) => setSelectedDateTime(newValue)}
          renderInput={(params) => <TextField {...params} />}
        />
        <Button variant="contained" onClick={handleSubmit}>Submit</Button>
        {countdownMsg && (
          <Typography color="primary" sx={{ mt: 2 }}>
            {countdownMsg}
          </Typography>
        )}
      </Box>
    </LocalizationProvider>
  );
};

export default ScheduleScan;
