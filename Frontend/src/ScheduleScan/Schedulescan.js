

import React, { useState, useEffect, useRef } from 'react';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { DateTimePicker } from '@mui/x-date-pickers/DateTimePicker';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { CircularProgress } from '@mui/material';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import axios from 'axios';
import { useNavigate } from "react-router-dom";
import './Schedulescan.css';

dayjs.extend(utc);
dayjs.extend(timezone);

const steps = ['Schedule', 'Waiting', 'Scan In Progress', 'Completed'];
const timezones = [
  { label: "India (IST)", value: "Asia/Kolkata" },
  { label: "UK (GMT)", value: "Europe/London" },
  { label: "US Eastern (EST)", value: "America/New_York" },
  { label: "US Pacific (PST)", value: "America/Los_Angeles" }
];

const ScheduleScan = () => {
  const [selectedDateTime, setSelectedDateTime] = useState(dayjs());
  const [selectedZone, setSelectedZone] = useState("Asia/Kolkata");
  const [countdownMsg, setCountdownMsg] = useState('');
  const [countdownActive, setCountdownActive] = useState(false);
  const [scanStatus, setScanStatus] = useState('');
  const [step, setStep] = useState(0);
  const [polling, setPolling] = useState(false);
  const intervalRef = useRef(null);
  const navigate = useNavigate();

  // Fetch scan status and scheduled time on mount or zone change
  useEffect(() => {
    const user = JSON.parse(localStorage.getItem("user"));
    if (!user) {
      alert("Please login first!");
      navigate("/signin");
      return;
    }
    axios.get(`http://localhost:8000/last_scan/${user.email}`)
      .then(res => {
        const { status, scheduled_time } = res.data;
        setScanStatus(status);
        if (status === "Pending" && scheduled_time) {
          setStep(1);
          setCountdownActive(true);
          setPolling(false);
          // Convert UTC scheduled_time to selected zone
          setSelectedDateTime(dayjs.utc(scheduled_time).tz(selectedZone));
        } else if (status === "InProgress") {
          setStep(2);
          setCountdownActive(false);
          setPolling(true);
        } else if (status === "Completed") {
          setStep(3);
          setCountdownActive(false);
          setPolling(false);
        } else {
          setStep(0);
          setCountdownActive(false);
          setPolling(false);
        }
      })
      .catch(() => {
        setStep(0);
        setCountdownActive(false);
        setPolling(false);
      });
    // eslint-disable-next-line
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

  // Live countdown effect for "Pending" state
  useEffect(() => {
    if (countdownActive && step === 1) {
      intervalRef.current = setInterval(() => {
        const now = dayjs().tz(selectedZone);
        const diff = selectedDateTime.diff(now);
        setCountdownMsg(`Your scan starts in ${formatCountdown(diff)}`);
        if (diff <= 0) {
          clearInterval(intervalRef.current);
          setCountdownActive(false);
          setCountdownMsg("Scan is starting...");
          setStep(2); // Move to "Scan In Progress"
          setPolling(true);
        }
      }, 1000);
      return () => clearInterval(intervalRef.current);
    }
  }, [countdownActive, selectedDateTime, selectedZone, step]);

  // Poll scan status from backend
  useEffect(() => {
    let pollTimer;
    const user = JSON.parse(localStorage.getItem("user"));
    if (polling && user && user.email) {
      pollTimer = setInterval(async () => {
        try {
          const res = await axios.get(`http://localhost:8000/trigger_status/${user.email}`);
          setScanStatus(res.data.status);
          if (res.data.status === "InProgress") setStep(2);
          if (res.data.status === "Completed") {
            setStep(3);
            setCountdownMsg("Scan is completed! Click Next for details.");
            setPolling(false);
            clearInterval(pollTimer);
          }
        } catch (e) {
          setScanStatus('');
        }
      }, 3000);
    }
    return () => clearInterval(pollTimer);
  }, [polling]);

  // Only allow scheduling if no scan or last scan is completed
  const canSchedule = !scanStatus || scanStatus === "Completed";

  const handleSubmit = async () => {
    try {
      const user = JSON.parse(localStorage.getItem("user"));
      if (!user || !user.email) {
        alert("User not logged in!");
        return;
      }
      // Convert selected time to UTC before sending
      const utcTime = selectedDateTime.tz(selectedZone).utc().format();
      const payload = {
        email: user.email,
        ScheduledTimeStamp: utcTime
      };
      const response = await axios.post("http://localhost:8000/triggers", payload);
      setCountdownActive(true); // Start live countdown
      setStep(1); // Move to "Waiting"
      setScanStatus('Pending');
      setPolling(false);
      alert(response.data.message);
    } catch (error) {
      console.error("Error while scheduling:", error);
      alert(error.response?.data?.detail || "Failed to save schedule");
    }
  };

  const handleNext = () => {
    navigate("/dashboard");
  };

  // When user changes timezone, update displayed time but keep the same UTC
  const handleZoneChange = (e) => {
    const newZone = e.target.value;
    // Convert current UTC time to new zone for display
    setSelectedDateTime(prev =>
      dayjs.utc(prev.utc().format()).tz(newZone)
    );
    setSelectedZone(newZone);
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <div className="scan-bg">
        <div className="scan-card">
          <h2 className="scan-title">Schedule Cloud Scan</h2>
          <div className="scan-stepper">
            {steps.map((label, idx) => (
              <div key={label} className={`scan-step ${step === idx ? "active" : ""}`}>
                <span className="scan-step-label">{label}</span>
                {idx < steps.length - 1 && <span className="scan-step-bar"></span>}
              </div>
            ))}
          </div>
          <div className="scan-form">
            <select
              className="scan-timezone"
              value={selectedZone}
              onChange={handleZoneChange}
              disabled={!canSchedule || step > 0}
            >
              {timezones.map(tz => (
                <option key={tz.value} value={tz.value}>{tz.label}</option>
              ))}
            </select>
            <DateTimePicker
              label="Select Date & Time"
              value={selectedDateTime}
              onChange={(newValue) => setSelectedDateTime(newValue)}
              renderInput={({ inputRef, inputProps, InputProps }) => (
                <div className="scan-datetime-row">
                  <input ref={inputRef} {...inputProps} className="scan-datetime-input" disabled={step > 0 || !canSchedule} />
                  {InputProps?.endAdornment}
                </div>
              )}
              disabled={step > 0 || !canSchedule}
            />
            <button
              className="scan-btn" 
              onClick={handleSubmit}
              disabled={step > 0 || !canSchedule}
            >
              Schedule Scan
            </button>
          </div>
          {countdownMsg && (
            <div className="scan-countdown">{countdownMsg}</div>
          )}
          {step > 0 && step < 3 && (
            <div className="scan-progress">
              <CircularProgress size={28} style={{ marginRight: 12 }} />
              <span>
                {scanStatus === "InProgress" ? "Scan is running..." : "Waiting for scan to start..."}
              </span>
            </div>
          )}
          {step === 3 && (
            <div className="scan-complete">
              <div className="scan-complete-msg">Scan completed successfully!</div>
              <button className="scan-next-btn" onClick={handleNext}>Next</button>
              <button
                className="scan-btn"
                style={{ marginLeft: 110 }}
                onClick={() => {
                  setStep(0);
                  setScanStatus('');
                  setCountdownMsg('');
                  setSelectedDateTime(dayjs());
                  setCountdownActive(false);
                  setPolling(false);
                }}
              >
                Schedule Again
              </button>
            </div>
          )}
          </div>
      </div>
    </LocalizationProvider>
  );
};

export default ScheduleScan;
