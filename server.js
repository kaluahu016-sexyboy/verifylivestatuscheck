const express = require('express');
const mongoose = require('mongoose');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI; 

// Connect to MongoDB
mongoose.connect(MONGO_URI)
  .then(() => console.log("Connected to Cloud Database"))
  .catch(err => console.error("Database connection error:", err));

// Database Memory Schema (Tracks Exact Times)
const srnSchema = new mongoose.Schema({
    srn: { type: String, unique: true },
    status: { type: String, default: 'Pending' },
    timeEntered: { type: Date, default: Date.now },
    timeApproved: { type: Date, default: null },
    durationSeconds: { type: Number, default: null } // The "middle" time
});
const SrnModel = mongoose.model('SRN', srnSchema);

app.use(express.json());
app.use(express.static('public'));

// API: Add a new SRN
app.post('/api/add', async (req, res) => {
    try {
        const newSrn = new SrnModel({ srn: req.body.srn });
        await newSrn.save();
        res.json({ success: true });
    } catch (error) {
        res.status(400).json({ success: false, message: "SRN exists or error" });
    }
});

// API: Fetch all stats for the dashboard
app.get('/api/status', async (req, res) => {
    const data = await SrnModel.find().sort({ timeEntered: -1 });
    res.json(data);
});

// API: Keep-alive ping route
app.get('/api/ping', (req, res) => res.send("Alive"));

// Serve Web Dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---------------------------------------------------------
// BACKGROUND WORKER (Runs continuously)
// ---------------------------------------------------------
const WS_URLS = [
    "wss://aadhaarmitra.uidai.gov.in/ws/v/216c7370-d2ed-43ef-8c94-526a57b21f6d32289672",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/11439326-0558-4f00-b710-f544346b351732289782"
]; // Add the rest of your URLs here

function checkSRN(srn, wsUrl) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(wsUrl);
        const payload = {
            formType: "SINGLE_FORM",
            form_data: { eid: srn, handler: "check_update_status" },
            type: "FORM", sendType: "visitor.form.submit", language: "eng"
        };
        
        ws.on('open', () => ws.send(JSON.stringify(payload)));
        ws.on('message', (data) => {
            let response = data.toString();
            try { response = JSON.parse(response).content || response; } catch(e){}
            ws.close();
            resolve(response);
        });
        ws.on('error', (err) => { ws.close(); reject(err); });
    });
}

async function processQueue() {
    try {
        // Find the oldest pending SRN
        const task = await SrnModel.findOne({ status: 'Pending' }).sort({ timeEntered: 1 });
        if (!task) return setTimeout(processQueue, 5000); // Check again in 5s if queue is empty

        const wsUrl = WS_URLS[Math.floor(Math.random() * WS_URLS.length)];
        const result = await checkSRN(task.srn, wsUrl);
        
        if (result.includes("Your Aadhaar has been updated")) {
            task.status = 'Approved';
            task.timeApproved = new Date(); // Exact time approved
            task.durationSeconds = Math.round((task.timeApproved - task.timeEntered) / 1000); // Exact time in middle
            await task.save();
        } else if (result.toLowerCase().includes("reject")) {
            task.status = 'Rejected';
            task.timeApproved = new Date();
            await task.save();
        }
    } catch (error) {
        console.error("Worker Error");
    }
    setTimeout(processQueue, 10000); // Throttle to prevent IP blocks
}

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    processQueue(); // Start the background loop
});
