const express = require('express');
const mongoose = require('mongoose');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI; 

mongoose.connect(MONGO_URI)
  .then(() => console.log("Connected to Cloud Database"))
  .catch(err => console.error("Database connection error:", err));

// Updated Schema with Live Logging
const srnSchema = new mongoose.Schema({
    srn: { type: String, unique: true },
    status: { type: String, default: 'Pending' },
    timeEntered: { type: Date, default: Date.now },
    timeApproved: { type: Date, default: null },
    durationSeconds: { type: Number, default: null },
    lastMessage: { type: String, default: 'Added to queue...' },
    lastCheckedAt: { type: Date, default: Date.now }
});
const SrnModel = mongoose.model('SRN', srnSchema);

app.use(express.json());
app.use(express.static('public'));

app.post('/api/add', async (req, res) => {
    try {
        const newSrn = new SrnModel({ srn: req.body.srn });
        await newSrn.save();
        res.json({ success: true });
    } catch (error) {
        res.status(400).json({ success: false, message: "SRN exists or database error" });
    }
});

app.get('/api/status', async (req, res) => {
    try {
        const data = await SrnModel.find().sort({ timeEntered: -1 });
        res.json(data);
    } catch (error) {
        res.status(500).json({ error: "Failed to fetch status" });
    }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/api/ping', (req, res) => res.send("Alive"));

const WS_URLS = [
    "wss://aadhaarmitra.uidai.gov.in/ws/v/216c7370-d2ed-43ef-8c94-526a57b21f6d32289672",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/11439326-0558-4f00-b710-f544346b351732289782",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/5cc7bd07-b123-47a2-8fc1-43d9cb2fd56032289821",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/6a1f4e24-177d-434b-a3bb-a25d7ba98fcb32289848",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/e36b188d-e5c0-4091-adeb-7b7c3d9e446132289872",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/d4fc58bc-1aa7-412d-805d-73aca6410bdd32289898",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/fe13c1e3-469c-4148-aecd-f871962216e732289916",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/72d49330-d3f3-465a-80e8-ee898dae1fcb32289936",
    "wss://aadhaarmitra.uidai.gov.in/ws/v/43e56a25-d0da-4d8e-9e10-36a9cbff573e32289953"
];

function checkSRN(srn, wsUrl) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(wsUrl);
        let isDone = false;

        // Failsafe: Prevent infinite hanging if UIDAI drops the connection silently
        const timeout = setTimeout(() => {
            if (isDone) return;
            isDone = true;
            ws.terminate();
            reject(new Error("Timeout: UIDAI did not respond within 10 seconds"));
        }, 10000);

        const payload = {
            formType: "SINGLE_FORM",
            form_data: { eid: srn, handler: "check_update_status" },
            type: "FORM", sendType: "visitor.form.submit", language: "eng"
        };
        
        ws.on('open', () => ws.send(JSON.stringify(payload)));
        
        ws.on('message', (data) => {
            if (isDone) return;
            isDone = true;
            clearTimeout(timeout);
            let response = data.toString();
            try { 
                const parsed = JSON.parse(response);
                response = parsed.content || response; 
            } catch(e) {}
            ws.close();
            resolve(response);
        });
        
        ws.on('error', (err) => { 
            if (isDone) return;
            isDone = true;
            clearTimeout(timeout);
            ws.close(); 
            reject(err); 
        });
    });
}

async function processQueue() {
    let activeTask = null;
    try {
        // Find oldest Pending SRN, OR a Checking SRN that got stuck for over 60 seconds
        const cutoff = new Date(Date.now() - 60000);
        activeTask = await SrnModel.findOne({
            $or: [
                { status: 'Pending' },
                { status: 'Checking', lastCheckedAt: { $lt: cutoff } }
            ]
        }).sort({ lastCheckedAt: 1 });

        if (!activeTask) return setTimeout(processQueue, 5000);

        // Update UI to show we are actively trying
        activeTask.status = 'Checking';
        activeTask.lastCheckedAt = new Date();
        activeTask.lastMessage = 'Connecting to UIDAI WebSocket...';
        await activeTask.save();

        const wsUrl = WS_URLS[Math.floor(Math.random() * WS_URLS.length)];
        const result = await checkSRN(activeTask.srn, wsUrl);
        
        if (result.includes("Your Aadhaar has been updated")) {
            activeTask.status = 'Approved';
            activeTask.timeApproved = new Date();
            activeTask.durationSeconds = Math.round((activeTask.timeApproved - activeTask.timeEntered) / 1000);
            activeTask.lastMessage = "SUCCESS: " + result;
        } else if (result.toLowerCase().includes("reject")) {
            activeTask.status = 'Rejected';
            activeTask.timeApproved = new Date();
            activeTask.durationSeconds = Math.round((activeTask.timeApproved - activeTask.timeEntered) / 1000);
            activeTask.lastMessage = "REJECTED: " + result;
        } else {
            activeTask.status = 'Pending';
            activeTask.lastMessage = "PENDING (UIDAI says): " + result;
        }
        await activeTask.save();

    } catch (error) {
        console.error("Worker Error:", error.message);
        if (activeTask) {
            activeTask.status = 'Pending';
            activeTask.lastMessage = `ERROR: ${error.message} - Retrying soon...`;
            await activeTask.save();
        }
    }
    
    setTimeout(processQueue, 10000); 
}

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    processQueue();
});
