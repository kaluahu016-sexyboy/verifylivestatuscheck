const express = require('express');
const mongoose = require('mongoose');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI; 

// 1. Connect to MongoDB Atlas
mongoose.connect(MONGO_URI)
  .then(() => console.log("Connected to Cloud Database"))
  .catch(err => console.error("Database connection error:", err));

// 2. Database Schema (Tracks Exact Times)
const srnSchema = new mongoose.Schema({
    srn: { type: String, unique: true },
    status: { type: String, default: 'Pending' },
    timeEntered: { type: Date, default: Date.now },
    timeApproved: { type: Date, default: null },
    durationSeconds: { type: Number, default: null }
});
const SrnModel = mongoose.model('SRN', srnSchema);

// Middleware
app.use(express.json());
app.use(express.static('public'));

// 3. API Routes for Frontend Dashboard
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

// Serve Web Dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// UptimeRobot Keep-Alive Route
app.get('/api/ping', (req, res) => res.send("Alive"));

// 4. Diagnostic Route (To test if UIDAI blocked Render)
app.get('/api/test-connection', (req, res) => {
    const testUrl = "wss://aadhaarmitra.uidai.gov.in/ws/v/216c7370-d2ed-43ef-8c94-526a57b21f6d32289672";
    const ws = new WebSocket(testUrl);
    
    // Set a 5-second timeout in case UIDAI drops the connection silently
    const timeout = setTimeout(() => {
        ws.terminate();
        res.send("FAILED: Connection timed out. UIDAI is likely blocking Render's IP.");
    }, 5000);

    ws.on('open', () => {
        clearTimeout(timeout);
        ws.close();
        res.send("SUCCESS: Render successfully connected to UIDAI.");
    });

    ws.on('error', (err) => {
        clearTimeout(timeout);
        res.send(`FAILED: Connection blocked or failed. Error: ${err.message}`);
    });
});

// 5. Background Worker (Runs Continuously)
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
        const payload = {
            formType: "SINGLE_FORM",
            form_data: { eid: srn, handler: "check_update_status" },
            type: "FORM", 
            sendType: "visitor.form.submit", 
            language: "eng"
        };
        
        ws.on('open', () => ws.send(JSON.stringify(payload)));
        
        ws.on('message', (data) => {
            let response = data.toString();
            try { 
                const parsed = JSON.parse(response);
                response = parsed.content || response; 
            } catch(e) {}
            ws.close();
            resolve(response);
        });
        
        ws.on('error', (err) => { 
            ws.close(); 
            reject(err); 
        });
    });
}

async function processQueue() {
    try {
        // Find the oldest pending SRN
        const task = await SrnModel.findOne({ status: 'Pending' }).sort({ timeEntered: 1 });
        
        // If queue is empty, wait 5 seconds and check again
        if (!task) {
            return setTimeout(processQueue, 5000);
        }

        // Pick a random WebSocket URL and check the SRN
        const wsUrl = WS_URLS[Math.floor(Math.random() * WS_URLS.length)];
        const result = await checkSRN(task.srn, wsUrl);
        
        // Process the result
        if (result.includes("Your Aadhaar has been updated")) {
            task.status = 'Approved';
            task.timeApproved = new Date();
            task.durationSeconds = Math.round((task.timeApproved - task.timeEntered) / 1000);
            await task.save();
            console.log(`SRN ${task.srn} Approved!`);
        } else if (result.toLowerCase().includes("reject")) {
            task.status = 'Rejected';
            task.timeApproved = new Date();
            task.durationSeconds = Math.round((task.timeApproved - task.timeEntered) / 1000);
            await task.save();
            console.log(`SRN ${task.srn} Rejected!`);
        } else {
            console.log(`SRN ${task.srn} still pending. Result: ${result}`);
        }
    } catch (error) {
        console.error("Worker Error:", error.message);
    }
    
    // Safety Delay: Always wait 10 seconds before making the next API call
    setTimeout(processQueue, 10000); 
}

// 6. Start the Server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    processQueue(); // Kick off the background loop
});
