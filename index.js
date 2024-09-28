const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const mongoose = require('mongoose');
const bodyParser = require('body-parser');

const app = express();
const port = 3000;

// MongoDB connection
const mongoURI = 'mongodb+srv://josephindavidlatha:dbpass@cluster0.kbgmw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';
mongoose.connect(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => console.log('Connected to MongoDB Atlas'))
    .catch(err => console.error('MongoDB connection error:', err));

app.use(bodyParser.json());

// Trade schema
const tradeSchema = new mongoose.Schema({
    UTC_Time: { type: Date, required: true },
    Operation: { type: String, required: true },
    Market: { type: String, required: true },
    Buy_Sell_Amount: { type: Number, required: true },
    Price: { type: Number, required: true }
});

const Trade = mongoose.model('Trade', tradeSchema);

// Multer storage configuration 
const storage = multer.diskStorage({
    destination: 'uploads/',
    filename: (req, file, cb) => {
        cb(null, file.fieldname + '-' + Date.now() + path.extname(file.originalname));
    }
});

const upload = multer({ storage: storage });

// Upload route to handle CSV file
app.post('/upload', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded.' });
    }

    const filePath = req.file.path;

    const results = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
            const trade = new Trade({
                UTC_Time: new Date(data.UTC_Time),
                Operation: data.Operation,
                Market: data.Market,
                Buy_Sell_Amount: parseFloat(data['Buy/Sell Amount']),
                Price: parseFloat(data.Price)
            });
            results.push(trade);
        })
        .on('end', () => {
            Trade.insertMany(results)
                .then(() => {
                    res.status(201).json({ message: 'CSV data uploaded and saved to MongoDB!' });
                })
                .catch(err => {
                    console.error('Error inserting into MongoDB:', err);
                    res.status(500).json({ message: 'Error saving to MongoDB', error: err });
                });
        });
});

// Route to handle balance calculation
app.post('/balance', async (req, res) => {
    const { timestamp } = req.body;

    if (!timestamp) {
        return res.status(400).json({ message: 'Timestamp is required.' });
    }

    const date = new Date(timestamp);

    try {
        const trades = await Trade.find({ UTC_Time: { $lte: date } });

        const balances = {};

        trades.forEach((trade) => {
            const [asset] = trade.Market.split('/'); 
            if (!balances[asset]) {
                balances[asset] = 0;
            }

            if (trade.Operation === 'BUY') {
                balances[asset] += trade.Buy_Sell_Amount;
            } else if (trade.Operation === 'SELL') {
                balances[asset] -= trade.Buy_Sell_Amount;
            }
        });

        res.status(200).json(balances);
    } catch (error) {
        console.error('Error fetching balance:', error);
        res.status(500).json({ message: 'Error fetching balance' });
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});

