// This file serves as the main entry point for the application, where you can initialize the application and set up any necessary configurations.

import express from 'express';

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware for parsing JSON bodies
app.use(express.json());

// Sample route
app.get('/', (req, res) => {
    res.send('Welcome to the Job Board Application!');
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});