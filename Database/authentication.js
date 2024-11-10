const crypto = require('crypto');
const config = require('../config');

function generateSessionToken() {
    return crypto.randomBytes(32).toString('hex');
}

async function handleAuthentication(data, ws, sessions) {
    const { username, password } = data;
    
    if (!username || !password) {
        throw new Error('Missing credentials');
    }

    // In a real-world scenario, you would check against a database of users
    if (username !== config.credentials.username || password !== config.credentials.password) {
        throw new Error('Invalid credentials');
    }

    const sessionToken = generateSessionToken();
    
    ws.sessionToken = sessionToken;
    ws.isAuthenticated = true;
    
    sessions.set(sessionToken, {
        username,
        timestamp: Date.now()
    });

    return { sessionToken };
}

module.exports = { handleAuthentication };