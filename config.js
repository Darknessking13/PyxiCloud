// config.js
module.exports = {
    serverIP: 'localhost',
    port: 8080,
    username: 'admin',
    password: 'adminpassword',
    subUsers: [
        { username: 'user1', password: 'pass1' },
        { username: 'user2', password: 'pass2' }
    ],
    ipWhitelist: false,
    whitelistedIps: ['127.0.0.1'],
    ipBlacklist: false,
    blacklistedIps: [],
    maxConnections: 100
};