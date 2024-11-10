// config.js
module.exports = {
    serverIP: 'localhost',
    port: 8080,
    ipWhitelist: false,
    whitelistedIps: ['127.0.0.1'],
    ipBlacklist: false,
    blacklistedIps: [],
    maxConnections: 100,
    encryptionKey: 'your-secret-encryption-key',
    credentials: {
        username: 'admin',
        password: 'admin123'
    },
    backupInterval: 24 * 60 * 60 * 1000, // 24 hours in milliseconds
    backupRetentionDays: 7 // Keep backups for 7 days
};