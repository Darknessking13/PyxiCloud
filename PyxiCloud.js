const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const config = require('./config');
const { generateKeyPair, computeSharedSecret, encrypt, decrypt } = require('./Database/encryption');
const { handleAuthentication } = require('./Database/authentication');

const schemaDirectory = path.join(__dirname, 'Database', 'schemas');
const collectionsDirectory = path.join(__dirname, 'Database', 'collections');
const backupDirectory = path.join(__dirname,  'Database', 'Backups');

class PyxiCloudServer {
    constructor() {
        this.wss = null;
        this.clients = new Set();
        this.sessions = new Map();
        this.keyPair = generateKeyPair();
        this.schemas = new Map();
        this.backupInterval = null;
    }

    start() {
        console.log(`PyxiCloud Server is Starting on pyx://${config.serverIP}:${config.port}`);
        this.wss = new WebSocket.Server({ 
            port: config.port,
            clientTracking: true,
            handleProtocols: () => 'pyxisdb-protocol',
            maxPayload: config.maxPayloadSize,
            verifyClient: this.verifyClient.bind(this)
        });

        this.wss.on('connection', this.handleConnection.bind(this));
        this.initializeDatabase();
        this.startBackupSystem();
        console.log(`WebSocket server is running on pyx://${config.serverIP}:${config.port}`);
    }

    verifyClient(info, callback) {
        const clientIp = info.req.socket.remoteAddress;

        if (config.ipWhitelist && !config.whitelistedIps.includes(clientIp)) {
            callback(false, 403, 'IP not whitelisted');
            return;
        }

        if (config.ipBlacklist && config.blacklistedIps.includes(clientIp)) {
            callback(false, 403, 'IP blacklisted');
            return;
        }

        if (this.clients.size >= config.maxConnections) {
            callback(false, 503, 'Server at maximum capacity');
            return;
        }

        callback(true);
    }

    initializeDatabase() {
        if (!fs.existsSync(schemaDirectory)) {
            fs.mkdirSync(schemaDirectory, { recursive: true });
        }
        if (!fs.existsSync(collectionsDirectory)) {
            fs.mkdirSync(collectionsDirectory, { recursive: true });
        }
        if (!fs.existsSync(backupDirectory)) {
            fs.mkdirSync(backupDirectory, { recursive: true });
        }
    }

    startBackupSystem() {
        this.backupInterval = setInterval(() => {
            this.createBackup();
            this.deleteOldBackups();
        }, config.backupInterval);
    }

    createBackup() {
        const timestamp = new Date().toISOString().replace(/:/g, '-');
        const backupPath = path.join(backupDirectory, `backup_${timestamp}`);
        fs.mkdirSync(backupPath);

        // Backup schemas
        fs.copyFileSync(schemaDirectory, path.join(backupPath, 'schemas'));

        // Backup collections
        fs.copyFileSync(collectionsDirectory, path.join(backupPath, 'collections'));

        console.log(`Backup created: ${backupPath}`);
    }

    deleteOldBackups() {
        const currentDate = new Date();
        fs.readdirSync(backupDirectory).forEach(backupFolder => {
            const backupPath = path.join(backupDirectory, backupFolder);
            const backupDate = new Date(backupFolder.split('_')[1]);
            const daysSinceBackup = (currentDate - backupDate) / (1000 * 60 * 60 * 24);

            if (daysSinceBackup > config.backupRetentionDays) {
                fs.rmdirSync(backupPath, { recursive: true });
                console.log(`Deleted old backup: ${backupPath}`);
            }
        });
    }

    handleConnection(ws, req) {
        const clientIP = req.socket.remoteAddress;
        console.log('Client connected from:', clientIP);

        ws.isAlive = true;
        ws.isAuthenticated = false;
        ws.sessionToken = null;
        ws.sharedSecret = null;

        this.clients.add(ws);

        // Send server's public key and parameters to the client
        ws.send(JSON.stringify({
            type: 'KeyExchange',
            publicKey: this.keyPair.publicKey,
            prime: this.keyPair.prime,
            generator: this.keyPair.generator
        }));

        ws.on('message', async (message) => {
            try {
                let event = JSON.parse(message);

                if (event.type === 'KeyExchange') {
                    // Compute shared secret
                    const sharedSecret = computeSharedSecret(
                        this.keyPair.privateKey,
                        event.publicKey,
                        this.keyPair.prime,
                        this.keyPair.generator
                    );
                    ws.sharedSecret = sharedSecret;
                    console.log('Key exchange completed');
                    return;
                }

                if (ws.isAuthenticated && ws.sharedSecret) {
                    const decrypted = decrypt(event, ws.sharedSecret);
                    event = JSON.parse(decrypted);
                }

                if (event.type === 'Authenticate') {
                    await handleAuthentication(event.data, ws, this.sessions);
                } else if (!ws.isAuthenticated) {
                    this.sendError(ws, 'Not authenticated', event.requestId);
                } else {
                    await this.handleRequest(event, ws);
                }
            } catch (error) {
                console.error('Error processing message:', error);
                this.sendError(ws, 'Invalid message format');
            }
        });

        ws.on('close', () => {
            console.log('Client disconnected from:', clientIP);
            if (ws.sessionToken) {
                this.sessions.delete(ws.sessionToken);
            }
            this.clients.delete(ws);
        });
    }

    async handleRequest(event, ws) {
        console.log('Handling request:', event.type);
        const { type, data, requestId } = event;

        try {
            switch (type) {
                case 'CreateSchema':
                    await this.createSchema(data, ws, requestId);
                    break;
                case 'UpdateSchema':
                    await this.updateSchema(data, ws, requestId);
                    break;
                case 'Query':
                    await this.handleQuery(data, ws, requestId);
                    break;
                default:
                    throw new Error('Unknown event type');
            }
        } catch (error) {
            this.sendError(ws, error.message, requestId);
        }
    }

    async createSchema(data, ws, requestId) {
        const { collectionName, schemaDefinition } = data;
        if (!this.validateSchemaData(collectionName, schemaDefinition)) {
            this.sendError(ws, 'Invalid schema data', requestId);
            return;
        }

        try {
            this.schemas.set(collectionName, schemaDefinition);
            const schemaPath = path.join(schemaDirectory, `${collectionName}.json`);
            fs.writeFileSync(schemaPath, JSON.stringify(schemaDefinition, null, 2));
            this.sendSuccess(ws, 'Schema created successfully', requestId);
        } catch (error) {
            this.sendError(ws, `Failed to create schema: ${error.message}`, requestId);
        }
    }

    async updateSchema(data, ws, requestId) {
        const { collectionName, schemaDefinition } = data;
        if (!this.validateSchemaData(collectionName, schemaDefinition)) {
            this.sendError(ws, 'Invalid schema data', requestId);
            return;
        }

        try {
            this.schemas.set(collectionName, schemaDefinition);
            const schemaPath = path.join(schemaDirectory, `${collectionName}.json`);
            fs.writeFileSync(schemaPath, JSON.stringify(schemaDefinition, null, 2));
            this.sendSuccess(ws, 'Schema updated successfully', requestId);
        } catch (error) {
            this.sendError(ws, `Failed to update schema: ${error.message}`, requestId);
        }
    }

    validateSchemaData(collectionName, schemaDefinition) {
        return (
            typeof collectionName === 'string' &&
            collectionName.length > 0 &&
            typeof schemaDefinition === 'object' &&
            Object.keys(schemaDefinition).length > 0
        );
    }

    async handleQuery(data, ws, requestId) {
        const { collectionName, operation, ...params } = data;
        if (!this.validateQueryData(collectionName, operation, params)) {
            this.sendError(ws, 'Invalid query data', requestId);
            return;
        }

        try {
            const schema = this.schemas.get(collectionName);
            if (!schema) {
                throw new Error(`Schema for collection "${collectionName}" not found`);
            }

            let result;
            switch (operation) {
                case 'insertOne':
                    result = await this.insertOne(collectionName, params.document, schema);
                    break;
                case 'insertMany':
                    result = await this.insertMany(collectionName, params.documents, schema);
                    break;
                case 'find':
                    result = await this.find(collectionName, params.query || {});
                    break;
                case 'findOne':
                    result = await this.findOne(collectionName, params.query || {});
                    break;
                case 'updateOne':
                    result = await this.updateOne(collectionName, params.query, params.updateFields);
                    break;
                case 'updateMany':
                    result = await this.updateMany(collectionName, params.query, params.updateFields);
                    break;
                case 'deleteOne':
                    result = await this.deleteOne(collectionName, params.query);
                    break;
                case 'deleteMany':
                    result = await this.deleteMany(collectionName, params.query);
                    break;
                default:
                    throw new Error('Invalid operation');
            }
            this.sendSuccess(ws, result, requestId);
        } catch (error) {
            this.sendError(ws, error.message, requestId);
        }
    }

    validateQueryData(collectionName, operation, params) {
        const validOperations = ['insertOne', 'insertMany', 'find', 'findOne', 'updateOne', 'updateMany', 'deleteOne', 'deleteMany'];
        return (
            typeof collectionName === 'string' &&
            collectionName.length > 0 &&
            validOperations.includes(operation) &&
            typeof params === 'object'
        );
    }

    async insertOne(collectionName, document, schema) {
        const validatedDocument = this.validateAndApplyDefaults(document, schema);
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        let collection = [];
        if (fs.existsSync(collectionPath)) {
            collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        }

        // Check for unique constraints
        for (const [field, fieldSchema] of Object.entries(schema)) {
            if (fieldSchema.unique && validatedDocument[field] !== undefined) {
                const isDuplicate = collection.some(doc => doc[field] === validatedDocument[field]);
                if (isDuplicate) {
                    throw new Error(`Duplicate value for unique field "${field}"`);
                }
            }
        }

        collection.push(validatedDocument);
        fs.writeFileSync(collectionPath, JSON.stringify(collection, null, 2));
        return validatedDocument;
    }

    async insertMany(collectionName, documents, schema) {
        const validatedDocuments = documents.map(doc => this.validateAndApplyDefaults(doc, schema));
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        let collection = [];
        if (fs.existsSync(collectionPath)) {
            collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        }

        // Check for unique constraints
        for (const document of validatedDocuments) {
            for (const [field, fieldSchema] of Object.entries(schema)) {
                if (fieldSchema.unique && document[field] !== undefined) {
                    const isDuplicate = collection.some(doc => doc[field] === document[field]);
                    if (isDuplicate) {
                        throw new Error(`Duplicate value for unique field "${field}"`);
                    }
                }
            }
        }

        collection.push(...validatedDocuments);
        fs.writeFileSync(collectionPath, JSON.stringify(collection, null, 2));
        return validatedDocuments;
    }

    validateAndApplyDefaults(document, schema) {
        const validatedDocument = { ...document };
        for (const [field, fieldSchema] of Object.entries(schema)) {
            if (!(field in validatedDocument)) {
                if ('default' in fieldSchema) {
                    validatedDocument[field] = typeof fieldSchema.default === 'function' 
                        ? fieldSchema.default() 
                        : fieldSchema.default;
                } else if (fieldSchema.required) {
                    throw new Error(`Required field "${field}" is missing`);
                }
            }
        }
        return validatedDocument;
    }

    async find(collectionName, query) {
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        if (!fs.existsSync(collectionPath)) {
            return [];
        }
        const collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        return collection.filter(doc => this.matchQuery(doc, query));
    }

    async findOne(collectionName, query) {
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        if (!fs.existsSync(collectionPath)) {
            return null;
        }
        const collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        return collection.find(doc => this.matchQuery(doc, query)) || null;
    }

    async updateOne(collectionName, query, updateFields) {
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        if (!fs.existsSync(collectionPath)) {
            return { matchedCount: 0, modifiedCount: 0 };
        }
        let collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        const schema = this.schemas.get(collectionName);

        const index = collection.findIndex(doc => this.matchQuery(doc, query));
        if (index !== -1) {
            const updatedDocument = { ...collection[index], ...updateFields };

            // Check for unique constraints
            for (const [field, fieldSchema] of Object.entries(schema)) {
                if (fieldSchema.unique && updateFields[field] !== undefined) {
                    const isDuplicate = collection.some((doc, i) => 
                        i !== index && doc[field] === updateFields[field]
                    );
                    if (isDuplicate) {
                        throw new Error(`Duplicate value for unique field "${field}"`);
                    }
                }
            }

            collection[index] = updatedDocument;
            fs.writeFileSync(collectionPath, JSON.stringify(collection, null, 2));
            return { matchedCount: 1, modifiedCount: 1 };
        }
        return { matchedCount: 0, modifiedCount: 0 };
    }

    async updateMany(collectionName, query, updateFields) {
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        if (!fs.existsSync(collectionPath)) {
            return { matchedCount: 0, modifiedCount: 0 };
        }
        let collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        const schema = this.schemas.get(collectionName);

        let modifiedCount = 0;
        const updatedCollection = collection.map((doc, index) => {
            if (this.matchQuery(doc, query)) {
                const updatedDocument = { ...doc, ...updateFields };

                // Check for unique constraints
                for (const [field, fieldSchema] of Object.entries(schema)) {
                    if (fieldSchema.unique && updateFields[field] !== undefined) {
                        const isDuplicate = collection.some((otherDoc, i) => 
                            i !== index && otherDoc[field] === updateFields[field]
                        );
                        if (isDuplicate) {
                            throw new Error(`Duplicate value for unique field "${field}"`);
                        }
                    }
                }

                modifiedCount++;
                return updatedDocument;
            }
            return doc;
        });

        fs.writeFileSync(collectionPath, JSON.stringify(updatedCollection, null, 2));
        return { matchedCount: modifiedCount, modifiedCount };
    }

    async deleteOne(collectionName, query) {
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        if (!fs.existsSync(collectionPath)) {
            return { deletedCount: 0 };
        }
        let collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        const initialLength = collection.length;
        collection = collection.filter(doc => !this.matchQuery(doc, query));
        fs.writeFileSync(collectionPath, JSON.stringify(collection, null, 2));
        return { deletedCount: initialLength - collection.length };
    }

    async deleteMany(collectionName, query) {
        const collectionPath = path.join(collectionsDirectory, `${collectionName}.json`);
        if (!fs.existsSync(collectionPath)) {
            return { deletedCount: 0 };
        }
        let collection = JSON.parse(fs.readFileSync(collectionPath, 'utf-8'));
        const initialLength = collection.length;
        collection = collection.filter(doc => !this.matchQuery(doc, query));
        fs.writeFileSync(collectionPath, JSON.stringify(collection, null, 2));
        return { deletedCount: initialLength - collection.length };
    }

    matchQuery(doc, query) {
        for (const [key, value] of Object.entries(query)) {
            if (typeof value === 'object' && value !== null) {
                if (!this.matchOperators(doc[key], value)) {
                    return false;
                }
            } else if (doc[key] !== value) {
                return false;
            }
        }
        return true;
    }

    matchOperators(fieldValue, operators) {
        for (const [operator, operand] of Object.entries(operators)) {
            switch (operator) {
                case '$eq':
                    if (fieldValue !== operand) return false;
                    break;
                case '$ne':
                    if (fieldValue === operand) return false;
                    break;
                case '$gt':
                    if (!(fieldValue > operand)) return false;
                    break;
                case '$gte':
                    if (!(fieldValue >= operand)) return false;
                    break;
                case '$lt':
                    if (!(fieldValue < operand)) return false;
                    break;
                case '$lte':
                    if (!(fieldValue <= operand)) return false;
                    break;
                case '$in':
                    if (!Array.isArray(operand) || !operand.includes(fieldValue)) return false;
                    break;
                case '$nin':
                    if (!Array.isArray(operand) || operand.includes(fieldValue)) return false;
                    break;
                default:
                    throw new Error(`Unsupported operator: ${operator}`);
            }
        }
        return true;
    }

    sendSuccess(ws, data, requestId) {
        const response = JSON.stringify({ 
            status: 'success', 
            data,
            requestId 
        });
        if (ws.isAuthenticated && ws.sharedSecret) {
            ws.send(encrypt(response, ws.sharedSecret));
        } else {
            ws.send(response);
        }
    }

    sendError(ws, message, requestId) {
        const response = JSON.stringify({ 
            status: 'error', 
            message,
            requestId 
        });
        if (ws.isAuthenticated && ws.sharedSecret) {
            ws.send(encrypt(response, ws.sharedSecret));
        } else {
            ws.send(response);
        }
    }
}

const server = new PyxiCloudServer();
server.start();

module.exports = PyxiCloudServer;