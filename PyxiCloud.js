// PyxiCloud.js
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const config = require('./config');

const schemaDirectory = path.join(__dirname, 'Database', 'schemas');
const collectionsDirectory = path.join(__dirname, 'Database', 'collections');
const backupDirectory = path.join(__dirname, 'Database', 'backups');

if (!fs.existsSync(schemaDirectory)) {
    fs.mkdirSync(schemaDirectory, { recursive: true });
}
if (!fs.existsSync(collectionsDirectory)) {
    fs.mkdirSync(collectionsDirectory, { recursive: true });
}
if (!fs.existsSync(backupDirectory)) {
    fs.mkdirSync(backupDirectory, { recursive: true });
}

const ENCRYPTION_ALGORITHM = 'aes-256-cbc';

class PyxiCloudServer {
    constructor() {
        this.wss = null;
        this.clients = new Set();
        this.heartbeatInterval = 30000;
        this.maxPayloadSize = 1024 * 1024;
        this.rateLimiter = new Map();
        this.schemas = new Map();
        this.sessions = new Map();
        this.backupInterval = null;
    }

    start() {
        this.wss = new WebSocket.Server({ 
            port: config.port,
            clientTracking: true,
            handleProtocols: () => 'pyxisdb-protocol',
            maxPayload: this.maxPayloadSize
        });

        this.wss.on('connection', this.handleConnection.bind(this));

        console.log(`WebSocket server is running on ws://${config.serverIP}:${config.port}`);

        this.startBackupProcess();
    }

    startBackupProcess() {
        this.backupInterval = setInterval(() => {
            this.createBackup();
            this.cleanupOldBackups();
        }, config.backupInterval);
    }


    createBackup() {
        const timestamp = new Date().toISOString().replace(/:/g, '-');
        const backupPath = path.join(backupDirectory, `backup_${timestamp}`);
        fs.mkdirSync(backupPath);

        // Backup schemas
        fs.copyFileSync(
            path.join(schemaDirectory, 'schemas.json'),
            path.join(backupPath, 'schemas.json')
        );

        // Backup collections
        fs.readdirSync(collectionsDirectory).forEach(file => {
            fs.copyFileSync(
                path.join(collectionsDirectory, file),
                path.join(backupPath, file)
            );
        });

        console.log(`Backup created: ${backupPath}`);
    }

    cleanupOldBackups() {
        const now = new Date();
        fs.readdirSync(backupDirectory).forEach(backupFolder => {
            const backupPath = path.join(backupDirectory, backupFolder);
            const stats = fs.statSync(backupPath);
            const diffDays = (now.getTime() - stats.mtime.getTime()) / (1000 * 3600 * 24);
            
            if (diffDays > config.backupRetentionDays) {
                fs.rmSync(backupPath, { recursive: true, force: true });
                console.log(`Old backup removed: ${backupPath}`);
            }
        });
    }

    handleConnection(ws, req) {
        const clientIP = req.socket.remoteAddress;
        console.log('Client connected from:', clientIP);

        if (this.checkAccessDenied(clientIP, ws)) {
            return;
        }

        ws.binaryType = 'arraybuffer';
        ws.isAlive = true;
        ws.isAuthenticated = false;
        ws.sessionToken = null;

        this.clients.add(ws);

        ws.on('pong', () => {
            ws.isAlive = true;
            console.log('Received pong from client:', clientIP);
        });

        ws.on('message', async (message) => {
            if (!this.checkRateLimit(clientIP)) {
                this.sendError(ws, 'Rate limit exceeded. Please try again later.');
                return;
            }
        
            try {
                const event = JSON.parse(message.toString());
        
                if (!this.validateEvent(event)) {
                    this.sendError(ws, 'Invalid event format');
                    return;
                }
        
                if (event.type === 'Authenticate') {
                    await this.handleAuthentication(event.data, ws, event.requestId);
                    return;
                }
        
                if (!ws.isAuthenticated) {
                    this.sendError(ws, 'Not authenticated', event.requestId);
                    return;
                }
        
                await this.handleRequest(event, ws);
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

        ws.on('error', (error) => {
            console.error('WebSocket error:', error);
            if (ws.sessionToken) {
                this.sessions.delete(ws.sessionToken);
            }
            this.clients.delete(ws);
        });
    }

    checkAccessDenied(clientIP, ws) {
        if (config.ipWhitelist && !config.whitelistedIps.includes(clientIP)) {
            this.sendError(ws, 'Access denied. IP not in whitelist.');
            ws.close();
            return true;
        }
        if (config.ipBlacklist && config.blacklistedIps.includes(clientIP)) {
            this.sendError(ws, 'Access denied. IP in blacklist.');
            ws.close();
            return true;
        }
        return false;
    }

    checkRateLimit(clientIP) {
        const now = Date.now();
        const limit = 100;
        const interval = 60000;

        if (!this.rateLimiter.has(clientIP)) {
            this.rateLimiter.set(clientIP, []);
        }

        const clientRequests = this.rateLimiter.get(clientIP);
        const recentRequests = clientRequests.filter(time => now - time < interval);

        if (recentRequests.length >= limit) {
            return false;
        }

        recentRequests.push(now);
        this.rateLimiter.set(clientIP, recentRequests);
        return true;
    }

    validateEvent(event) {
        return (
            event &&
            typeof event === 'object' &&
            typeof event.type === 'string' &&
            typeof event.requestId === 'string' &&
            event.data !== undefined
        );
    }

    startHeartbeat() {
        setInterval(() => {
            this.clients.forEach(ws => {
                if (ws.isAlive === false) {
                    this.clients.delete(ws);
                    return ws.terminate();
                }
                
                ws.isAlive = false;
                ws.ping('', false, (error) => {
                    if (error) console.error('Ping error:', error);
                });
            });
        }, this.heartbeatInterval);
    }

    async handleAuthentication(data, ws, requestId) {
        const { username, password } = data;
        
        if (!username || !password) {
            this.sendError(ws, 'Missing credentials', requestId);
            return;
        }

        if (username !== config.credentials.username || password !== config.credentials.password) {
            this.sendError(ws, 'Invalid credentials', requestId);
            return;
        }

        const sessionToken = this.generateSessionToken();
        
        ws.sessionToken = sessionToken;
        ws.isAuthenticated = true;
        
        this.sessions.set(sessionToken, {
            username,
            timestamp: Date.now()
        });

        this.sendSuccess(ws, { sessionToken }, requestId);
    }

    generateSessionToken() {
        return crypto.randomBytes(32).toString('hex');
    }

    encrypt(data, key) {
        const iv = crypto.randomBytes(16);
        const cipher = crypto.createCipheriv(ENCRYPTION_ALGORITHM, key, iv);
        let encrypted = cipher.update(data, 'utf8', 'hex');
        encrypted += cipher.final('hex');
        return iv.toString('hex') + ':' + encrypted;
    }

    decrypt(data, key) {
        const [ivHex, encryptedData] = data.split(':');
        const iv = Buffer.from(ivHex, 'hex');
        const decipher = crypto.createDecipheriv(ENCRYPTION_ALGORITHM, key, iv);
        let decrypted = decipher.update(encryptedData, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        return decrypted;
    }

    handleRequest(event, ws) {
        console.log('Handling request:', event.type);
        try {
            const { requestId } = event;
            switch (event.type) {
                case 'CreateSchema':
                    console.log('Processing CreateSchema request');
                    this.createSchema(event.data, ws, requestId);
                    break;
                case 'UpdateSchema':
                    console.log('Processing UpdateSchema request');
                    this.updateSchema(event.data, ws, requestId);
                    break;
                case 'Query':
                    console.log('Processing Query request:', event.data.operation);
                    this.handleQuery(event.data, ws, requestId);
                    break;
                default:
                    console.log('Unknown event type:', event.type);
                    this.sendError(ws, 'Unknown event type', requestId);
            }
        } catch (error) {
            console.error('Error handling event:', error);
            this.sendError(ws, error.message, event.requestId);
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
            collectionName.length > 0  &&
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
        ws.send(response);
    }

    sendError(ws, message, requestId) {
        const response = JSON.stringify({ 
            status: 'error', 
            message,
            requestId 
        });
        ws.send(response);
    }
}

const server = new PyxiCloudServer();
server.start();
server.startHeartbeat();

module.exports = PyxiCloudServer;