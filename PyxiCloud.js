const WebSocket = require('ws');
const pyx = require('./Database/index');
const fs = require('fs');
const path = require('path');
const config = require('./config');
const crypto = require('crypto');

const schemaDirectory = path.join(__dirname, 'Database', 'schemas');
const collectionsDirectory = path.join(__dirname, 'Database', 'collections');

// Create directories if they don't exist
if (!fs.existsSync(schemaDirectory)) {
    fs.mkdirSync(schemaDirectory, { recursive: true });
}
if (!fs.existsSync(collectionsDirectory)) {
    fs.mkdirSync(collectionsDirectory, { recursive: true });
}

class PyxiCloudServer {
    constructor() {
        this.wss = null;
        this.clients = new Set();
        this.heartbeatInterval = 30000; // 30 seconds
        this.maxPayloadSize = 1024 * 1024; // 1MB
        this.rateLimiter = new Map();
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
    }

    handleConnection(ws, req) {
        const clientIP = req.socket.remoteAddress;
        console.log('Client connected from:', clientIP);

        if (this.checkAccessDenied(clientIP, ws)) {
            return;
        }

        ws.binaryType = 'arraybuffer';
        ws.isAlive = true;

        this.clients.add(ws);

        ws.on('pong', () => {
            ws.isAlive = true;
            console.log('Received pong from client:', clientIP);
        });

        ws.on('message', async (message) => {
            if (!this.checkRateLimit(clientIP)) {
                sendError(ws, 'Rate limit exceeded. Please try again later.');
                return;
            }

            console.log('Received message from client:', message.toString());
            try {
                const event = JSON.parse(message.toString());
                if (!this.validateEvent(event)) {
                    sendError(ws, 'Invalid event format');
                    return;
                }
                console.log('Parsed event:', event);
                await this.handleRequest(event, ws);
            } catch (error) {
                console.error('Error processing message:', error);
                sendError(ws, 'Invalid message format');
            }
        });

        ws.on('close', () => {
            console.log('Client disconnected from:', clientIP);
            this.clients.delete(ws);
        });

        ws.on('error', (error) => {
            console.error('WebSocket error:', error);
            this.clients.delete(ws);
        });
    }

    checkAccessDenied(clientIP, ws) {
        if (config.ipWhitelist && !config.whitelistedIps.includes(clientIP)) {
            sendError(ws, 'Access denied. IP not in whitelist.');
            ws.close();
            return true;
        }
        if (config.ipBlacklist && config.blacklistedIps.includes(clientIP)) {
            sendError(ws, 'Access denied. IP in blacklist.');
            ws.close();
            return true;
        }
        return false;
    }

    checkRateLimit(clientIP) {
        const now = Date.now();
        const limit = 100; // 100 requests
        const interval = 60000; // per minute

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

    async handleRequest(event, ws) {
        console.log('Handling request:', event.type);
        try {
            const { requestId } = event;
            switch (event.type) {
                case 'CreateSchema':
                    console.log('Processing CreateSchema request');
                    await this.createSchema(event.data, ws, requestId);
                    break;
                case 'UpdateSchema':
                    console.log('Processing UpdateSchema request');
                    await this.updateSchema(event.data, ws, requestId);
                    break;
                case 'Query':
                    console.log('Processing Query request:', event.data.operation);
                    await this.handleQuery(event.data, ws, requestId);
                    break;
                default:
                    console.log('Unknown event type:', event.type);
                    sendError(ws, 'Unknown event type', requestId);
            }
        } catch (error) {
            console.error('Error handling event:', error);
            sendError(ws, error.message, event.requestId);
        }
    }

    async createSchema(data, ws, requestId) {
        const { collectionName, schemaDefinition } = data;
        if (!this.validateSchemaData(collectionName, schemaDefinition)) {
            sendError(ws, 'Invalid schema data', requestId);
            return;
        }

        const schemaPath = path.join(schemaDirectory, `${this.sanitizeFileName(collectionName)}.json`);

        try {
            if (fs.existsSync(schemaPath)) {
                sendError(ws, 'Schema already exists', requestId);
            } else {
                await fs.promises.writeFile(schemaPath, JSON.stringify(schemaDefinition, null, 2));
                sendSuccess(ws, 'Schema created successfully', requestId);
            }
        } catch (error) {
            sendError(ws, `Failed to create schema: ${error.message}`, requestId);
        }
    }

    async updateSchema(data, ws, requestId) {
        const { collectionName, schemaDefinition } = data;
        if (!this.validateSchemaData(collectionName, schemaDefinition)) {
            sendError(ws, 'Invalid schema data', requestId);
            return;
        }

        const schemaPath = path.join(schemaDirectory, `${this.sanitizeFileName(collectionName)}.json`);

        try {
            if (!fs.existsSync(schemaPath)) {
                sendError(ws, 'Schema does not exist', requestId);
                return;
            }

            await fs.promises.writeFile(schemaPath, JSON.stringify(schemaDefinition, null, 2));
            sendSuccess(ws, 'Schema updated successfully', requestId);
        } catch (error) {
            sendError(ws, `Failed to update schema: ${error.message}`, requestId);
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

    sanitizeFileName(fileName) {
        return fileName.replace(/[^a-z0-9]/gi, '_').toLowerCase();
    }

    async handleQuery(data, ws, requestId) {
        const { collectionName, operation, ...params } = data;
        if (!this.validateQueryData(collectionName, operation, params)) {
            sendError(ws, 'Invalid query data', requestId);
            return;
        }

        const schemaPath = path.join(schemaDirectory, `${this.sanitizeFileName(collectionName)}.json`);
        const collectionPath = path.join(collectionsDirectory, `${this.sanitizeFileName(collectionName)}.json`);

        try {
            await this.ensureCollectionExists(collectionPath);
            let result;

            switch (operation) {
                case 'insert':
                    result = await this.insertDocument(collectionPath, params.document);
                    break;
                case 'find':
                    result = await this.findDocuments(collectionPath, params.query || {});
                    break;
                case 'update':
                    result = await this.updateDocuments(collectionPath, params.query, params.updateFields);
                    break;
                case 'delete':
                    result = await this.deleteDocuments(collectionPath, params.query);
                    break;
                case 'count':
                    result = await this.countDocuments(collectionPath, params.query || {});
                    break;
                default:
                    throw new Error('Invalid operation');
            }
            sendSuccess(ws, result, requestId);
        } catch (error) {
            sendError(ws, error.message, requestId);
        }
    }

    validateQueryData(collectionName, operation, params) {
        const validOperations = ['insert', 'find', 'update', 'delete', 'count'];
        return (
            typeof collectionName === 'string' &&
            collectionName.length > 0 &&
            validOperations.includes(operation) &&
            typeof params === 'object'
        );
    }

    async ensureCollectionExists(collectionPath) {
        try {
            await fs.promises.access(collectionPath);
        } catch (error) {
            if (error.code === 'ENOENT') {
                await fs.promises.writeFile(collectionPath, JSON.stringify([], null, 2));
            } else {
                throw error;
            }
        }
    }

    async insertDocument(collectionPath, document) {
        await this.ensureCollectionExists(collectionPath);
        const data = await fs.promises.readFile(collectionPath, 'utf-8');
        let documents = JSON.parse(data);
        
        // Remove _id and createdAt if they exist
        delete document._id;
        delete document.createdAt;
        
        // Check for duplicates
        const isDuplicate = documents.some(doc => this.isEqual(doc, document));
        if (isDuplicate) {
            throw new Error('Duplicate document');
        }
        
        documents.push(document);
        await fs.promises.writeFile(collectionPath, JSON.stringify(documents, null, 2));
        return document;
    }

    isEqual(obj1, obj2) {
        return JSON.stringify(obj1) === JSON.stringify(obj2);
    }

    async findDocuments(collectionPath, query) {
        await this.ensureCollectionExists(collectionPath);
        const data = await fs.promises.readFile(collectionPath, 'utf-8');
        const documents = JSON.parse(data);
        return documents.filter(doc => this.matchQuery(doc, query));
    }

    async updateDocuments(collectionPath, query, updateFields) {
        await this.ensureCollectionExists(collectionPath);
        const data = await fs.promises.readFile(collectionPath, 'utf-8');
        let documents = JSON.parse(data);
        let updatedCount = 0;
        documents = documents.map(doc => {
            if (this.matchQuery(doc, query)) {
                updatedCount++;
                return { ...doc, ...updateFields };
            }
            return doc;
        });
        await fs.promises.writeFile(collectionPath, JSON.stringify(documents, null, 2));
        return updatedCount;
    }

    async deleteDocuments(collectionPath, query) {
        await this.ensureCollectionExists(collectionPath);
        const data = await fs.promises.readFile(collectionPath, 'utf-8');
        let documents = JSON.parse(data);
        const initialCount = documents.length;
        documents = documents.filter(doc => !this.matchQuery(doc, query));
        await fs.promises.writeFile(collectionPath, JSON.stringify(documents, null, 2));
        return initialCount - documents.length;
    }

    async countDocuments(collectionPath, query) {
        try {
            const data = await fs.promises.readFile(collectionPath, 'utf-8');
            const documents = JSON.parse(data);
            if (Object.keys(query).length === 0) {
                return documents.length;
            }
            return documents.filter(doc => this.matchQuery(doc, query)).length;
        } catch (error) {
            if (error.code === 'ENOENT') {
                await fs.promises.writeFile(collectionPath, JSON.stringify([], null, 2));
                return 0;
            }
            throw error;
        }
    }

    matchQuery(doc, query) {
        for (const [key, value] of Object.entries(query)) {
            if (doc[key] !== value) {
                return false;
            }
        }
        return true;
    }
}

function sendSuccess(ws, data, requestId) {
    console.log('Sending success response:', { data, requestId });
    const response = JSON.stringify({ 
        status: 'success', 
        data,
        requestId 
    });
    ws.send(response, { binary: false }, (error) => {
        if (error) console.error('Send error:', error);
    });
}

function sendError(ws, message, requestId) {
    console.log('Sending error response:', { message, requestId });
    const response = JSON.stringify({ 
        status: 'error', 
        message,
        requestId 
    });
    ws.send(response, { binary: false }, (error) => {
        if (error) console.error('Send error:', error);
    });
}

// Initialize and start the server
const server = new PyxiCloudServer();
server.start();
server.startHeartbeat();

module.exports = PyxiCloudServer;