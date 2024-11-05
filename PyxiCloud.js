const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const config = require('./config');

const schemaDirectory = path.join(__dirname, 'Database', 'schemas');
const collectionsDirectory = path.join(__dirname, 'Database', 'collections');

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
        this.heartbeatInterval = 30000;
        this.maxPayloadSize = 1024 * 1024;
        this.rateLimiter = new Map();
        this.schemas = new Map();
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
                this.sendError(ws, 'Rate limit exceeded. Please try again later.');
                return;
            }

            console.log('Received message from client:', message.toString());
            try {
                const event = JSON.parse(message.toString());
                if (!this.validateEvent(event)) {
                    this.sendError(ws, 'Invalid event format');
                    return;
                }
                console.log('Parsed event:', event);
                await this.handleRequest(event, ws);
            } catch (error) {
                console.error('Error processing message:', error);
                this.sendError(ws, 'Invalid message format');
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
        const index = collection.findIndex(doc => this.matchQuery(doc, query));
        if (index !== -1) {
            collection[index] = { ...collection[index], ...updateFields };
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
        let modifiedCount = 0;
        collection = collection.map(doc => {
            if (this.matchQuery(doc, query)) {
                modifiedCount++;
                return { ...doc, ...updateFields };
            }
            return doc;
        });
        fs.writeFileSync(collectionPath, JSON.stringify(collection, null, 2));
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