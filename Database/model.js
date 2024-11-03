class Model {
    constructor(collectionName) {
        this.collectionName = collectionName;
        this.schema = Schema.loadSchema(collectionName);
        this.filePath = path.join(__dirname, 'collections', `${collectionName}.json`);
        this.cache = new Map();
        this.cacheSize = 100;
        
        if (!fs.existsSync(this.filePath)) {
            fs.writeFileSync(this.filePath, JSON.stringify([]));
        } else {
            const data = this._readData();
            data.slice(-this.cacheSize).forEach(doc => {
                this.cache.set(this._getDocumentKey(doc), doc);
            });
        }
    }

    _readData() {
        const data = fs.readFileSync(this.filePath, 'utf-8');
        return JSON.parse(data);
    }

    _writeData(data) {
        fs.writeFileSync(this.filePath, JSON.stringify(data, null, 2));
        this.cache.clear();
        data.slice(-this.cacheSize).forEach(doc => {
            this.cache.set(this._getDocumentKey(doc), doc);
        });
    }


    _getDocumentKey(document) {
        // Create a unique key based on document content, excluding _id and createdAt
        const docCopy = { ...document };
        delete docCopy._id;
        delete docCopy.createdAt;
        return JSON.stringify(docCopy);
    }

    _isUnique(document) {
        const docKey = this._getDocumentKey(document);
        if (this.cache.has(docKey)) {
            return false;
        }

        const data = this._readData();
        return !data.some(existingDoc => this._getDocumentKey(existingDoc) === docKey);
    }


    _compareObjects(obj1, obj2, schema = this.schema.schemaDefinition) {
        for (const key in schema) {
            const fieldDefinition = schema[key];
            if (fieldDefinition.type === 'object' && fieldDefinition.properties) {
                if (!this._compareObjects(obj1[key], obj2[key], fieldDefinition.properties)) {
                    return false;
                }
            } else if (fieldDefinition.type === 'array' && fieldDefinition.items && fieldDefinition.items.type === 'object') {
                if (!Array.isArray(obj1[key]) || !Array.isArray(obj2[key]) || obj1[key].length !== obj2[key].length) {
                    return false;
                }
                for (let i = 0; i < obj1[key].length; i++) {
                    if (!this._compareObjects(obj1[key][i], obj2[key][i], fieldDefinition.items.properties)) {
                        return false;
                    }
                }
            } else if (fieldDefinition.type !== 'boolean' && fieldDefinition.type !== 'date' && fieldDefinition.type !== 'array') {
                if (obj1[key] !== obj2[key]) {
                    return false;
                }
            }
        }
        return true;
    }

    insert(document) {
        this._applyDefaults(document, this.schema.schemaDefinition);
        this.schema.validateDocument(document);
        
        if (!this._isUnique(document)) {
            throw new Error("An identical document already exists in the collection");
        }
        
        const data = this._readData();
        data.push(document);
        this._writeData(data);
        
        const docKey = this._getDocumentKey(document);
        if (this.cache.size >= this.cacheSize) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
        this.cache.set(docKey, document);
        
        return document;
    }



    _applyDefaults(document, schema) {
        for (const field in schema) {
            const fieldDefinition = schema[field];
            if (!(field in document) && 'default' in fieldDefinition) {
                document[field] = 
                    typeof fieldDefinition.default === 'function'
                        ? fieldDefinition.default()
                        : fieldDefinition.default;
            }
            if (fieldDefinition.type === 'object' && fieldDefinition.properties) {
                if (!document[field]) {
                    document[field] = {};
                }
                this._applyDefaults(document[field], fieldDefinition.properties);
            }
            if (fieldDefinition.type === 'array' && fieldDefinition.items && fieldDefinition.items.type === 'object') {
                if (!document[field]) {
                    document[field] = [];
                }
                document[field].forEach(item => this._applyDefaults(item, fieldDefinition.items.properties));
            }
        }
    }

    
    find(query = {}) {
        if (Object.keys(query).length === 0) {
            return Array.from(this.cache.values());
        }
        
        const data = this._readData();
        return data.filter(doc => this._matchQuery(doc, query));
    }


    _matchQuery(doc, query) {
        for (const key in query) {
            const queryValue = query[key];
            const docValue = this._getNestedValue(doc, key);
            if (typeof queryValue === 'object' && !Array.isArray(queryValue)) {
                if (!this._matchQuery(docValue, queryValue)) {
                    return false;
                }
            } else if (docValue !== queryValue) {
                return false;
            }
        }
        return true;
    }

    _getNestedValue(obj, path) {
        return path.split('.').reduce((current, key) => {
            return current && current[key] !== undefined ? current[key] : undefined;
        }, obj);
    }

    update(query, updateFields) {
        const data = this._readData();
        let updatedCount = 0;
        const updatedData = data.map(doc => {
            if (this._matchQuery(doc, query)) {
                updatedCount++;
                return this._updateDocument(doc, updateFields);
            }
            return doc;
        });
        this._writeData(updatedData);
        return updatedCount;
    }

    _updateDocument(doc, updateFields) {
        const updatedDoc = { ...doc };
        for (const key in updateFields) {
            const value = updateFields[key];
            if (typeof value === 'object' && !Array.isArray(value) && value !== null) {
                this._updateNestedField(updatedDoc, key.split('.'), value);
            } else {
                this._setNestedValue(updatedDoc, key, value);
            }
        }
        this._validateUpdateFields(updatedDoc);
        return updatedDoc;
    }

    _validateUpdateFields(updatedDoc) {
        for (const [field, definition] of Object.entries(this.schema.schemaDefinition)) {
            if (field in updatedDoc) {
                const value = updatedDoc[field];
                if (definition.type === 'number' && typeof value !== 'number') {
                    throw new Error(`Field "${field}" must be a number.`);
                }
                if (definition.type === 'string' && typeof value !== 'string') {
                    throw new Error(`Field "${field}" must be a string.`);
                }
                if (definition.type === 'boolean' && typeof value !== 'boolean') {
                    throw new Error(`Field "${field}" must be a boolean.`);
                }
                if (definition.type === 'date' && !(value instanceof Date)) {
                    throw new Error(`Field "${field}" must be a Date object.`);
                }
                if (definition.type === 'object' && typeof value !== 'object') {
                    throw new Error(`Field "${field}" must be an object.`);
                }
                if (definition.type === 'array' && !Array.isArray(value)) {
                    throw new Error(`Field "${field}" must be an array.`);
                }
            }
        }
    }

    _updateNestedField(doc, pathArray, value) {
        const key = pathArray.shift();
        if (pathArray.length === 0) {
            doc[key] = value;
        } else {
            if (!doc[key]) {
                doc[key] = {};
            }
            this._updateNestedField(doc[key], pathArray, value);
        }
    }

    _setNestedValue(obj, path, value) {
        const keys = path.split('.');
        const lastKey = keys.pop();
        const lastObj = keys.reduce((current, key) => {
            if (!current[key]) current[key] = {};
            return current[key];
        }, obj);
        lastObj[lastKey] = value;
    }

    delete(query) {
        const data = this._readData();
        const filteredData = data.filter(doc => !this._matchQuery(doc, query));
        const deletedCount = data.length - filteredData.length;
        this._writeData(filteredData);
        return deletedCount;
    }

    count(query = {}) {
        return this.find(query).length;
    }
}

module.exports = Model;