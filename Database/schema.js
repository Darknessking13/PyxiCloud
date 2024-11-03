// schema.js
const fs = require('fs');
const path = require('path');

class Schema {
    constructor(schemaDefinition) {
        this.schemaDefinition = schemaDefinition;
    }

    validateDocument(document, schema = this.schemaDefinition, prefix = '') {
        for (const field in schema) {
            const fieldDefinition = schema[field];
            const value = document[field];
            const fullFieldName = prefix ? `${prefix}.${field}` : field;
    
            if (value !== undefined && value !== null) {
                // Type validation
                if (fieldDefinition.type === 'object' && fieldDefinition.properties) {
                    if (typeof value !== 'object') {
                        throw new Error(`Field "${fullFieldName}" should be an object.`);
                    }
                    this.validateDocument(value, fieldDefinition.properties, fullFieldName);
                } else if (fieldDefinition.type === 'array' && fieldDefinition.items) {
                    if (!Array.isArray(value)) {
                        throw new Error(`Field "${fullFieldName}" should be an array.`);
                    }
                    value.forEach((item, index) => {
                        this.validateDocument(item, fieldDefinition.items, `${fullFieldName}[${index}]`);
                    });
                } else {
                    // Basic type validation
                    if (fieldDefinition.type === 'date') {
                        if (!(value instanceof Date) || isNaN(value.getTime())) {
                            throw new Error(`Field "${fullFieldName}" should be a valid Date.`);
                        }
                    } else if (typeof value !== fieldDefinition.type) {
                        throw new Error(`Field "${fullFieldName}" should be of type "${fieldDefinition.type}".`);
                    }
                }
            } else if (fieldDefinition.required) {
                throw new Error(`Field "${fullFieldName}" is required.`);
            }
        }
        return true;
    }
    
    getUniqueFields(schema = this.schemaDefinition, prefix = '') {
        let uniqueFields = [];
        for (const field in schema) {
            const fieldDefinition = schema[field];
            const fullFieldName = prefix ? `${prefix}.${field}` : field;

            if (fieldDefinition.unique) {
                uniqueFields.push(fullFieldName);
            }

            if (fieldDefinition.type === 'object' && fieldDefinition.properties) {
                uniqueFields = uniqueFields.concat(this.getUniqueFields(fieldDefinition.properties, fullFieldName));
            }

            if (fieldDefinition.type === 'array' && fieldDefinition.items && fieldDefinition.items.type === 'object') {
                uniqueFields = uniqueFields.concat(this.getUniqueFields(fieldDefinition.items.properties, `${fullFieldName}[]`));
            }
        }
        return uniqueFields;
    }

    static loadSchema(collectionName) {
        const schemaPath = path.join(__dirname, 'schemas', `${collectionName}.json`);
        if (!fs.existsSync(schemaPath)) {
            throw new Error(`Schema for collection "${collectionName}" does not exist`);
        }
        const schemaDefinition = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
        return new Schema(schemaDefinition);
    }
}

module.exports = Schema;