// index.js
const Schema = require('./schema');
const Model = require('./model');

class pyx {
    static Schema = Schema;

    static model(collectionName) {
        return new Model(collectionName);
    }
}

module.exports = pyx;