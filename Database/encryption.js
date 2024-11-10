const crypto = require('crypto');

const ENCRYPTION_ALGORITHM = 'aes-256-cbc';

function generateKeyPair() {
    const dh = crypto.createDiffieHellman(2048);
    const keys = dh.generateKeys();
    return {
        publicKey: dh.getPublicKey('base64'),
        privateKey: dh.getPrivateKey('base64'),
        prime: dh.getPrime('base64'),
        generator: dh.getGenerator('base64')
    };
}

function computeSharedSecret(privateKey, otherPublicKey, prime, generator) {
    const dh = crypto.createDiffieHellman(Buffer.from(prime, 'base64'), Buffer.from(generator, 'base64'));
    dh.setPrivateKey(Buffer.from(privateKey, 'base64'));
    return dh.computeSecret(Buffer.from(otherPublicKey, 'base64'));
}

function encrypt(data, sharedSecret) {
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipheriv(ENCRYPTION_ALGORITHM, sharedSecret.slice(0, 32), iv);
    let encrypted = cipher.update(data, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    return iv.toString('hex') + ':' + encrypted;
}

function decrypt(data, sharedSecret) {
    const [ivHex, encryptedData] = data.split(':');
    const iv = Buffer.from(ivHex, 'hex');
    const decipher = crypto.createDecipheriv(ENCRYPTION_ALGORITHM, sharedSecret.slice(0, 32), iv);
    let decrypted = decipher.update(encryptedData, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
}

module.exports = { generateKeyPair, computeSharedSecret, encrypt, decrypt };