# PyxisCloud Documentation

PyxisCloud is a lightweight, real-time database server built for Node.js applications. It offers fast, scalable data storage and retrieval via WebSocket connections, making it perfect for applications requiring real-time data synchronization.

## Important Note

PyxisCloud is designed to be used exclusively with PyxisDB, the client library for interacting with PyxisCloud. You cannot directly access PyxisCloud without using PyxisDB.

## Installation and Setup

1. Clone the PyxisCloud repository:
   ```
   git clone https://github.com/Darknessking13/PyxiCloud.git
   cd pyxiscloud
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Start the PyxisCloud server:
   ```
   node .
   ```

   By default, PyxisCloud will start on `ws://localhost:8080`. You can configure the port in the `config.js` file.

## Configuration

PyxisCloud is configured using a `config.js` file in the root directory of the project. Here's an example of the configuration options:

```javascript
// config.js
module.exports = {
    serverIP: 'localhost',
    port: 8080,
    ipWhitelist: false,
    whitelistedIps: ['127.0.0.1'],
    ipBlacklist: false,
    blacklistedIps: [],
    maxConnections: 100
};
```

Configuration options:
- `serverIP`: The IP address the server will bind to (default: 'localhost')
- `port`: The port on which PyxisCloud will listen (default: 8080)
- `ipWhitelist`: Enable/disable IP whitelisting (default: false)
- `whitelistedIps`: Array of IP addresses allowed to connect when whitelist is enabled
- `ipBlacklist`: Enable/disable IP blacklisting (default: false)
- `blacklistedIps`: Array of IP addresses blocked from connecting when blacklist is enabled
- `maxConnections`: Maximum number of simultaneous WebSocket connections (default: 100)

To modify these settings, edit the `config.js` file before starting the PyxisCloud server.

## Security

PyxisCloud provides basic security features through IP whitelisting and blacklisting. To enhance security:

1. Use a reverse proxy (like Nginx) to handle SSL/TLS encryption.
2. Implement authentication in your application layer using PyxisDB.
3. Use firewalls to restrict access to the PyxisCloud server.
4. Enable IP whitelisting or blacklisting as needed in the `config.js` file.

## Data Persistence

PyxisCloud uses in-memory storage by default. For data persistence, you may need to implement a custom storage solution or integrate with a database like MongoDB. Consult the PyxisCloud documentation or repository for information on extending the storage capabilities.

## Troubleshooting

Common issues and their solutions:

1. Connection refused: Ensure PyxisCloud is running and the port is not blocked by a firewall.
2. High latency: Check network conditions and consider scaling your PyxisCloud deployment.
3. Data inconsistency: Verify that all PyxisCloud instances are using the same storage backend when running in a distributed setup.
4. IP blocked: Check the `ipWhitelist` and `ipBlacklist` settings in `config.js` if you're having trouble connecting from certain IP addresses.

## Best Practices

1. Regularly backup your data, especially if you've implemented a custom persistence solution.
2. Monitor your PyxisCloud instances for performance and stability.
3. Use schema validation in PyxisDB to ensure data integrity.
4. Implement proper error handling in your client applications.
5. Regularly review and update your `config.js` file, especially the IP whitelist and blacklist, to maintain security.

## Limitations

- PyxisCloud is designed for real-time applications and may not be suitable for heavy analytical workloads.
- Complex queries and joins are not supported natively. These operations should be performed on the client-side using PyxisDB.
- The default configuration may need to be adjusted for production environments, especially regarding security and connection limits.

## Support and Community

For support, bug reports, or feature requests, please open an issue on the [PyxisCloud GitHub repository](https://github.com/Darknessking13/PyxiCloud.git/issues).

## License

PyxisCloud is released under the MIT License. See the LICENSE file in the repository for full details.
