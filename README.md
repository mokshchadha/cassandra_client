# Cassandra Client for Dart

A lightweight Dart FFI wrapper for the DataStax C/C++ Cassandra driver, providing native performance for Cassandra database operations in Dart applications.

## Features

- Connect to Cassandra clusters
- Execute CQL queries (SELECT, INSERT, UPDATE, DELETE)
- Perform schema operations (CREATE TABLE, ALTER TABLE, DROP TABLE)
- Handle various Cassandra data types
- JSON output format for query results
- Low-level direct access to the Cassandra C driver

## Installation

Add this package to your `pubspec.yaml`:

```yaml
dependencies:
  cassandra_client: ^0.1.0
```

### Prerequisites

This package requires the DataStax C/C++ driver to be installed on your system:

#### MacOS

```bash
brew install cassandra-cpp-driver
```

#### Linux

```bash
sudo apt-get install libuv1-dev libssl-dev
sudo apt-get install libcassandra-dev
```

#### Windows

Download and install the DataStax C/C++ driver binaries from their GitHub releases.

## Usage

### Basic Connection

```dart
import 'package:cassandra_client/cassandra_client.dart';

void main() async {
  final client = CassandraClient();
  
  try {
    // Configure the connection
    client.setContactPoints('127.0.0.1');
    client.setPort(9042);
    
    // Connect to Cassandra
    final connected = await client.connect();
    
    if (connected) {
      print('Successfully connected to Cassandra!');
      
      // Close the connection when done
      await client.close();
    }
  } catch (e) {
    print('Error: $e');
  } finally {
    // Always dispose resources
    client.dispose();
  }
}
```

### Executing Queries

```dart
import 'dart:convert';
import 'package:cassandra_client/cassandra_client.dart';

void main() async {
  final client = CassandraClient();
  
  try {
    client.setContactPoints('127.0.0.1');
    client.setPort(9042);
    await client.connect();
    
    // Create keyspace and table
    await client.query('''
      CREATE KEYSPACE IF NOT EXISTS example 
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    ''');
    
    await client.query('''
      CREATE TABLE IF NOT EXISTS example.users (
        id UUID PRIMARY KEY,
        username TEXT,
        email TEXT,
        age INT
      )
    ''');
    
    // Insert data
    await client.query('''
      INSERT INTO example.users (id, username, email, age)
      VALUES (uuid(), 'john_doe', 'john@example.com', 30)
    ''');
    
    // Query data
    final result = await client.query('SELECT * FROM example.users');
    
    // Parse JSON result
    final users = jsonDecode(result);
    for (var user in users) {
      print('User: ${user['username']}, Email: ${user['email']}');
    }
    
  } finally {
    await client.close();
    client.dispose();
  }
}
```

### Using with Docker

```dart
// Docker Compose file example:
// 
// services:
//   cassandra:
//     image: cassandra:latest
//     ports:
//       - "9042:9042"
//     environment:
//       - CASSANDRA_CLUSTER_NAME=MyCluster
//       - CASSANDRA_ENDPOINT_SNITCH=SimpleSnitch

import 'package:cassandra_client/cassandra_client.dart';

void main() async {
  final client = CassandraClient();
  
  try {
    client.setContactPoints('127.0.0.1'); // Docker host
    client.setPort(9042);                 // Exposed port
    await client.connect();
    
    // Execute queries...
    
  } finally {
    await client.close();
    client.dispose();
  }
}
```

## API Reference

### CassandraClient

- `CassandraClient()` - Creates a new client instance
- `setContactPoints(String contactPoints)` - Sets the Cassandra hosts to connect to
- `setPort(int port)` - Sets the Cassandra port (default: 9042)
- `connect()` - Connects to the Cassandra cluster
- `query(String cql)` - Executes a CQL query and returns results as JSON
- `close()` - Closes the connection
- `dispose()` - Releases all resources

## Data Type Support

The client supports the following Cassandra data types:

- Text/String types (TEXT, VARCHAR, ASCII)
- Numeric types (INT, BIGINT, DOUBLE, FLOAT)
- Boolean
- UUID
- Timestamp
- Blob (returned as Base64 encoded strings)

## Error Handling

The client provides detailed error messages from the Cassandra driver:

```dart
try {
  await client.query('INVALID QUERY');
} catch (e) {
  print('Query error: $e');
}
```

## Limitations

- Limited support for collection types (list, map, set)
- No batch operation support yet
- No prepared statements yet

## Development

### Testing

```bash
# Start a Cassandra instance (e.g., with Docker)
docker run --name cassandra-test -p 9042:9042 -d cassandra:latest

# Run tests
dart test
```

### Building from Source

```bash
git clone https://github.com/yourusername/cassandra_client.git
cd cassandra_client
dart pub get
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.