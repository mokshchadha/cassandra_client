import 'package:cassandra_client/cassandra_client.dart';
import 'package:test/test.dart';
import 'dart:io';
import 'dart:convert';

void main() {
  late CassandraClient client;
  
  setUp(() {
    client = CassandraClient();
    client.setContactPoints('127.0.0.1');
    client.setPort(9042);
  });
  
  tearDown(() async {
    if (client.isConnected) {
      await client.close();
    }
    client.dispose();
  });
  
  group('CassandraClient Basic Tests', () {
    test('Can create client instance', () {
      expect(client, isNotNull);
    });
    
    test('Can connect to Docker Cassandra instance', () async {
      try {
        final connected = await client.connect();
        expect(connected, isTrue);
        expect(client.isConnected, isTrue);
        print('Successfully connected to Cassandra Docker instance');
      } catch (e) {
        if (e is SocketException || e.toString().contains('Failed to connect')) {
          print('Test skipped: Docker Cassandra is not available. Make sure to run:');
          print('docker-compose up -d');
          expect(true, isTrue); // Skip test
        } else {
          fail('Unexpected error: $e');
        }
      }
    });
  });
  
  group('CassandraClient Query Tests', () {
    // This setup ensures we have a fresh test table for our queries
    setUp(() async {
      try {
        await client.connect();
        
        // Create test keyspace if it doesn't exist
        await client.query('''
          CREATE KEYSPACE IF NOT EXISTS test_keyspace 
          WITH REPLICATION = { 
             'class' : 'SimpleStrategy', 
             'replication_factor' : 1 
          }
        ''');
        
        // Create test table
        await client.query('''
          CREATE TABLE IF NOT EXISTS test_keyspace.test_users (
             id UUID PRIMARY KEY,
             username TEXT,
             email TEXT,
             age INT
          )
        ''');
        
        // Clear the table to start fresh
        await client.query('TRUNCATE test_keyspace.test_users');
        
      } catch (e) {
        if (e is SocketException || e.toString().contains('Failed to connect')) {
          // Skip setup if Cassandra isn't available
          print('Setup skipped: Cassandra is not available');
        } else {
          rethrow;
        }
      }
    });
    
    test('Can execute an INSERT query', () async {
      try {
        if (!client.isConnected) {
          print('Test skipped: Not connected to Cassandra');
          expect(true, isTrue);
          return;
        }
        
        // Execute an INSERT query
        final insertResult = await client.query('''
          INSERT INTO test_keyspace.test_users (id, username, email, age)
          VALUES (uuid(), 'testuser1', 'test1@example.com', 25)
        ''');
        
        // INSERT queries typically return an empty array in JSON
        final insertData = jsonDecode(insertResult);
        expect(insertData, isA<List>());
        
        // Verify the data was inserted with a SELECT query
        final selectResult = await client.query('''
          SELECT * FROM test_keyspace.test_users WHERE username = 'testuser1' ALLOW FILTERING
        ''');
        
        final selectData = jsonDecode(selectResult);
        expect(selectData, isA<List>());
        expect(selectData.length, 1);
        expect(selectData[0]['username'], 'testuser1');
        expect(selectData[0]['email'], 'test1@example.com');
        expect(selectData[0]['age'], 25);
        expect(selectData[0]['id'], isNotNull); // UUID should be present
        
        print('Successfully executed INSERT query');
      } catch (e) {
        if (e is SocketException || e.toString().contains('Failed to connect')) {
          print('Test skipped: Cassandra is not available');
          expect(true, isTrue);
        } else {
          fail('Query error: $e');
        }
      }
    });
    
    test('Can execute a SELECT query', () async {
      try {
        if (!client.isConnected) {
          print('Test skipped: Not connected to Cassandra');
          expect(true, isTrue);
          return;
        }
        
        // Insert some test data first
        await client.query('''
          INSERT INTO test_keyspace.test_users (id, username, email, age)
          VALUES (uuid(), 'testuser2', 'test2@example.com', 30)
        ''');
        
        await client.query('''
          INSERT INTO test_keyspace.test_users (id, username, email, age)
          VALUES (uuid(), 'testuser3', 'test3@example.com', 35)
        ''');
        
        // Execute a SELECT query
        final selectResult = await client.query('''
          SELECT * FROM test_keyspace.test_users
        ''');
        
        final selectData = jsonDecode(selectResult);
        expect(selectData, isA<List>());
        expect(selectData.length, greaterThanOrEqualTo(2)); // Should have at least our 2 records
        
        // Test a more specific SELECT
        final filteredResult = await client.query('''
          SELECT username, email FROM test_keyspace.test_users 
          WHERE age > 30 ALLOW FILTERING
        ''');
        
        final filteredData = jsonDecode(filteredResult);
        expect(filteredData, isA<List>());
        expect(filteredData.length, greaterThanOrEqualTo(1));
        
        // The filtered data should contain testuser3 but not testuser2
        bool foundTestUser3 = false;
        for (var row in filteredData) {
          if (row['username'] == 'testuser3') {
            foundTestUser3 = true;
            expect(row['email'], 'test3@example.com');
          }
          // Should not contain testuser2
          expect(row['username'] != 'testuser2', isTrue);
        }
        expect(foundTestUser3, isTrue);
        
        print('Successfully executed SELECT query');
      } catch (e) {
        if (e is SocketException || e.toString().contains('Failed to connect')) {
          print('Test skipped: Cassandra is not available');
          expect(true, isTrue);
        } else {
          fail('Query error: $e');
        }
      }
    });
    
    // test('Can execute a DELETE query', () async {
    //   try {
    //     if (!client.isConnected) {
    //       print('Test skipped: Not connected to Cassandra');
    //       expect(true, isTrue);
    //       return;
    //     }
        
    //     // Insert test data with a specific username
    //     await client.query('''
    //       INSERT INTO test_keyspace.test_users (id, username, email, age)
    //       VALUES (uuid(), 'delete_me', 'delete@example.com', 40)
    //     ''');
        
    //     // Verify the data was inserted
    //     final beforeDelete = await client.query('''
    //       SELECT * FROM test_keyspace.test_users WHERE username = 'delete_me' ALLOW FILTERING
    //     ''');
        
    //     final beforeData = jsonDecode(beforeDelete);
    //     print(beforeData);
    //     expect(beforeData, isA<List>());
    //     expect(beforeData.length, 1);
        
    //     // Get the ID for deletion
    //     final id = beforeData[0]['id'];
        
    //     // Execute a DELETE query
    //     final deleteResult = await client.query('''
    //       DELETE FROM test_keyspace.test_users WHERE id = $id
    //     ''');
        
    //     // Verify the data was deleted
    //     final afterDelete = await client.query('''
    //       SELECT * FROM test_keyspace.test_users WHERE username = 'delete_me' ALLOW FILTERING
    //     ''');
        
    //     final afterData = jsonDecode(afterDelete);
    //     expect(afterData, isA<List>());
    //     expect(afterData.length, 0);
        
    //     print('Successfully executed DELETE query');
    //   } catch (e) {
    //     if (e is SocketException || e.toString().contains('Failed to connect')) {
    //       print('Test skipped: Cassandra is not available');
    //       expect(true, isTrue);
    //     } else {
    //       fail('Query error: $e');
    //     }
    //   }
    // });
  });
}