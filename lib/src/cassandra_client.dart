import 'dart:ffi';
import 'dart:io' show Platform, File;
import 'dart:convert';
import 'package:ffi/ffi.dart';

// Cassandra client with query capability
class CassandraClient {
  late final DynamicLibrary _dylib;
  Pointer<Void>? _cluster;
  Pointer<Void>? _session;
  bool _isConnected = false;

  // Constructor
  CassandraClient() {
    _dylib = _loadDynamicLibrary();
  }

  // Load the Cassandra C driver library
  DynamicLibrary _loadDynamicLibrary() {
    if (Platform.isMacOS) {
      final appleLibPath = '/opt/homebrew/lib/libcassandra.dylib';
      final intelLibPath = '/usr/local/lib/libcassandra.dylib';
      
      if (File(appleLibPath).existsSync()) {
        return DynamicLibrary.open(appleLibPath);
      } else if (File(intelLibPath).existsSync()) {
        return DynamicLibrary.open(intelLibPath);
      } else {
        throw Exception('Cassandra library not found. Please ensure it is installed with Homebrew.');
      }
    } else if (Platform.isWindows) {
      return DynamicLibrary.open('cassandra.dll');
    } else {
      return DynamicLibrary.open('libcassandra.so');
    }
  }

  // Set contact points
  void setContactPoints(String contactPoints) {
    if (_cluster == null) {
      _initCluster();
    }
    
    final contactPointsNative = contactPoints.toNativeUtf8();
    
    try {
      final setContactPointsFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Utf8>)>>('cass_cluster_set_contact_points')
          .asFunction<int Function(Pointer<Void>, Pointer<Utf8>)>();
          
      setContactPointsFunc(_cluster!, contactPointsNative);
    } finally {
      malloc.free(contactPointsNative);
    }
  }
  
  // Set port
  void setPort(int port) {
    if (_cluster == null) {
      _initCluster();
    }
    
    final setPortFunc = _dylib
        .lookup<NativeFunction<Int32 Function(Pointer<Void>, Int32)>>('cass_cluster_set_port')
        .asFunction<int Function(Pointer<Void>, int)>();
        
    setPortFunc(_cluster!, port);
  }

  // Initialize cluster
  void _initCluster() {
    final clusterNewFunc = _dylib
        .lookup<NativeFunction<Pointer<Void> Function()>>('cass_cluster_new')
        .asFunction<Pointer<Void> Function()>();
        
    _cluster = clusterNewFunc();
    if (_cluster == null) {
      throw Exception('Failed to create Cassandra cluster');
    }
  }

  // Connect to Cassandra
  Future<bool> connect() async {
    if (_cluster == null) {
      _initCluster();
    }
    
    final sessionNewFunc = _dylib
        .lookup<NativeFunction<Pointer<Void> Function()>>('cass_session_new')
        .asFunction<Pointer<Void> Function()>();
        
    _session = sessionNewFunc();
    if (_session == null) {
      throw Exception('Failed to create Cassandra session');
    }
    
    final connectFunc = _dylib
        .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>, Pointer<Void>)>>('cass_session_connect')
        .asFunction<Pointer<Void> Function(Pointer<Void>, Pointer<Void>)>();
        
    final future = connectFunc(_session!, _cluster!);
    
    try {
      final result = await _waitForFuture(future);
      _isConnected = result;
      return result;
    } finally {
      _freeFuture(future);
    }
  }
  
  // Execute a query and return results as JSON
  Future<String> query(String cql) async {
    if (!_isConnected || _session == null) {
      throw Exception('Not connected to Cassandra. Call connect() first.');
    }
    
    // Create the statement
    final statementFunc = _dylib
        .lookup<NativeFunction<Pointer<Void> Function(Pointer<Utf8>, Size)>>('cass_statement_new')
        .asFunction<Pointer<Void> Function(Pointer<Utf8>, int)>();
        
    final cqlNative = cql.toNativeUtf8();
    final statement = statementFunc(cqlNative, 0); // 0 parameters
    
    if (statement == nullptr) {
      malloc.free(cqlNative);
      throw Exception('Failed to create statement');
    }
    
    try {
      // Execute the query
      final executeFunc = _dylib
          .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>, Pointer<Void>)>>('cass_session_execute')
          .asFunction<Pointer<Void> Function(Pointer<Void>, Pointer<Void>)>();
          
      final future = executeFunc(_session!, statement);
      
      if (future == nullptr) {
        throw Exception('Failed to execute query');
      }
      
      try {
        // Wait for query to complete
        final success = await _waitForFuture(future);
        if (!success) {
          final errorMessage = _getFutureError(future);
          throw Exception('Query execution failed: $errorMessage');
        }
        
        // Get the result
        final getResultFunc = _dylib
            .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>)>>('cass_future_get_result')
            .asFunction<Pointer<Void> Function(Pointer<Void>)>();
            
        final result = getResultFunc(future);
        
        if (result == nullptr) {
          throw Exception('No result returned from query');
        }
        
        try {
          // Convert result to JSON
          return _resultToJson(result);
        } finally {
          // Free the result
          final freeResultFunc = _dylib
              .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_result_free')
              .asFunction<void Function(Pointer<Void>)>();
              
          freeResultFunc(result);
        }
      } finally {
        _freeFuture(future);
      }
    } finally {
      // Free the statement and CQL string
      final freeStatementFunc = _dylib
          .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_statement_free')
          .asFunction<void Function(Pointer<Void>)>();
          
      freeStatementFunc(statement);
      malloc.free(cqlNative);
    }
  }
  
 
// Convert a result to JSON
String _resultToJson(Pointer<Void> resultPtr) {
  final rows = <Map<String, dynamic>>[];
  
  // Get the column count
  final columnCountFunc = _dylib
      .lookup<NativeFunction<Size Function(Pointer<Void>)>>('cass_result_column_count')
      .asFunction<int Function(Pointer<Void>)>();
      
  final columnCount = columnCountFunc(resultPtr);
  
  // Create an iterator for the result
  final iteratorFunc = _dylib
      .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>)>>('cass_iterator_from_result')
      .asFunction<Pointer<Void> Function(Pointer<Void>)>();
      
  final iterator = iteratorFunc(resultPtr);
  
  if (iterator == nullptr) {
    return jsonEncode([]);
  }
  
  try {
    // Get column names - using a safer approach
    final columnNames = <String>[];
    for (int i = 0; i < columnCount; i++) {
      // Use the safer string approach with temporary pointers
      final stringPtrPtr = calloc<Pointer<Utf8>>();
      final sizePtr = calloc<Size>();
      
      try {
        final getNameFunc = _dylib
            .lookup<NativeFunction<Int32 Function(Pointer<Void>, Size, Pointer<Pointer<Utf8>>, Pointer<Size>)>>('cass_result_column_name')
            .asFunction<int Function(Pointer<Void>, int, Pointer<Pointer<Utf8>>, Pointer<Size>)>();
            
        final nameResult = getNameFunc(resultPtr, i, stringPtrPtr, sizePtr);
        
        if (nameResult == 0) { // CASS_OK
          final stringPtr = stringPtrPtr.value;
          final size = sizePtr.value;
          final name = stringPtr.toDartString(length: size);
          columnNames.add(name);
        } else {
          // Add a default name if we can't get the actual name
          columnNames.add('column_$i');
        }
      } finally {
        calloc.free(stringPtrPtr);
        calloc.free(sizePtr);
      }
    }
    
    // Iterate through the rows
    final nextFunc = _dylib
        .lookup<NativeFunction<Int32 Function(Pointer<Void>)>>('cass_iterator_next')
        .asFunction<int Function(Pointer<Void>)>();
        
    final getRowFunc = _dylib
        .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>)>>('cass_iterator_get_row')
        .asFunction<Pointer<Void> Function(Pointer<Void>)>();
        
    while (nextFunc(iterator) != 0) {
      final row = getRowFunc(iterator);
      final rowData = <String, dynamic>{};
      
      // Process each column in the row
      for (int i = 0; i < columnCount; i++) {
        final getColumnFunc = _dylib
            .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>, Size)>>('cass_row_get_column')
            .asFunction<Pointer<Void> Function(Pointer<Void>, int)>();
            
        final value = getColumnFunc(row, i);
        
        if (value != nullptr) {
          // Safely get the column name (if available)
          final columnName = i < columnNames.length ? columnNames[i] : 'column_$i';
          rowData[columnName] = _extractValue(value);
          
          // Removed the call to cass_value_free since it's not available
        } else {
          // Handle null values
          final columnName = i < columnNames.length ? columnNames[i] : 'column_$i';
          rowData[columnName] = null;
        }
      }
      
      rows.add(rowData);
    }
  } finally {
    // Free the iterator
    final freeIteratorFunc = _dylib
        .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_iterator_free')
        .asFunction<void Function(Pointer<Void>)>();
        
    freeIteratorFunc(iterator);
  }
  
  return jsonEncode(rows);
}
  
  // Extract a value from a CassValue
  dynamic _extractValue(Pointer<Void> value) {
    // Get the value type
    final getTypeFunc = _dylib
        .lookup<NativeFunction<Int32 Function(Pointer<Void>)>>('cass_value_type')
        .asFunction<int Function(Pointer<Void>)>();
        
    final isNullFunc = _dylib
        .lookup<NativeFunction<Int32 Function(Pointer<Void>)>>('cass_value_is_null')
        .asFunction<int Function(Pointer<Void>)>();
        
    // Check if the value is null
    if (isNullFunc(value) != 0) {
      return null;
    }
    
    final type = getTypeFunc(value);
    
    // Types reference:
    // CASS_VALUE_TYPE_UNKNOWN   = 0x0000
    // CASS_VALUE_TYPE_CUSTOM    = 0x0000
    // CASS_VALUE_TYPE_ASCII     = 0x0001
    // CASS_VALUE_TYPE_BIGINT    = 0x0002
    // CASS_VALUE_TYPE_BLOB      = 0x0003
    // CASS_VALUE_TYPE_BOOLEAN   = 0x0004
    // CASS_VALUE_TYPE_COUNTER   = 0x0005
    // CASS_VALUE_TYPE_DECIMAL   = 0x0006
    // CASS_VALUE_TYPE_DOUBLE    = 0x0007
    // CASS_VALUE_TYPE_FLOAT     = 0x0008
    // CASS_VALUE_TYPE_INT       = 0x0009
    // CASS_VALUE_TYPE_TEXT      = 0x000A
    // CASS_VALUE_TYPE_TIMESTAMP = 0x000B
    // CASS_VALUE_TYPE_UUID      = 0x000C
    // CASS_VALUE_TYPE_VARCHAR   = 0x000D
    // CASS_VALUE_TYPE_VARINT    = 0x000E
    // CASS_VALUE_TYPE_TIMEUUID  = 0x000F
    // CASS_VALUE_TYPE_INET      = 0x0010
    // CASS_VALUE_TYPE_DATE      = 0x0011
    // CASS_VALUE_TYPE_TIME      = 0x0012
    // CASS_VALUE_TYPE_SMALL_INT = 0x0013
    // CASS_VALUE_TYPE_TINY_INT  = 0x0014
    
    switch (type) {
      case 0x0001: // ASCII
      case 0x000A: // TEXT
      case 0x000D: // VARCHAR
        return _getString(value);
        
      case 0x0002: // BIGINT
      case 0x0005: // COUNTER
      case 0x000B: // TIMESTAMP
        return _getInt64(value);
        
      case 0x0004: // BOOLEAN
        return _getBool(value);
        
      case 0x0007: // DOUBLE
        return _getDouble(value);
        
      case 0x0008: // FLOAT
        return _getFloat(value);
        
      case 0x0009: // INT
        return _getInt32(value);
        
      case 0x000C: // UUID
      case 0x000F: // TIMEUUID
        return _getUuid(value);
        
      case 0x0003: // BLOB
        return _getBlobAsBase64(value);
        
      default:
        // For other types, return as string for now
        return _getString(value);
    }
  }
  
  // Get a string value
  String _getString(Pointer<Void> value) {
    final stringPtrPtr = calloc<Pointer<Utf8>>();
    final sizePtr = calloc<Size>();
    
    try {
      final getStringFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Pointer<Utf8>>, Pointer<Size>)>>('cass_value_get_string')
          .asFunction<int Function(Pointer<Void>, Pointer<Pointer<Utf8>>, Pointer<Size>)>();
          
      final result = getStringFunc(value, stringPtrPtr, sizePtr);
      
      if (result != 0) {
        return ''; // Error getting string
      }
      
      final stringPtr = stringPtrPtr.value;
      final size = sizePtr.value;
      
      return stringPtr.toDartString(length: size);
    } finally {
      calloc.free(stringPtrPtr);
      calloc.free(sizePtr);
    }
  }
  
  // Get a 64-bit integer value
  int _getInt64(Pointer<Void> value) {
    final int64Ptr = calloc<Int64>();
    
    try {
      final getInt64Func = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Int64>)>>('cass_value_get_int64')
          .asFunction<int Function(Pointer<Void>, Pointer<Int64>)>();
          
      final result = getInt64Func(value, int64Ptr);
      
      if (result != 0) {
        return 0; // Error getting int64
      }
      
      return int64Ptr.value;
    } finally {
      calloc.free(int64Ptr);
    }
  }
  
  // Get a 32-bit integer value
  int _getInt32(Pointer<Void> value) {
    final int32Ptr = calloc<Int32>();
    
    try {
      final getInt32Func = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Int32>)>>('cass_value_get_int32')
          .asFunction<int Function(Pointer<Void>, Pointer<Int32>)>();
          
      final result = getInt32Func(value, int32Ptr);
      
      if (result != 0) {
        return 0; // Error getting int32
      }
      
      return int32Ptr.value;
    } finally {
      calloc.free(int32Ptr);
    }
  }
  
  // Get a boolean value
  bool _getBool(Pointer<Void> value) {
    final boolPtr = calloc<Uint8>();
    
    try {
      final getBoolFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Uint8>)>>('cass_value_get_bool')
          .asFunction<int Function(Pointer<Void>, Pointer<Uint8>)>();
          
      final result = getBoolFunc(value, boolPtr);
      
      if (result != 0) {
        return false; // Error getting bool
      }
      
      return boolPtr.value != 0;
    } finally {
      calloc.free(boolPtr);
    }
  }
  
  // Get a double value
  double _getDouble(Pointer<Void> value) {
    final doublePtr = calloc<Double>();
    
    try {
      final getDoubleFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Double>)>>('cass_value_get_double')
          .asFunction<int Function(Pointer<Void>, Pointer<Double>)>();
          
      final result = getDoubleFunc(value, doublePtr);
      
      if (result != 0) {
        return 0.0; // Error getting double
      }
      
      return doublePtr.value;
    } finally {
      calloc.free(doublePtr);
    }
  }
  
  // Get a float value
  double _getFloat(Pointer<Void> value) {
    final floatPtr = calloc<Float>();
    
    try {
      final getFloatFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Float>)>>('cass_value_get_float')
          .asFunction<int Function(Pointer<Void>, Pointer<Float>)>();
          
      final result = getFloatFunc(value, floatPtr);
      
      if (result != 0) {
        return 0.0; // Error getting float
      }
      
      return floatPtr.value;
    } finally {
      calloc.free(floatPtr);
    }
  }
  
  // Get a UUID as string
  String _getUuid(Pointer<Void> value) {
    final uuidPtr = calloc<Uint8>(16); // UUIDs are 16 bytes
    
    try {
      final getUuidFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Uint8>)>>('cass_value_get_uuid')
          .asFunction<int Function(Pointer<Void>, Pointer<Uint8>)>();
          
      final result = getUuidFunc(value, uuidPtr);
      
      if (result != 0) {
        return ''; // Error getting UUID
      }
      
      // Format UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
      final bytes = [
        for (int i = 0; i < 16; i++) uuidPtr[i],
      ];
      
      final hexBytes = bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).toList();
      
      return '${hexBytes.sublist(0, 4).join('')}-'
             '${hexBytes.sublist(4, 6).join('')}-'
             '${hexBytes.sublist(6, 8).join('')}-'
             '${hexBytes.sublist(8, 10).join('')}-'
             '${hexBytes.sublist(10).join('')}';
    } finally {
      calloc.free(uuidPtr);
    }
  }
  
  // Get a blob as base64 encoded string
  String _getBlobAsBase64(Pointer<Void> value) {
    final bytesPtr = calloc<Pointer<Uint8>>();
    final sizePtr = calloc<Size>();
    
    try {
      final getBytesFunc = _dylib
          .lookup<NativeFunction<Int32 Function(Pointer<Void>, Pointer<Pointer<Uint8>>, Pointer<Size>)>>('cass_value_get_bytes')
          .asFunction<int Function(Pointer<Void>, Pointer<Pointer<Uint8>>, Pointer<Size>)>();
          
      final result = getBytesFunc(value, bytesPtr, sizePtr);
      
      if (result != 0) {
        return ''; // Error getting bytes
      }
      
      final bytes = <int>[];
      final size = sizePtr.value;
      final byteArray = bytesPtr.value;
      
      for (int i = 0; i < size; i++) {
        bytes.add(byteArray[i]);
      }
      
      // Convert to base64
      return base64Encode(bytes);
    } finally {
      calloc.free(bytesPtr);
      calloc.free(sizePtr);
    }
  }
  
  // Wait for a future to complete
  Future<bool> _waitForFuture(Pointer<Void> future) async {
    final readyFunc = _dylib
        .lookup<NativeFunction<Int32 Function(Pointer<Void>)>>('cass_future_ready')
        .asFunction<int Function(Pointer<Void>)>();
        
    final errorCodeFunc = _dylib
        .lookup<NativeFunction<Int32 Function(Pointer<Void>)>>('cass_future_error_code')
        .asFunction<int Function(Pointer<Void>)>();
    
    final waitFunc = _dylib
        .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_future_wait')
        .asFunction<void Function(Pointer<Void>)>();
    
    // Wait for the future to complete
    waitFunc(future);
    
    // Check if it's ready
    if (readyFunc(future) == 0) {
      return false;
    }
    
    // Check error code
    return errorCodeFunc(future) == 0; // 0 is CASS_OK
  }
  
  // Get error message from a future
  String _getFutureError(Pointer<Void> future) {
    final messagePtr = calloc<Pointer<Utf8>>();
    final sizePtr = calloc<Size>();
    
    try {
      final errorMessageFunc = _dylib
          .lookup<NativeFunction<Void Function(Pointer<Void>, Pointer<Pointer<Utf8>>, Pointer<Size>)>>('cass_future_error_message')
          .asFunction<void Function(Pointer<Void>, Pointer<Pointer<Utf8>>, Pointer<Size>)>();
          
      errorMessageFunc(future, messagePtr, sizePtr);
      
      final message = messagePtr.value.toDartString(length: sizePtr.value);
      return message;
    } finally {
      calloc.free(messagePtr);
      calloc.free(sizePtr);
    }
  }
  
  // Free a future
  void _freeFuture(Pointer<Void> future) {
    final freeFunc = _dylib
        .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_future_free')
        .asFunction<void Function(Pointer<Void>)>();
        
    freeFunc(future);
  }

  // Check if connected
  bool get isConnected => _isConnected;

  // Close connection
  Future<bool> close() async {
    if (!_isConnected || _session == null) {
      return true;
    }
    
    final closeFunc = _dylib
        .lookup<NativeFunction<Pointer<Void> Function(Pointer<Void>)>>('cass_session_close')
        .asFunction<Pointer<Void> Function(Pointer<Void>)>();
        
    final future = closeFunc(_session!);
    
    try {
      final result = await _waitForFuture(future);
      
      if (result) {
        _freeSession();
        _freeCluster();
        _isConnected = false;
      }
      
      return result;
    } finally {
      _freeFuture(future);
    }
  }
  
  // Free session
  void _freeSession() {
    if (_session != null) {
      final freeFunc = _dylib
          .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_session_free')
          .asFunction<void Function(Pointer<Void>)>();
          
      freeFunc(_session!);
      _session = null;
    }
  }
  
  // Free cluster
  void _freeCluster() {
    if (_cluster != null) {
      final freeFunc = _dylib
          .lookup<NativeFunction<Void Function(Pointer<Void>)>>('cass_cluster_free')
          .asFunction<void Function(Pointer<Void>)>();
          
      freeFunc(_cluster!);
      _cluster = null;
    }
  }
  
  // Dispose resources
  void dispose() {
    _freeSession();
    _freeCluster();
    _isConnected = false;
  }
}