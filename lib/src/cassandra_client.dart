import 'dart:ffi';
import 'dart:io' show Platform, File;
import 'package:ffi/ffi.dart';

// Simple Cassandra client that only handles connection
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