'use strict';

var ClientWorkload = require('../src/client-workload');

var insertQuery = "INSERT INTO standard1 (key, c0, c1, c2, c3, c4) VALUES (?, ?, ?, ?, ?, ?)";
var selectQuery = "SELECT key, c0, c1, c2, c3, c4 FROM standard1 WHERE key = ?";

var workload = new ClientWorkload('Standard1');
workload
  .queries([
    "DROP KEYSPACE IF EXISTS test_nodejs_benchmarks_standard",
    "CREATE KEYSPACE test_nodejs_benchmarks_standard WITH replication = {'class': 'SimpleStrategy'," +
    " 'replication_factor' : 1} and durable_writes = true",
    "USE test_nodejs_benchmarks_standard",
    "CREATE TABLE standard1 (key blob PRIMARY KEY,c0 blob,c1 blob,c2 blob,c3 blob,c4 blob)"
  ])
  .add('insert', function (client, n, callback) {
    var b = Buffer.allocUnsafe(4);
    b.writeInt32BE(n, 0, true);
    client.execute(insertQuery, [ b, b, b, b, b, b], { prepare: true }, callback);
  })
  .add('select', function (client, n, callback) {
    var b = Buffer.allocUnsafe(4);
    b.writeInt32BE(n, 0, true);
    client.execute(selectQuery, [ b ], { prepare: true }, callback);
  })
  .run({debug: true});