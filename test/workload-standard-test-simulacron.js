'use strict';

var ClientWorkload = require('../src/throttled-client-workload');

var insertQuery = "INSERT INTO standard1 (key, c0, c1, c2, c3, c4) VALUES (?, ?, ?, ?, ?, ?)";
var selectQuery = "SELECT key, c0, c1, c2, c3, c4 FROM standard1 WHERE key = ?";

var workload = new ClientWorkload('Standard1');
workload
  .queries([
    "DROP KEYSPACE IF EXISTS test_nodejs_benchmarks_standard",
    "CREATE KEYSPACE test_nodejs_benchmarks_standard WITH replication = {'class': 'SimpleStrategy'," +
    " 'replication_factor' : 1} and durable_writes = true",
    "USE test_nodejs_benchmarks_standard",
    "CREATE TABLE standard1 (key text PRIMARY KEY,c0 text,c1 text,c2 text,c3 text,c4 text)"
  ])
  .add('insert', function (client, n, callback) {
    var b = n + '';
    client.execute(insertQuery, [ b, b, b, b, b, b], { prepare: true }, callback);
  })
  .add('select', function (client, n, callback) {
    var b = n + '';
    client.execute(selectQuery, [ b ], { prepare: true }, callback);
  })
  .run();
