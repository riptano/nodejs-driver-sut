'use strict';
var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var metrics = require('metrics');
var util = require('util');
var utils = require('../src/utils');
var currentMicros = utils.currentMicros;

var options = utils.parseCommonOptions();

var client = new cassandra.Client({
  contactPoints: [ options.contactPoint || '127.0.0.1' ],
  policies: { loadBalancing: new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy()},
  socketOptions: {
    tcpNoDelay: true,
    readTimeout: 0
  },
  pooling: {
    coreConnectionsPerHost: {'0': options.connectionsPerHost, '1': 1, '2': 0},
    heartBeatInterval: 0
  }
});

var insertQuery = "INSERT INTO t (id) VALUES ('a')";
var limit = options.outstanding;

async.series([
  client.connect.bind(client),
  function createSchema(seriesNext) {
    var queries = [
      "DROP KEYSPACE IF EXISTS ks_iqtt",
      "CREATE KEYSPACE ks_iqtt " +
        "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false",
      "USE ks_iqtt",
      "CREATE TABLE t (id text PRIMARY KEY)"
    ];
    async.eachSeries(queries, client.execute.bind(client), seriesNext);
  },
  function warmup(seriesNext) {
    async.times(100, function (n, next) {
      client.execute(insertQuery, [], {}, next);
    }, function (err) {
      assert.ifError(err);
      utils.outputTestHeader(options);
      seriesNext();
    });
  },
  function insert(seriesNext) {
    console.log('Starting insert test');
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    async.timesSeries(options.series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      utils.timesLimit(options.ops, limit, function (i, next) {
        var queryStart = currentMicros();
        client.execute(insertQuery, null, null, function (err) {
          var duration = currentMicros() - queryStart;
          seriesTimer.update(duration);
          totalTimer.update(duration);
          next(err);
        });
      }, function (err) {
        assert.ifError(err);
        utils.logTimer(seriesTimer);
        nextIteration();
      });
    }, function (err) {
      process.stdout.write('\n');
      console.log("Totals:");
      utils.logTimerHeader();
      utils.logTimer(totalTimer);
      console.log('-------------------------');
      seriesNext(err);
    });
  }], function seriesFinished(err) {
  assert.ifError(err);
  client.shutdown();
});