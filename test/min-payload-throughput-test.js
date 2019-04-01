'use strict';
var dse = require('dse-driver');
var assert = require('assert');
var async = require('async');
var metrics = require('metrics');
var util = require('util');
var utils = require('../src/utils');
var currentMicros = utils.currentMicros;

var options = utils.parseCommonOptions();

var client = new dse.Client(utils.connectOptions());

var insertQuery = "INSERT INTO t (id) VALUES ('a')";
var limit = options.outstanding;

client.on('log', (level, className, message) => {
  if (level !== 'verbose') {
    console.log(level, className, message);
  }
});

async.series([
  client.connect.bind(client),
  function createSchema(seriesNext) {
    var queries = [
      // "DROP KEYSPACE IF EXISTS ks_iqtt",
      // "CREATE KEYSPACE ks_iqtt " +
      //   "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false",
      // "USE ks_iqtt",
      "CREATE TABLE IF NOT EXISTS t (id text PRIMARY KEY)"
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
    console.log('Minimum payload test');
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    utils.timesSeries(options.series, function (n, nextIteration) {
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