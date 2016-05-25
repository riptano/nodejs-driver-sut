'use strict';
var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var metrics = require('metrics');
var types = cassandra.types;
var util = require('util');
var utils = require('../src/utils');
var currentMicros = utils.currentMicros;

var options = utils.parseOptions({
  'c': 'contactPoint',
  'ks': 'keyspace',
  'p': 'connectionsPerHost',
  'r': 'ops',
  's': 'series',
  'o': 'outstanding'
}, {
  outstanding: 256,
  connectionsPerHost: 1,
  ops: 100000,
  series: 10
});

var client = new cassandra.Client({
  contactPoints: [ options['contactPoint'] || '127.0.0.1' ],
  keyspace: options['keyspace'] || 'killrvideo',
  policies: { loadBalancing: new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy()},
  encoding: { copyBuffer: false},
  socketOptions: { tcpNoDelay: true },
  pooling: {
    coreConnectionsPerHost: {'0': parseInt(options['connectionsPerHost'], 10), '1': 1, '2': 0},
    heartBeatInterval: 30000
  }
});

var insertQuery = 'INSERT INTO comments_by_video (videoid, commentid, comment) VALUES (?, ?, ?)';
var selectQuery = 'SELECT comment FROM comments_by_video WHERE videoid = ? and commentid = ?';
var ops = parseInt(options['ops'], 10);
var series = parseInt(options['series'], 10);
var commentIds = utils.times(ops / 100, types.TimeUuid.now);
var videoIds = utils.times(ops / 100, types.Uuid.random);
var limit = parseInt(options['outstanding'], 10);

async.series([
  client.connect.bind(client),
  function warmup(seriesNext) {
    async.times(100, function (n, next) {
      var params = [types.Uuid.random(), types.TimeUuid.now(), 'hello!'];
      var selectParams = [params[0], params[1]];
      async.series([
        function (warmupNext) {
          client.execute(insertQuery, params, { prepare: 1, consistency: types.consistencies.all}, warmupNext);
        },
        function (warmupNext) {
          client.execute(selectQuery, selectParams, { prepare: 1, consistency: types.consistencies.all}, warmupNext);
        }
      ], next);
    }, function (err) {
      assert.ifError(err);
      utils.outputTestHeader(options);
      seriesNext();
    });
  },
  function insert(seriesNext) {
    console.log('Starting insert test');
    var videoIdsLength = videoIds.length;
    var commentIdsLength = commentIds.length;
    var totalTimer = new metrics.Timer();
    logTimerHeader();
    utils.timesSeries(series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      utils.timesLimit(ops, limit, function (i, next) {
        var params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength], i.toString()];
        var queryStart = currentMicros();
        client.execute(insertQuery, params, { prepare: true, consistency: types.consistencies.any}, function (err) {
          var duration = currentMicros() - queryStart;
          seriesTimer.update(duration);
          totalTimer.update(duration);
          next(err);
        });
      }, function (err) {
        assert.ifError(err);
        logTimer(seriesTimer);
        nextIteration();
      });
    }, function (err) {
      process.stdout.write('\n');
      console.log("Totals:");
      logTimerHeader();
      logTimer(totalTimer);
      console.log('-------------------------');
      seriesNext(err);
    });
  },
  function select(seriesNext) {
    console.log('Starting select test');
    var totalTimer = new metrics.Timer();
    logTimerHeader();
    async.timesSeries(series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      async.timesLimit(ops, limit, function (n, next) {
        var params = [videoIds[n % 100], commentIds[(~~(n / 100)) % 100]];
        var queryStart = currentMicros();
        client.execute(selectQuery, params, { prepare: true, consistency: types.consistencies.any}, function (err) {
          var duration = currentMicros() - queryStart;
          seriesTimer.update(duration);
          totalTimer.update(duration);
          next(err);
        });
      }, function (err) {
        assert.ifError(err);
        logTimer(seriesTimer);
        nextIteration();
      });
    }, function (err) {
      process.stdout.write('\n');
      console.log("Totals:");
      logTimerHeader();
      logTimer(totalTimer);
      console.log('-------------------------');
      seriesNext(err);
    });
  }], function seriesFinished(err) {
  console.log('Series finished');
  assert.ifError(err);
  client.shutdown();
});

function logTimerHeader() {
  console.log("min,25,50,75,95,98,99,99.9,max,mean,count,thrpt,rss,heapTotal,heapUsed");
}

function logTimer(timer) {
  var percentiles = timer.percentiles([.25,.50,.75,.95,.98,.99,.999]);
  var mem = process.memoryUsage();
  console.log("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
    timer.min().toFixed(2),
    percentiles['0.25'].toFixed(2),
    percentiles['0.5'].toFixed(2),
    percentiles['0.75'].toFixed(2),
    percentiles['0.95'].toFixed(2),
    percentiles['0.98'].toFixed(2),
    percentiles['0.99'].toFixed(2),
    percentiles['0.999'].toFixed(2),
    timer.max().toFixed(2),
    timer.mean().toFixed(2),
    timer.count(),
    timer.meanRate().toFixed(2),
    (mem.rss / 1024.0 / 1024.0).toFixed(2),
    (mem.heapTotal / 1024.0 / 1024.0).toFixed(2),
    (mem.heapUsed / 1024.0 / 1024.0).toFixed(2));
}
