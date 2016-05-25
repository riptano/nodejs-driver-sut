'use strict';
var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var metrics = require('metrics');
var types = cassandra.types;
var util = require('util');
var utils = require('../src/utils');
var currentMicros = utils.currentMicros;

var options = utils.parseCommonOptions();

var client = new cassandra.Client({
  contactPoints: [ options.contactPoint || '127.0.0.1' ],
  keyspace: options.keyspace || 'killrvideo',
  policies: { loadBalancing: new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy()},
  socketOptions: { tcpNoDelay: true },
  pooling: {
    coreConnectionsPerHost: {'0': options.connectionsPerHost, '1': 1, '2': 0},
    heartBeatInterval: 30000
  }
});

var insertQuery = 'INSERT INTO comments_by_video (videoid, commentid, comment) VALUES (?, ?, ?)';
var selectQuery = 'SELECT comment FROM comments_by_video WHERE videoid = ? and commentid = ?';
var commentIds = utils.times(options.ops / 100, types.TimeUuid.now);
var videoIds = utils.times(options.ops / 100, types.Uuid.random);
var limit = options.outstanding;

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
    utils.logTimerHeader();
    utils.timesSeries(options.series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      utils.timesLimit(options.ops, limit, function (i, next) {
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
  },
  function select(seriesNext) {
    console.log('Starting select test');
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    async.timesSeries(options.series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      async.timesLimit(options.ops, limit, function (n, next) {
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
  console.log('Series finished');
  assert.ifError(err);
  client.shutdown();
});
