'use strict';
var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var types = cassandra.types;
var util = require('util');
var utils = require('../src/utils');

var options = utils.parseOptions({
  'c': 'contactPoint',
  'ks': 'keyspace',
  'p': 'connectionsPerHost',
  'r': 'ops',
  'o': 'outstanding'
}, {
  outstanding: 256,
  connectionsPerHost: 1,
  ops: 100000
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
    var elapsed = [];
    var videoIdsLength = videoIds.length;
    var commentIdsLength = commentIds.length;
    async.timesSeries(3, function (n, nextIteration) {
      var start = process.hrtime();
      async.timesLimit(ops, limit, function (i, next) {
        var params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength], i.toString()];
        client.execute(insertQuery, params, { prepare: true, consistency: types.consistencies.any}, function (err) {
          next(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        if (n === 0) {
          console.log('Executed %d times', ops);
        }
        else {
          process.stdout.write('.');
        }
        elapsed.push(end[0] + end[1] / 1000000000);
        nextIteration();
      });
    }, function (err) {
      process.stdout.write('\n');
      var meanElapsed = utils.mean(elapsed);
      console.log('mean:   %s secs', meanElapsed.toFixed(4));
      console.log('median: %s secs', utils.median(elapsed).toFixed(4));
      console.log('throughput (mean): %s ops/sec', ~~(ops / meanElapsed));
      console.log('-------------------------');
      seriesNext(err);
    });
  },
  function select(seriesNext) {
    console.log('Starting select test');
    var elapsed = [];
    async.timesSeries(5, function (n, nextIteration) {
      var start = process.hrtime();
      async.timesLimit(ops, limit, function (n, next) {
        var params = [videoIds[n % 100], commentIds[(~~(n / 100)) % 100], n.toString()];
        client.execute(insertQuery, params, { prepare: true, consistency: types.consistencies.any}, function (err) {
          next(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        if (n === 0) {
          console.log('Executed %d times', ops);
        }
        else {
          process.stdout.write('.');
        }
        elapsed.push(end[0] + end[1] / 1000000000);
        nextIteration();
      });
    }, function (err) {
      process.stdout.write('\n');
      var meanElapsed = utils.mean(elapsed);
      console.log('mean:   %s secs', meanElapsed.toFixed(4));
      console.log('median: %s secs', utils.median(elapsed).toFixed(4));
      console.log('throughput (mean): %s ops/sec', ~~(ops / meanElapsed));
      console.log('-------------------------');
      seriesNext(err);
    });
  }], function seriesFinished(err) {
  console.log('Series finished');
  assert.ifError(err);
  client.shutdown();
});