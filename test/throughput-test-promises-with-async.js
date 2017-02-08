'use strict';
var cassandra = require('cassandra-driver');
var assert = require('assert');
var metrics = require('metrics');
var types = cassandra.types;
var utils = require('../src/utils');
var currentMicros = utils.currentMicros;

var options = utils.parseCommonOptions();

var keyspace = options.keyspace || 'killrvideo';
var client = new cassandra.Client(utils.extend({ keyspace: keyspace }, utils.connectOptions()));
var insertQuery = 'INSERT INTO comments_by_video (videoid, commentid, comment) VALUES (?, ?, ?)';
var selectQuery = 'SELECT comment FROM comments_by_video WHERE videoid = ? and commentid = ?';
var commentIds = utils.times(options.ops / 100, types.TimeUuid.now);
var videoIds = utils.times(options.ops / 100, types.Uuid.random);
var limit = options.outstanding;

utils.series([
  function initSchema(seriesNext) {
    utils.initSchema(utils.connectOptions(), keyspace, seriesNext);
  },
  client.connect.bind(client),
  function warmup(seriesNext) {
    utils.aTimes(100, function (n, next) {
      var params = [types.Uuid.random(), types.TimeUuid.now(), 'hello!'];
      var selectParams = [params[0], params[1]];
      utils.series([
        function (warmupNext) {
          client.execute(insertQuery, params, { prepare: 1, consistency: types.consistencies.all})
            .then(function (result) { 
              warmupNext(null, result);
            })
            .catch(function (err) {
              warmupNext(err);
            });
        },
        function (warmupNext) {
          client.execute(selectQuery, selectParams, { prepare: 1, consistency: types.consistencies.all})
            .then(function (result) {
              warmupNext(null, result);
            })
            .catch(function (err) {
              warmupNext(err);
            });
        }
      ], next);
    }, function (err) {
      assert.ifError(err);
      utils.outputTestHeader(options);
      seriesNext();
    });
  },
  function insert(seriesNext) {
    console.log('Insert test');
    var videoIdsLength = videoIds.length;
    var commentIdsLength = commentIds.length;
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    utils.timesSeries(options.series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      utils.timesLimit(options.ops, limit, function (i, next) {
        var params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength], i.toString()];
        var queryStart = currentMicros();
        client.execute(insertQuery, params, { prepare: true, consistency: types.consistencies.any})
          .then(function () {
            var duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
            next(null);
          })
          .catch(function (err) {
            next(err);
          });
      }, function (err) {
        assert.ifError(err);
        utils.logTimer(seriesTimer);
        nextIteration();
      });
    }, function (err) {
      assert.ifError(err);
      utils.logTotals(totalTimer);
      seriesNext(err);
    });
  },
  function select(seriesNext) {
    console.log('Select test');
    var videoIdsLength = videoIds.length;
    var commentIdsLength = commentIds.length;
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    utils.timesSeries(options.series, function (n, nextIteration) {
      var seriesTimer = new metrics.Timer();
      utils.timesLimit(options.ops, limit, function (n, next) {
        var params = [videoIds[n % videoIdsLength], commentIds[(~~(n / 100)) % commentIdsLength]];
        var queryStart = currentMicros();
        client.execute(selectQuery, params, { prepare: true, consistency: types.consistencies.any})
          .then(function () {
            var duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
            next(null);
          })
          .catch(function (err) {
            next(err);
          });
      }, function (err) {
        assert.ifError(err);
        utils.logTimer(seriesTimer);
        nextIteration();
      });
    }, function (err) {
      assert.ifError(err);
      utils.logTotals(totalTimer);
      seriesNext(err);
    });
  }], function seriesFinished(err) {
  console.log('Series finished');
  assert.ifError(err);
  client.shutdown();
});
