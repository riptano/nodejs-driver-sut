'use strict';
var cassandra = require('cassandra-driver');
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

var videoIdsLength = videoIds.length;
var commentIdsLength = commentIds.length;

var initSchema = require('bluebird').promisify(utils.initSchema, { context: utils });

initSchema(utils.connectOptions(), keyspace)
  .then(function() {
    return client.connect();
  })
  .then(function () {
    utils.outputTestHeader(options);
    return utils.pTimes(100, function() {
      var params = [types.Uuid.random(), types.TimeUuid.now(), 'hello!'];
      var selectParams = [params[0], params[1]];
      return client.execute(insertQuery, params, { prepare: 1, consistency: types.consistencies.all })
        .then(function (result) {
          client.execute(selectQuery, selectParams, { prepare: 1, consistency: types.consistencies.all });
        });
    });
  })
  .then(function () {
    console.log('Insert test');
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    return utils.pTimesSeries(options.series, function() {
      var seriesTimer = new metrics.Timer();
      return utils.pTimesLimit(options.ops, limit, function (i) {
        var params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength], i.toString()];
        var queryStart = currentMicros();
        return client.execute(insertQuery, params, { prepare: true, consistency: types.consistencies.any})
          .then(function () {
            var duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
          });
      }).then(function () {
        utils.logTimer(seriesTimer);
      });
    }).then(function () {
      utils.logTotals(totalTimer);
    });
  })
  .then(function() {
    console.log('Select test');
    var totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    return utils.pTimesSeries(options.series, function() {
      var seriesTimer = new metrics.Timer();
      return utils.pTimesLimit(options.ops, limit, function(i) {
        var params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength]];
        var queryStart = currentMicros();
        return client.execute(selectQuery, params, {prepare: true, consistency: types.consistencies.any})
          .then(function() {
            var duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
          });
      }).then(function() { 
        utils.logTimer(seriesTimer);
      });
    }).then(function() {
      utils.logTotals(totalTimer);
    });
  }).then(function() {
    console.log("Finished");
    client.shutdown();
  }).catch(function(err) {
    console.error(err.stack);
    console.log("Error encountered", err);
  });
