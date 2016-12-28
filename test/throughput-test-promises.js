'use strict';
var cassandra = require('cassandra-driver');
var assert = require('assert');
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
  },
  promiseFactory: options.promiseFactory
});

var insertQuery = 'INSERT INTO comments_by_video (videoid, commentid, comment) VALUES (?, ?, ?)';
var selectQuery = 'SELECT comment FROM comments_by_video WHERE videoid = ? and commentid = ?';
var commentIds = utils.times(options.ops / 100, types.TimeUuid.now);
var videoIds = utils.times(options.ops / 100, types.Uuid.random);
var limit = options.outstanding;

const videoIdsLength = videoIds.length;
const commentIdsLength = commentIds.length;

client.connect()
  .then(() => {
    utils.outputTestHeader(options);
    return utils.pTimes(100, () => {
      var params = [types.Uuid.random(), types.TimeUuid.now(), 'hello!'];
      var selectParams = [params[0], params[1]];
      return client.execute(insertQuery, params, { prepare: 1, consistency: types.consistencies.all })
        .then(result => client.execute(selectQuery, selectParams, { prepare: 1, consistency: types.consistencies.all }));
    });
  })
  .then(() => {
    console.log('Insert test');
    const totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    return utils.pTimesSeries(options.series, () => {
      const seriesTimer = new metrics.Timer();
      return utils.pTimesLimit(options.ops, limit, (i) => {
        const params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength], i.toString()];
        const queryStart = currentMicros();
        return client.execute(insertQuery, params, { prepare: true, consistency: types.consistencies.any})
          .then(() => {
            const duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
          });
      }).then(() => utils.logTimer(seriesTimer));
    }).then(() => utils.logTotals(totalTimer));
  })
  .then(() => {
    console.log('Select test');
    const totalTimer = new metrics.Timer();
    utils.logTimerHeader();
    return utils.pTimesSeries(options.series, () => {
      const seriesTimer = new metrics.Timer();
      return utils.pTimesLimit(options.ops, limit, (i) => {
        const params = [videoIds[i % videoIdsLength], commentIds[(~~(i / 100)) % commentIdsLength]];
        const queryStart = currentMicros();
        return client.execute(selectQuery, params, {prepare: true, consistency: types.consistencies.any})
          .then(() => {
            const duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
          });
      }).then(() => utils.logTimer(seriesTimer));
    }).then(() => utils.logTotals(totalTimer));
  }).then(() => {
    console.log("Finished");
    client.shutdown();
  }).catch(err => console.log("Error encountered", err));
