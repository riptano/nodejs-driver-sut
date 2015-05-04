var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var types = cassandra.types;

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({ contactPoints: [ contactPoint ], keyspace: 'ks1'});
var ops = 10000;
var insertQuery = 'INSERT INTO comments_by_video (videoid, username, comment_ts, comment) VALUES (?, ?, ?, ?)';

async.series([
  client.connect.bind(client),
  function warmup(seriesNext) {
    async.timesSeries(10, function (n, next) {
      var params = [types.Uuid.random(), 'u1', types.TimeUuid.now(), 'hello!'];
      client.execute(insertQuery, params, { prepare: 1}, next);
    }, seriesNext);
  },
  function insert(seriesNext) {
    var arr = new Array(ops);
    async.timesSeries(5, function (n, timesNext) {
      var start = process.hrtime();
      var id = types.Uuid.random();
      async.eachLimit(arr, 400, function (i, eachNext) {
        client.execute(insertQuery, [id, 'u1', types.TimeUuid.now(), 'hello!'], { prepare: true}, function (err) {
          eachNext(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        console.log('elapsed (s, nanos)', end);
        timesNext();
      });
    }, seriesNext);
  }], function seriesFinished(err) {
  assert.ifError(err);
  client.shutdown();
});