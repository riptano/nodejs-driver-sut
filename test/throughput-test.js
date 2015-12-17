var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var types = cassandra.types;
var fs = require('fs');
var util = require('util');

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({
  contactPoints: [ contactPoint ],
  keyspace: process.argv[3] || 'killrvideo',
  policies: { loadBalancing: new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy('datacenter1')},
  encoding: { copyBuffer: false},
  socketOptions: { tcpNoDelay: true },
  pooling: { coreConnectionsPerHost: {'0': parseInt(process.argv[4], 10) || 1, '1': 1, '2': 0} }
});
var insertQuery = 'INSERT INTO comments_by_video (videoid, userid, commentid, comment) VALUES (?, ?, ?, ?)';
var selectQuery = 'SELECT videoid, userid, commentid, comment FROM comments_by_video WHERE videoid = ? and commentid = ?';
var ops = 40000;
var ids = Array.apply(null, new Array(10)).map(function () { return types.Uuid.random();});
var userId = types.Uuid.random();
var selectParams;

async.series([
  client.connect.bind(client),
  function warmup(seriesNext) {
    async.times(100, function (n, next) {
      var params = [types.Uuid.random(), userId, types.TimeUuid.now(), 'hello!'];
      //it will be later be used for selecting
      selectParams = [params[0], params[2]];
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
      console.log('Starting tests');
      console.log('%d Connections per host', client.hosts.slice(0)[0].pool.connections.length);
      var driverVersion = JSON.parse(fs.readFileSync('node_modules/cassandra-driver/package.json', 'utf8')).version;
      console.log('Driver v%s', driverVersion);
      console.log('-------------------------');
      seriesNext();
    });
  },
  function insert(seriesNext) {
    console.log('Starting insert test');
    var elapsed = [];
    var arr = Array.apply(null, new Array(ops)).map(function () { return types.TimeUuid.now() });
    async.timesSeries(5, function (n, timesNext) {
      var start = process.hrtime();
      var id = ids[n];
      if (!id) {
        return timesNext();
      }
      async.eachLimit(arr, 256, function (tid, eachNext) {
        client.execute(insertQuery, [id, userId, tid, 'hello!'], { prepare: true, consistency: types.consistencies.any}, function (err) {
          eachNext(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        if (n === 0) {
          console.log('Executed %d times', arr.length);
        }
        else {
          process.stdout.write('.');
        }
        elapsed.push(end[0] + end[1] / 1000000000);
        timesNext();
      });
    }, function (err) {
      process.stdout.write('\n');
      var meanElapsed = mean(elapsed);
      console.log('mean:   %s secs', meanElapsed.toFixed(6));
      console.log('median: %s secs', median(elapsed).toFixed(6));
      console.log('throughput (mean): %s ops/sec', ~~(ops / meanElapsed));
      console.log('-------------------------');
      seriesNext(err);
    });
  },
  function select(seriesNext) {
    console.log('Starting select test');
    var elapsed = [];
    var arr = Array.apply(null, new Array(ops)).map(function () { return types.TimeUuid.now() });
    async.timesSeries(5, function (n, timesNext) {
      var start = process.hrtime();
      var id = ids[n];
      if (!id) {
        return timesNext();
      }
      async.eachLimit(arr, 256, function (tid, eachNext) {
        client.execute(selectQuery, selectParams, { prepare: true, consistency: types.consistencies.one}, function (err) {
          eachNext(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        if (n === 0) {
          console.log('Executed %d times', arr.length);
        }
        else {
          process.stdout.write('.');
        }
        elapsed.push(end[0] + end[1] / 1000000000);
        timesNext();
      });
    }, function (err) {
      process.stdout.write('\n');
      var meanElapsed = mean(elapsed);
      console.log('mean:   %s secs', meanElapsed.toFixed(6));
      console.log('median: %s secs', median(elapsed).toFixed(6));
      console.log('throughput (mean): %s ops/sec', ~~(ops / meanElapsed));
      console.log('-------------------------');
      seriesNext(err);
    });
  }], function seriesFinished(err) {
  console.log('Series finished');
  assert.ifError(err);
  client.shutdown();
});

function mean(arr) {
  return (arr.reduce(function (p, v) { return p + v; }, 0) / arr.length);
}

function median(arr) {
  arr = arr.slice(0).sort();
  var num = arr.length;
  if (num % 2 !== 0) {
    return arr[(num - 1) / 2];
  }
  // even: return the average of the two middle values
  var left = arr[num / 2 - 1];
  var right = arr[num / 2];

  return (left + right) / 2;
}
