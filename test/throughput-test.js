var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var types = cassandra.types;
var fs = require('fs');

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({
  contactPoints: [ contactPoint ],
  keyspace: process.argv[3] || 'killrvideo',
  encoding: { copyBuffer: false},
  socketOptions: { tcpNoDelay: false },
  pooling: { coreConnectionsPerHost: {'0': parseInt(process.argv[3]) || 8, '1': 1, '2': 0} }
});
var insertQuery = 'INSERT INTO comments_by_video (videoid, userid, commentid, comment) VALUES (?, ?, ?, ?)';
var selectQuery = 'SELECT videoid, userid, commentid, comment FROM comments_by_video WHERE videoid = ?';
var ops = 10000;
var ids = Array.apply(null, new Array(10)).map(function () { return types.Uuid.random();});
var userId = types.Uuid.random();

async.series([
  client.connect.bind(client),
  function warmup(seriesNext) {
    async.timesSeries(10, function (n, next) {
      var params = [types.Uuid.random(), userId, types.TimeUuid.now(), 'hello!'];
      client.execute(insertQuery, params, { prepare: 1}, next);
    }, function (err) {
      console.log('Starting tests');
      assert.ifError(err);
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
    async.timesSeries(10, function (n, timesNext) {
      var start = process.hrtime();
      var id = ids[n];
      if (!id) {
        return timesNext();
      }
      async.eachLimit(arr, 50, function (tid, eachNext) {
        client.execute(insertQuery, [id, userId, tid, 'hello!'], { prepare: true, consistency: types.consistencies.any}, function (err) {
          eachNext(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        if (n === 0) {
          console.log('Inserted %d rows', arr.length);
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
  function select (seriesNext) {
    console.log('Starting select test');
    var elapsed = [];
    async.timesSeries(10, function (n, timesNext) {
      async.eachLimit(ids, 3, function (id, next) {
        var start = process.hrtime();
        var retrieved = 0;
        client.eachRow(selectQuery, [id], { prepare: true, autoPage: true}, function (n, row) {
          //use the row value for something
          retrieved += row['videoid'].toString().length;
        }, function (err) {
          assert.ifError(err);
          var end = process.hrtime(start);
          elapsed.push(end[0] + end[1] / 1000000000);
          next();
        });
      }, timesNext);
    }, function (err) {
      var meanElapsed = mean(elapsed);
      console.log('mean:   %s secs', meanElapsed.toFixed(6));
      console.log('median: %s secs', median(elapsed).toFixed(6));
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