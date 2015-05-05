var cassandra = require('../nodejs-driver');
//var cassandra = require('cassandra-driver');
var assert = require('assert');
var async = require('async');
var types = cassandra.types;

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({
  contactPoints: [ contactPoint ],
  keyspace: 'ks1',
  encoding: { copyBuffer: false},
  socketOptions: { tcpNoDelay: false }
});
var insertQuery = 'INSERT INTO comments_by_video (videoid, username, comment_ts, comment) VALUES (?, ?, ?, ?)';
var selectQuery = 'SELECT videoid, username, comment_ts, comment FROM comments_by_video WHERE videoid = ?';
var ops = 10000;
var ids = Array.apply(null, new Array(10)).map(function () { return types.Uuid.random();});

async.series([
  client.connect.bind(client),
  function warmup(seriesNext) {
    async.timesSeries(10, function (n, next) {
      var params = [types.Uuid.random(), 'u1', types.TimeUuid.now(), 'hello!'];
      client.execute(insertQuery, params, { prepare: 1}, next);
    }, function (err) {
      console.log('Starting tests');
      assert.ifError(err);
      console.log('%d Connections per host', client.hosts.slice(0)[0].pool.connections.length);
      var version = '1.0';
      if (client._setKeyspace) {
        version = '2.1';
      }
      else if (client._setKeyspaceFirst) {
        version = '2.0';
      }
      console.log('Driver v%s', version);
      console.log('-------------------------');
      seriesNext(err);
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
        client.execute(insertQuery, [id, 'u1', tid, 'hello!'], { prepare: true}, function (err) {
          eachNext(err);
        });
      }, function (err) {
        assert.ifError(err);
        var end = process.hrtime(start);
        if (n === 0) {
          console.log('Inserted %d rows', arr.length);
        }
        else if (n === 1) {
          console.log('...');
        }
        elapsed.push(end[0] + end[1] / 1000000000);
        timesNext();
      });
    }, function (err) {
      console.log('mean:   %s secs', mean(elapsed).toFixed(6));
      console.log('median: %s secs', median(elapsed).toFixed(6));
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
        client.eachRow(selectQuery, [id], { prepare: true, autoPage: false}, function (n, row) {
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
      console.log('mean:   %s secs', mean(elapsed).toFixed(6));
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