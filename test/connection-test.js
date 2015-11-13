var cassandra = require('cassandra-driver');
var Connection = require('../node_modules/cassandra-driver/lib/connection');
var requests = require('../node_modules/cassandra-driver/lib/requests');
var assert = require('assert');
var async = require('async');
var types = cassandra.types;
var util = require('util');

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({
  contactPoints: [ contactPoint ]
});
var ops = 200000;
var parallelOps = 1000;
var options = client.options;
options.socketOptions.coalescingThreshold = 8000;
options.socketOptions.readTimeout = 0;
options.pooling.heartBeatInterval = 30000;

var c = new Connection(contactPoint + ':9042', 3, options);
async.series([
  c.open.bind(c),
  function startTest(next) {
    var elapsed = [];
    async.timesSeries(5, function (n, iterationNext) {
      var sendOptions = {};
      var startTime = process.hrtime();
      async.timesLimit(ops, parallelOps, function (n, timesNext) {
        var request = new requests.QueryRequest("INSERT INTO ks1.t (id) VALUES ('a')");
        c.sendStream(request, sendOptions, function (err, response) {
          timesNext(err);
        });
      }, function (err) {
        var end = process.hrtime(startTime);
        elapsed.push(end[0] * 1000 + end[1] / 1000000);
        iterationNext(err);
      });
    }, function (err) {
      var meanElapsed = mean(elapsed);
      console.log('throughput: %s ops/sec', ~~(1000 * ops / meanElapsed));
      next(err);
    });
  },
  c.close.bind(c)
], function (err) {
  assert.ifError(err);
  console.log('finished');
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
