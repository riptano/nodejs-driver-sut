var net = require('net');
var Stats = require('fast-stats').Stats;

//amount of seconds for each slice
var throughputSliceLength = 1;
var latencySliceLength = 30;

function MetricsTracker(host, port, driverVersion) {
  this.host = host;
  this.port = port;
  this.baseKey = 'sut.nodejs-driver.' + driverVersion.replace(/\./g, "_") + '.';
  this.socket = new net.Socket();
  this._throughputMap = null;
  this._latencyMap = null;
  this._throughputSliceTime = null;
  this._latencySliceTime = null;
}

MetricsTracker.prototype.connect = function (callback) {
  var self = this;
  this.socket.once('error', function (err) {
    console.error('It was not possible to connect to %s:%s: %j', self.host, self.port, err);
    callback(err);
  });
  this.socket.connect(this.port, this.host, function connectCallback() {
    self.socket.removeAllListeners('error');
    self.socket.on('close', function () {
      console.log('Connection to %s:%s was closed', self.host, self.port);
    });
    callback();
  });
};

MetricsTracker.prototype.update = function (key, diff, callback) {
  //timestamp in seconds
  var timestamp = Date.now() / 1000;
  var throughputSliceTime = ~~(timestamp / throughputSliceLength);
  var latencySliceTime = ~~(timestamp / latencySliceLength);
  if (this._throughputSliceTime !== throughputSliceTime) {
    //save current map
    this._writeThroughput(this._throughputSliceTime * throughputSliceLength, this._throughputMap);
    //create a new map
    this._throughputSliceTime = throughputSliceTime;
    //the previous map gets de-referenced and will be GC
    this._throughputMap = {};
  }
  if (this._latencySliceTime !== latencySliceTime) {
    //save current map
    this._writeLatency(this._latencySliceTime * latencySliceLength, this._latencyMap);
    //create a new map
    this._latencySliceTime = latencySliceTime;
    //the previous map gets de-referenced and will be GC
    this._latencyMap = {};
  }
  //latency in micros
  var latency = diff[0] * 1000000 + (~~ (diff[1] / 1000));
  this._throughputMap[key] = (this._throughputMap[key] || 0) + 1;
  this._latencyMap[key] = (this._latencyMap[key] || new Stats({ bucket_precision: 10 })).push(latency);
  callback();
};

/**
 * Writes to the wire the values of the stat map
 * @param {Number} timestamp
 * @param map
 * @private
 */
MetricsTracker.prototype._writeThroughput = function (timestamp, map) {
  if (!map) {
    return;
  }
  for (var key in map) {
    if (!map.hasOwnProperty(key)) {
      continue;
    }
    this.socket.write(this.baseKey + key + '.throughput ' + ~~(map[key] / throughputSliceLength) + ' ' + timestamp + '\n');
  }
};

MetricsTracker.prototype._writeLatency = function (timestamp, map) {
  if (!map) {
    return;
  }
  for (var key in map) {
    if (!map.hasOwnProperty(key)) {
      continue;
    }
    var latencies = map[key];
    var message =
      this.baseKey + key + '.p50.latency '  + latencies.percentile(50)  + ' ' + timestamp + '\n' +
      this.baseKey + key + '.p90.latency '  + latencies.percentile(90)  + ' ' + timestamp + '\n' +
      this.baseKey + key + '.p95.latency '  + latencies.percentile(95)  + ' ' + timestamp + '\n' +
      this.baseKey + key + '.p990.latency ' + latencies.percentile(99)  + ' ' + timestamp + '\n' +
      this.baseKey + key + '.max.latency '  + latencies.percentile(100) + ' ' + timestamp + '\n';
    this.socket.write(message);
  }
};

MetricsTracker.prototype._write = function (key, value, timestamp) {
  this.socket.write(key + ' ' + value + ' ' + timestamp + '\n');
};

MetricsTracker.prototype.shutdown = function (callback) {
  this.socket.once('close', function (hadError) {
    if (hadError) {
      self.log('info', 'The socket closed with a transmission error');
    }
    callback();
  });
  this.socket.end();
};

module.exports = MetricsTracker;