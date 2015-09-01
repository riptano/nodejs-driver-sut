var net = require('net');
var Stats = require('fast-stats').Stats;

function MetricsTracker(host, port, driverVersion) {
  this.host = host;
  this.port = port;
  this.baseKey = 'sut.nodejs-driver.' + driverVersion.replace(/\./g, "_") + '.';
  this.socket = new net.Socket();
  this._statMap = null;
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
  var timestamp = ~~ (Date.now() / 1000);
  if (this._time !== timestamp) {
    //store current map
    this._writeMap(this._time, this._statMap);
    //create a new map
    this._time = timestamp;
    this._statMap = {
      throughput: {},
      latency: {}
    };
  }
  //latency in micros
  var latency = diff[0] * 1000000 + (~~ (diff[1] / 1000));
  var map = this._statMap;
  map.throughput[key] = (map.throughput[key] || 0) + 1;
  //latency in micros
  map.latency[key] = (map.latency[key] || new Stats({ bucket_precision: 10 })).push(latency);
  callback();
};

/**
 * Writes to the wire the values of the stat map
 * @param {Number} timestamp
 * @param map
 * @private
 */
MetricsTracker.prototype._writeMap = function (timestamp, map) {
  if (!map) {
    return;
  }
  for (var key in map.throughput) {
    if (!map.throughput.hasOwnProperty(key)) {
      continue;
    }
    this.socket.write(this.baseKey + key + '.throughput ' + map.throughput[key] + ' ' + timestamp + '\n');
    var latencies = map.latency[key];
    this.socket.write(this.baseKey + key + '.p50.latency ' + latencies.percentile(50)  + ' ' + timestamp + '\n');
    this.socket.write(this.baseKey + key + '.p90.latency ' + latencies.percentile(90)  + ' ' + timestamp + '\n');
    this.socket.write(this.baseKey + key + '.p95.latency ' + latencies.percentile(95)  + ' ' + timestamp + '\n');
    this.socket.write(this.baseKey + key + '.p990.latency ' + latencies.percentile(99)  + ' ' + timestamp + '\n');
    this.socket.write(this.baseKey + key + '.max.latency ' + latencies.percentile(100)  + ' ' + timestamp + '\n');
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