var net = require('net');

function MetricsTracker(host, port) {
  this.host = host;
  this.port = port;
  this.socket = new net.Socket();
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
  var micros = diff[0] * 1000000 + (~~ (diff[1] / 1000));
  //timestamp in seconds
  var timestamp = ~~ (Date.now() / 1000);
  this.socket.write(key + '.latency ' + micros + ' ' + timestamp + '\n');
  callback();
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