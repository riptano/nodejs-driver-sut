'use strict';
var util = require('util');
var Socket = require('net').Socket;
var EventEmitter = require('events').EventEmitter;
var Histogram = require('native-hdr-histogram');
var LinkedList = require('linkedlist');

/**
 * A custom reporter that stores the graphite metrics locally.
 * @param {String} baseTime A timestamp to be used as base to report
 * @constructor
 */
var LocalMemRecorder = module.exports = function(baseTime) {
  this.localSnapshots = new LinkedList();
  var currentTime = (new Date()).getTime();
  this.timeshift = (currentTime - baseTime) / 1000;
}

util.inherits(LocalMemRecorder, EventEmitter);

LocalMemRecorder.prototype.send = function(name, value, timestamp) {
  this.localStorage.push(util.format('%s.%s %s %s\n', this.prefix, name, value,
  timestamp));
};

LocalMemRecorder.prototype.start = function(callback, intervalInMs, testStartTime) {
  this.interval = setInterval(callback, intervalInMs);
  this.testStart = (new Date).getTime();
  return this.testStart;
};

LocalMemRecorder.prototype.stop = function() {
  if('interval' in this) {
    clearInterval(this.interval);
  }
};

LocalMemRecorder.prototype.record = function(counter, errorCounter, histogram) {
  this.localSnapshots.push({
    timestamp: (new Date).getTime(), 
    counter: counter,
    errCounter : errorCounter,
    histogram: {
      min: histogram.min(),
      max: histogram.max(),
      p50: histogram.percentile(50),
      p75: histogram.percentile(75),
      p95: histogram.percentile(95),
      p98: histogram.percentile(98),
      p99: histogram.percentile(99),
      p999: histogram.percentile(99.9),
    }    
  });  
};

LocalMemRecorder.prototype.reportGraphite = function(host, port, prefix, itemName, callback) {
  var self = this;
  this.socket = new Socket();
  this.socket.on('error', function(exc) {
    if(!reconnecting) {
      reconnecting = true;
      self.emit('log', 'warn', util.format('Lost connection to %s. Will reconnect in 10 seconds.', host), exc);
      setTimeout(function () {
        reconnecting = false;
        self.reportGraphite(host, port, callback);
      }, 1000);
    }
  });

  self.emit('log', 'verbose', util.format("Connecting to graphite @ %s:%d", host, port));
  this.socket.connect(port, host, function() {
    self.emit('log', 'verbose', util.format('Successfully connected to graphite @ %s:%d.', host, port));
    self.emit('log', 'verbose', util.format('Attempt to send %d metrics records to graphite @ %s:%d.', self.localSnapshots.length, host, port));
    var requestMetricName = itemName + '.requests';
    var errorMetricName = itemName + '.errors';
    while (self.localSnapshots.length) {
      var snapshot = self.localSnapshots.shift();
      var timestampToReport = (snapshot.timestamp / 1000) - self.timeshift;
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.count', snapshot.counter, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, errorMetricName + '.count', snapshot.errCounter, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.mean_rate', self.getThroughput(snapshot).toFixed(2), timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.min', snapshot.histogram.min, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.max', snapshot.histogram.max, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.p50', snapshot.histogram.p50, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.p75', snapshot.histogram.p75, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.p98', snapshot.histogram.p98, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.p99', snapshot.histogram.p99, timestampToReport));
      self.socket.write(util.format('%s.%s %s %s\n', prefix, requestMetricName + '.p999', snapshot.histogram.p999, timestampToReport));
    }
    self.emit('log', 'verbose', util.format('Successfully sent all metrics to graphite @ %s:%d.', host, port));
    self.socket.end();
    callback();
  });  
};

LocalMemRecorder.prototype.reportConsole = function() {
  const durationUnit = 'ms';
  if (this.localSnapshots.length) {
    var snapshot = this.localSnapshots.tail;
    console.log('            errors = %s', snapshot.errCounter);
    console.log('           counter = %s requests', snapshot.counter);
    console.log('          duration = %ss', (snapshot.timestamp - this.testStart) / 1000);
    console.log('               min = %s%s', snapshot.histogram.min, durationUnit);
    console.log('               max = %s%s', snapshot.histogram.max, durationUnit);
    console.log('         mean rate = %s req/sec', this.getThroughput(snapshot).toFixed(2));
    console.log('              50%% <= %s%s', snapshot.histogram.p50, durationUnit);
    console.log('              75%% <= %s%s', snapshot.histogram.p75, durationUnit);
    console.log('              95%% <= %s%s', snapshot.histogram.p95, durationUnit);
    console.log('              98%% <= %s%s', snapshot.histogram.p98, durationUnit);
    console.log('              99%% <= %s%s', snapshot.histogram.p99, durationUnit);
    console.log('            99.9%% <= %s%s', snapshot.histogram.p999, durationUnit);
    }
};

LocalMemRecorder.prototype.getThroughput = function(snapshot) {
  return (snapshot.counter / (snapshot.timestamp - this.testStart) * 1000);
}
