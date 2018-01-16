'use strict';
var GraphiteReporter = require('metrics').GraphiteReporter,
  ScheduledReporter = require('metrics').ScheduledReporter,
  ConsoleReporter = require('metrics').ConsoleReporter,
  Counter = require('metrics').Counter,
  Histogram = require('metrics').Histogram,
  util = require('util'),
  Timer = require('./timer'),
  Meter = require('./meter'),
  Socket = require('net').Socket;

var reconnecting = false;

/**
 * A custom reporter that stores the graphite metrics locally.
 * @param {Report} registry report instance whose metrics to report on.
 * @param {String} prefix A string to prefix on each metric (i.e. app.hostserver)
 * @param {String} baseTime A timestamp to be used as base to report
 * @constructor
 */
var LocalMemReporter = module.exports = function(registry, prefix, baseTime) {
  LocalMemReporter.super_.call(this, registry, prefix, null, 0);
  this.localStorage = [];
  var currentTime = (new Date()).getTime();
  this.timeshift = (currentTime - baseTime) / 1000;
}

util.inherits(LocalMemReporter, GraphiteReporter);

/**
 * Instead of send metrics to graphite server, store its values on an array.
 * @param {String} name Metric name
 * @param {Number} value Metric value
 * @param {Number} timestamp Metric timestamp
 */
LocalMemReporter.prototype.send = function(name, value, timestamp) {
  this.localStorage.push(util.format('%s.%s %s %s\n', this.prefix, name, value,
  timestamp));
};

LocalMemReporter.prototype.start = function(intervalInMs) {
  ScheduledReporter.prototype.start.call(this, intervalInMs);
};

LocalMemReporter.prototype.stop = function() {
  ScheduledReporter.prototype.stop.call(this);
};


/**
 * Retrieve the metrics associated with the report given to this reporter in a format that's easy to consume
 * by reporters.  That is an object with separate references for meters, timers counters, and histograms.
 * @returns {{meters: Array, timers: Array, counters: Array}}
 */
LocalMemReporter.prototype.getMetrics = function() {
  var meters = [];
  var timers = [];
  var counters = [];
  var histograms = [];

  var trackedMetrics = this.registry.trackedMetrics;
  // Flatten metric name to be namespace.name is has a namespace and separate out metrics
  // by type.
  for(var namespace in trackedMetrics) {
    for(var name in trackedMetrics[namespace]) {
      var metric = trackedMetrics[namespace][name];
      if(namespace.length > 0) {
        metric.name = namespace + '.' + name;
      } else {
        metric.name = name;
      }
      var metricType = Object.getPrototypeOf(metric);
      if(metric.type === 'meter') {
        meters.push(metric);
      } else if(metric.type === 'timer') {
        timers.push(metric);
      }
    }
  }

  return { meters: meters, timers: timers, counters: counters, histograms: histograms };
};

LocalMemReporter.prototype.reportTimer = function(timer, timestamp) {
  var send = this.send.bind(this);
  send(util.format('%s.%s', timer.name, 'count'), timer.count(), timestamp);
  send(util.format('%s.%s', timer.name, 'mean_rate'), timer.meanRate(), timestamp);
  this.reportHistogram(timer, timestamp);
};

/**
 * Same implementation as metrics.GraphiteReporter, the only difference is the 
 * timeshift used on timestamp if you want to generate metrics to compare with 
 * other test run.
 */
LocalMemReporter.prototype.report = function(timestampIn) {
  var metrics = this.getMetrics();
  var self = this;
  var timestamp = timestampIn - this.timeshift;

  if(metrics.counters.length != 0) {
    metrics.counters.forEach(function (count) {
      self.reportCounter.bind(self)(count, timestamp);
    })
  }

  if(metrics.meters.length != 0) {
    metrics.meters.forEach(function (meter) {
      self.reportMeter.bind(self)(meter, timestamp);
    })
  }

  if(metrics.timers.length != 0) {
    metrics.timers.forEach(function (timer) {
      // Don't log timer if its recorded no metrics.
      if(timer.min() != null) {
        self.reportTimer.bind(self)(timer, timestamp);
      }
    })
  }

  if(metrics.histograms.length != 0) {
    metrics.histograms.forEach(function (histogram) {
      // Don't log histogram if its recorded no metrics.
      if(histogram.min != null) {
        self.reportHistogram.bind(self)(histogram, timestamp);
      }
    })
  }
};

LocalMemReporter.prototype.reportGraphite = function(host, port, callback) {
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
    self.emit('log', 'verbose', util.format('Attempt to send %d metrics records to graphite @ %s:%d.', self.localStorage.length, host, port));
    for(var i = 0; i < self.localStorage.length; i++) {
      self.socket.write(self.localStorage[i]);
    }
    self.emit('log', 'verbose', util.format('Successfully sent all metrics to graphite @ %s:%d.', host, port));
    self.socket.end();
    callback();
  });  
};

LocalMemReporter.prototype.reportConsole = function() {
  ConsoleReporter.prototype.report.call(this);
};
