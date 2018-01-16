'use strict';
var Histogram = require('metrics').Histogram,
    MetricsTimer = require('metrics').Timer,
    SimpleMeter = require('./meter'),
    ExponentiallyDecayingSample = require('metrics').ExponentiallyDecayingSample,
    util = require('util');

/*
*  A delay a timer tracks the rate of events and histograms the durations
*/
var Timer = module.exports = function Timer(startTime, stopTime) {
  Timer.super_.call(this);
  this.startTime = startTime;
  this.stopTime = stopTime;
  this.meter = new SimpleMeter(startTime, stopTime);
}

util.inherits(Timer, MetricsTimer);

Timer.prototype.setStopTime = function(stopTime) {
  this.stopTime = stopTime;
  this.meter.stopTime = stopTime;
}
