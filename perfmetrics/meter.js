'use strict';
var MetricsMeter = require('metrics').Meter,
util = require('util');

/**
 * @param {Number} startTime timestamp of the init.
 * @param {Number} stopTime timestamp of the end.
 * @constructor
 */
var Meter = module.exports = function(startTime, stopTime) {
  Meter.super_.call(this);
  this.startTime = startTime;
  this.stopTime = stopTime;
}

util.inherits(Meter, MetricsMeter);

Meter.prototype.meanRate = function() { 
  return this.count / (this.stopTime - this.startTime) * 1000;
}

Meter.prototype.mark = function(n) {
  if (!n) { n = 1; }
  this.count += n;
}

Meter.prototype.rates = function() {
  return {mean: this.meanRate()};
}