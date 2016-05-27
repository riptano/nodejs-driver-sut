"use strict";
var helper = require('./helper');

var baseOptions = {
  consistency: 1,
  fetchSize: 5000,
  prepare: false,
  retryOnTimeout: true,
  captureStackTrace: false
};

var options = [
  {
    prepare: true,
    readTimeout: 5000
  },
  {},
  null,
  { prepare: false }
];

//traceQuery, retry, readTimeout, customPayload
function dynamicClone(o) {
  return extend({}, baseOptions, o);
}

var emptyObject = {};

function manualClose(o) {
  o = o || emptyObject;
  return ({
    consistency: ifUndefined(o.consistency, baseOptions.consistency),
    fetchSize: ifUndefined(o.fetchSize, baseOptions.fetchSize),
    prepare: ifUndefined(o.prepare, baseOptions.prepare),
    retryOnTimeout: ifUndefined(o.retryOnTimeout, baseOptions.retryOnTimeout),
    traceQuery: ifUndefined(o.traceQuery, baseOptions.traceQuery),
    retry: ifUndefined(o.retry, baseOptions.retry),
    customPayload: ifUndefined(o.customPayload, baseOptions.customPayload),
    captureStackTrace: ifUndefined(o.captureStackTrace, baseOptions.captureStackTrace)
  });
}

var elapsedDynamic = [];
var elapsedManual = [];

helper.syncTimes(10, function testMethods() {
  elapsedDynamic.push(testMethod(dynamicClone));
  elapsedManual.push(testMethod(manualClose));
});

console.log('dynamic: %sms', helper.avg(elapsedDynamic));
console.log('manual: %sms', helper.avg(elapsedManual));

function testMethod(method) {
  var start = process.hrtime();
  for (var j = 0; j < 100000; j++) {
    for (var i = 0; i < options.length; i++) {
      method(options[i]);
    }
  }
  return process.hrtime(start);
}

function extend(target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
}

function ifUndefined(v1, v2) {
  return (v1 === undefined ? v2 : v1)
}