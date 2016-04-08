'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');

helper.asyncTest(testMethod, {
  'mapSeries': mapSeries,
  'async1_mapSeries': async1.mapSeries,
  'async2_mapSeries': async2.mapSeries
});

function testMethod(method, callback) {
  var a = 0;
  var start = process.hrtime();
  helper.timesSeries(200000, function (n, timesNext) {
    method(new Array(20), function (item, next) {
      var x = a++;
      process.nextTick(function () {
        next(null, x);
      });
    }, timesNext);
  }, function (err) {
    var diff = process.hrtime(start);
    if (err) throw err;
    assert.ok(a > 0);
    callback(diff);
  });
}

function noop() {}

function mapSeries(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  var length = arr.length;
  if (length === 0) {
    return callback(null, []);
  }
  var result = new Array(length);
  var index = 0;
  var sync;
  invoke(0);
  if (sync === undefined) {
    sync = false;
  }

  function invoke(i) {
    fn(arr[i], function mapItemCallback(err, transformed) {
      result[i] = transformed;
      next(err);
    });
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (++index === length) {
      return callback(null, result);
    }
    if (sync === undefined) {
      sync = true;
    }
    if (sync) {
      var i = index;
      process.nextTick(function () {
        invoke(i);
      });
    }
    invoke(index);
  }
}