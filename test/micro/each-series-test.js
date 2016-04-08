'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');


helper.asyncTest(testMethod, {
  'eachSeries': eachSeries,
  'async1_eachSeries': async1.eachSeries,
  'async2_eachSeries': async2.eachSeries
});

function testMethod(method, callback) {
  var a = 0;
  var start = process.hrtime();
  helper.timesSeries(1000000, function (n, timesNext) {
    method(new Array(5), function (item, next) {
      a++;
      process.nextTick(next);
    }, timesNext);
  }, function (err) {
    var diff = process.hrtime(start);
    if (err) throw err;
    assert.ok(a > 0);
    callback(diff);
  });
}

function noop() {}

function eachSeries(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter is not an Array');
  }
  callback = callback || noop;
  var length = arr.length;
  if (length === 0) {
    return callback();
  }
  var sync;
  var index = 1;
  fn(arr[0], next);
  if (sync === undefined) {
    sync = false;
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index >= length) {
      return callback();
    }
    if (sync === undefined) {
      sync = true;
    }
    if (sync) {
      return process.nextTick(function () {
        fn(arr[index++], next);
      });
    }
    fn(arr[index++], next);
  }
}