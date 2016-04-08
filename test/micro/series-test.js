'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');

helper.asyncTest(testMethod, {
  'baseline': function dummy(arr, cb) {
    process.nextTick(cb);
  },
  'series': series,
  'async1_series': async1.series,
  'async2_series': async2.series
});

function testMethod(seriesMethod, callback) {
  var a;
  var funcArray = [
    function stepOne(seriesNext) {
      a++;
      setImmediate(seriesNext);
    },
    function stepTwo(seriesNext) {
      a = a * 4;
      setImmediate(seriesNext);
    },
    function stepThree(seriesNext) {
      a = a + 20;
      assert.strictEqual(a, 24);
      setImmediate(seriesNext);
    }
  ];
  var start = process.hrtime();
  timesSeries(100000, function (n, next) {
    a = 0;
    seriesMethod(funcArray, next);
  }, function (err) {
    var diff = process.hrtime(start);
    if (err) throw err;
    callback(diff)
  });
}

function series(arr, callback) {
  var index = 0;
  var sync;
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index === arr.length) {
      return callback();
    }
    if (sync) {
      return process.nextTick(function () {
        sync = true;
        arr[index++](next);
        sync = false;
      });
    }
    sync = true;
    arr[index++](next);
    sync = false;
  }
}

function timesSeries(count, iteratorFunction, callback) {
  if (!count) {
    return callback();
  }
  var index = 0;
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index === count) {
      return callback();
    }
    iteratorFunction(index++, next);
  }
}