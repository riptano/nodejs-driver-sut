'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');


helper.asyncTest(testMethod, {
  'timesLimit': timesLimit,
  'async1_timesLimit': async1.timesLimit,
  'async2_timesLimit': async2.timesLimit
});

function testMethod(method, callback) {
  var start = process.hrtime();
  var a = 1;
  method(1000000, 100, function (n, next) {
    a = a + 1 / (n + 1);
    process.nextTick(next);
  }, function (err) {
    var diff = process.hrtime(start);
    if (err) throw err;
    assert.ok(a > 0);
    callback(diff);
  });
}

function noop() {}

function timesLimit(count, limit, iteratorFunc, callback) {
  callback = callback || noop;
  var index = limit - 1;
  var i;
  for (i = 0; i < limit; i++) {
    iteratorFunc(i, next);
  }
  i = -1;
  var completed = 0;
  var sync = undefined;
  function next(err) {
    if (err) {
      var cb = callback;
      callback = noop;
      cb(err);
      return;
    }
    if (++completed === count){
      return callback();
    }
    index++;
    if (index >= count) {
      return;
    }
    if (sync === undefined) {
      sync = (i >= 0);
    }
    if (sync) {
      var captureIndex = index;
      return process.nextTick(function () {
        iteratorFunc(captureIndex, next);
      });
    }
    iteratorFunc(index, next);
  }
}