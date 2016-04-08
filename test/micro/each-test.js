'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');


helper.asyncTest(testMethod, {
  'each': each,
  'async1_each': async1.each,
  'async2_each': async2.each
});

function testMethod(method, callback) {
  var a = 0;
  var start = process.hrtime();
  helper.timesSeries(200000, function (n, timesNext) {
    method(new Array(20), function (item, next) {
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

function each(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter is not an Array');
  }
  callback = callback || noop;
  var length = arr.length;
  var completed = 0;
  for (var i = 0; i < length; i++) {
    fn(i, next);
  }
  function next(err) {
    if (err) {
      var cb = callback;
      callback = noop;
      cb(err);
      return;
    }
    if (++completed !== length) {
      return;
    }
    callback();
  }
}