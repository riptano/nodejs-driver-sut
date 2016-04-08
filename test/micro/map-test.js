'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');

helper.asyncTest(testMethod, {
  'map': map,
  'async1_map': async1.map,
  'async2_map': async2.map
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


function map(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  var length = arr.length;
  if (length === 0) {
    return callback(null, []);
  }
  var result = new Array(length);
  var completed = 0;
  for (var i = 0; i < length; i++) {
    invoke(i);
  }

  function invoke(i) {
    fn(arr[i], function mapItemCallback(err, transformed) {
      result[i] = transformed;
      next(err);
    });
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
    callback(null, result);
  }
}