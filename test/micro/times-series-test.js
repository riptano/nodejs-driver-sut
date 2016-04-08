'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');

helper.asyncTest(testMethod, {
  'timesSeries': timesSeries,
  'async1_timesSeries': async1.timesSeries,
  'async2_timesSeries': async2.timesSeries
});

function testMethod(method, callback) {
  var start = process.hrtime();
  var a = 1;
  method(1000000, function (n, next) {
    a = a + 1 / (n + 1);
    setImmediate(next);
  }, function (err) {
    var diff = process.hrtime(start);
    if (err) throw err;
    assert.ok(a > 0);
    callback(diff);
  });
}

function timesSeries(count, iteratorFunction, callback) {
  count = +count;
  if (isNaN(count) || count < 1) {
    return callback();
  }
  var index = 0;
  var sync = 0;
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index === count) {
      return callback();
    }
    if (sync === 0) {
      sync = 1;
      iteratorFunction(index++, function (err) {
        if (sync === 1) {
          //sync function
          sync = 4;
        }
        next(err);
      });
      if (sync === 1) {
        //async function
        sync = 2;
      }
      return;
    }
    if (sync === 4) {
      //Prevent "Maximum call stack size exceeded"
      return process.nextTick(function () {
        iteratorFunction(index++, next);
      });
    }
    //do a sync call as the callback is going to call on a future tick
    iteratorFunction(index++, next);
  }
}