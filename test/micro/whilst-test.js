'use strict';
var assert  =require('assert');
var async1 = require('async');
var utils = require('../../src/utils');
var helper = require('./helper');
var async2 = utils.requireOptional('neo-async');

helper.asyncTest(testMethod, {
  'whilst': whilst,
  //'async1_whilst': async1.whilst,
  'async2_whilst': async2.whilst
});

function testMethod(method, callback) {
  var a = 0;
  var start = process.hrtime();
  method(function condition() {
    return a < 5000000;
  }, function (next) {
    a++;
    next();
    //setImmediate(next);
  }, function (err) {
    var diff = process.hrtime(start);
    if (err) throw err;
    assert.ok(a > 0);
    callback(diff);
  });
}

function whilst(condition, fn, callback) {
  var sync = 0;
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (!condition()) {
      return callback();
    }
    if (sync === 0) {
      sync = 1;
      fn(function (err) {
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
        fn(next);
      });
    }
    //do a sync call as the callback is going to call on a future tick
    fn(next);
  }
}