'use strict';
var helper = {
  asyncTest: function asyncTest(benchmark, methods, callback) {
    var names = Object.keys(methods);
    var index = 0;
    var results = {};
    var repeatIndex = 0;
    whilst(function () {
      if (repeatIndex > 2) {
        repeatIndex = 0;
        index++;
      }
      else {
        repeatIndex++;
      }
      return index < names.length;
    }, function (next) {
      var name = names[index];
      if (!methods[name]) {
        return next();
      }
      benchmark(methods[name], function (diff) {
        var itemResults = results[name] = results[name] || [];
        itemResults.push(diff);
        next();
      });
    }, function () {
      if (!callback) {
        console.log('-------results-------');
        return names.forEach(function (name) {
          return console.log('%s\t%s', avg(results[name]), name);
        });
      }
      callback(results);
    });
  },
  timesSeries: timesSeries,
  syncTimes: function (n, handler) {
    for (var i = 0; i < n; i++) {
      handler(i);
    }
  },
  avg: avg
};

function whilst(condition, fn, callback) {
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (!condition()) {
      return callback();
    }
    fn(next);
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

function avg(results) {
  return (results
    .map(function (diff) {
      return diff[0] * 1000 + ~~(diff[1] / 1000000);
    })
    .reduce(function (prev, curr) {
      return prev + curr
    }, 0) / results.length).toFixed();
}

function median() {
  return 0;
}

module.exports = helper;