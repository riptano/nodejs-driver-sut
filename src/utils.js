'use strict';
var fs = require('fs');

exports.times = function times (n, f){
  var arr = new Array(n);
  for (var i = 0; i < n; i++) {
    arr[i] = f();
  }
  return arr;
};

exports.mean = function mean(arr) {
  return (arr.reduce(function (p, v) { return p + v; }, 0) / arr.length);
};

exports.median = function median(arr) {
  arr = arr.slice(0).sort();
  var num = arr.length;
  if (num % 2 !== 0) {
    return arr[(num - 1) / 2];
  }
  // even: return the average of the two middle values
  var left = arr[num / 2 - 1];
  var right = arr[num / 2];

  return (left + right) / 2;
};

exports.parseOptions = function parseOptions(optionNames, defaults) {
  var options = {};
  for (var i = 0; i < process.argv.length; i = i + 2) {
    var optionId = process.argv[i];
    if (!optionId || optionId.indexOf('-') !== 0) {
      continue;
    }
    optionId = optionId.substr(1);
    var name = optionNames[optionId] || optionId;
    options[name] = process.argv[i + 1];
  }
  Object.keys(defaults).forEach(function (name) {
    options[name] = options[name] || defaults[name];
  });
  return options;
};

exports.requireOptional = function (moduleName) {
  var result;
  try{
    result = require(moduleName);
  }
  catch (e) {
    console.error('Module %s not found', moduleName);
  }
  return result;
};

/**
 * @param options
 */
exports.outputTestHeader = function outputTestHeader(options) {
  console.log('-----------------------------------------------------');
  console.log('Using:');
  var driverVersion = JSON.parse(fs.readFileSync('node_modules/cassandra-driver/package.json', 'utf8')).version;
  console.log('- Driver v%s', driverVersion);
  console.log('- Connections per hosts: %d', options.connectionsPerHost);
  console.log('- Max outstanding requests: %d', options.outstanding);
  console.log('- Operations: %d', options.ops);
  console.log('-----------------------------------------------------');
};
