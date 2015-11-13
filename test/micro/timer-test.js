'use strict';

testMethod(testConstantTimeout);
testMethod(testVariableTimeout);

function testMethod(method) {
  var start = process.hrtime();
  method();
  var diff = process.hrtime(start);
  console.log(method.name, diff);
}

function testConstantTimeout() {
  var arr = new Array(100000);
  var i;
  for (i = 0; i < arr.length; i++) {
    arr[i] = setTimeout(function noop() {}, 100);
  }
  for (i = 0; i < arr.length; i++) {
    clearTimeout(arr[i]);
  }
}

function testVariableTimeout() {
  var arr = new Array(100000);
  var i;
  for (i = 0; i < arr.length; i++) {
    arr[i] = setTimeout(function noop() {}, 100 + i * 2);
  }
  for (i = 0; i < arr.length; i++) {
    clearTimeout(arr[i]);
  }
}