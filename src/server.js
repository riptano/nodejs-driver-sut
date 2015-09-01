var fs = require('fs');
var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require('async');
var util = require('util');
var MetricsTracker = require('./metrics-tracker');
var Repository = require('./repository');
var Uuid = cassandra.types.Uuid;

//settings
var contactPoint = process.argv[2] || '127.0.0.1';
var metricsHost = process.argv[3] || '127.0.0.1';
var executionTimes = parseInt(process.argv[4], 10) || 1;
var executionLimit = parseInt(process.argv[5], 10) || 50;

var driverVersion = JSON.parse(fs.readFileSync('node_modules/cassandra-driver/package.json', 'utf8')).version;
var client = new cassandra.Client({ contactPoints: [ contactPoint ], keyspace: 'killrvideo'});
var tracker = new MetricsTracker(metricsHost, 2003, driverVersion);
var repository = new Repository(client, tracker, executionTimes, executionLimit);
var app = express();
app.use(bodyParser.urlencoded({ extended: true }));

app.get('/', function (req, res) {
  res.send('Hello World!');
});
app.get('/cassandra', function (req, res, next) {
  client.execute('SELECT key from system.local', function (err, result) {
    if (err) return next(err);
    res.send(result.rows[0]['key'].toString());
  });
});
app.post('/prepared-statements/credentials', function (req, res, next) {
  repository.insertCredentials(true, req.body.email || Uuid.random().toString(), null, function (err, result) {
    if (err) return next(err);
    res.json(result);
    next();
  });
});
app.get('/prepared-statements/credentials/:email([\\w\\.@_-]+)', function (req, res, next) {
  repository.getCredentials(true, req.params.email, function (err, result) {
    if (err) return next(err);
    if (!result) return res.status(404).send('Not found');
    res.json(result);
    next();
  });
});
app.post('/prepared-statements/video-events', function (req, res, next) {
  repository.insertVideoEvent(true, req.body, function (err, result) {
    if (err) return next(err);
    res.json(result);
    next();
  });
});
app.get('/prepared-statements/video-events/:videoid([\\w-]+)/:userid([\\w-]+)', function (req, res, next) {
  repository.getVideoEvent(true, req.params.videoid, req.params.userid, function (err, result) {
    if (err) return next(err);
    if (!result) return res.status(404).send('Not found');
    res.json(result);
    next();
  });
});

async.series([
  client.connect.bind(client),
  tracker.connect.bind(tracker)
], function (err) {
  if (err) {
    console.error('Initialization error', err);
    return;
  }
  var server = app.listen(8080, function () {
    console.log();
    console.log('Using: driver version: %s; contactPoint: %s; metricsHost: %s;\nexecutionTimes: %s; executionLimit: %s',
      driverVersion, contactPoint, metricsHost, executionTimes, executionLimit);
    console.log('App listening at http://%s:%s', 'localhost', server.address().port);
  });
});
process.on('SIGINT', function() {
  console.log("\nShutting down");
  async.parallel([
    client.shutdown.bind(client),
    tracker.shutdown.bind(tracker)
  ], function () {
    process.exit();
  });
});