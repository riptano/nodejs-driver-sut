var express = require('express');
var cassandra = require('cassandra-driver');
var async = require('async');
var MetricsTracker = require('./metrics-tracker');
var Repository = require('./repository');
var Uuid = cassandra.types.Uuid;

var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({ contactPoints: [ contactPoint ], keyspace: 'killrvideo'});
var tracker = new MetricsTracker(process.argv[2] || '127.0.0.1', 2003);
var repository = new Repository(client, tracker);
var app = express();

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
  repository.insertCredentials(true, req.params.email || Uuid.random().toString(), req.params.password, function (err, result) {
    if (err) return next(err);
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
    console.log('App listening at http://%s:%s', 'localhost', server.address().port);
  });
});