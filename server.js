var express = require('express');
var cassandra = require('cassandra-driver');
var async = require('async');
var contactPoint = process.argv[2] || '127.0.0.1';
var client = new cassandra.Client({ contactPoints: [ contactPoint ]});

var app = express();
app.get('/', function (req, res) {
  res.send('Hello World!');
});
app.get('/cassandra', function (req, res) {
  client.execute('SELECT NOW() from system.local', function (err, result) {
    res.send('Now: ' + result.rows[0]['NOW()'].toString());
  });
});
app.post('/simple-statements/users/:id(\\d+)/:length(\\d+)?', function (req, res, next) {
  insertUser(false, req, res, next);
});
app.post('/prepared-statements/users/:id(\\d+)/:length(\\d+)?', function (req, res, next) {
  insertUser(true, req, res, next);
});
app.get('/simple-statements/users/:id(\\d+)/:length(\\d+)?', function (req, res, next) {
  selectUsers(false, req, res, next);
});
app.get('/prepared-statements/users/:id(\\d+)/:length(\\d+)?', function (req, res, next) {
  selectUsers(true, req, res, next);
});

function selectUsers (prepare, req, res, next) {
  var start = parseInt(req.params.id, 10);
  var length = parseInt(req.params.length, 10) || 1;
  var query = 'SELECT username FROM videodb.users WHERE username = ?';
  var userArray = [];
  async.times(length, function (n, timesNext) {
    var id = (start + n).toString();
    client.execute(query, ['user-' + id], { prepare: prepare}, function (err, result) {
      if (err) return timesNext(err);
      if (result.rows.length === 0) return timesNext();
      userArray.push(result.rows[0].username);
      timesNext();
    });
  }, function (err) {
    if (err) return next(err);
    res.send(userArray.join(','));
    next();
  });
}

function insertUser (prepare, req, res, next) {
  var start = parseInt(req.params.id, 10);
  var length = parseInt(req.params.length, 10) || 1;
  var query = 'INSERT INTO videodb.users (username, firstname, lastname, password, email, created_date) VALUES (?, ?, ?, ?, ?, ?)';
  async.times(length, function (n, timesNext) {
    var id = (start + n).toString();
    var params = ['user-' + id, 'first-' + id, 'last-' + id, 'pass', [id + '@datastax.com'], new Date()];
    client.execute(query, params, { prepare: prepare}, timesNext);
  }, function (err) {
    if (err) return next(err);
    res.send('OK');
    next();
  });
}

client.connect(function (err) {
  if (err) {
    console.log('Cassandra driver was not able to connect to %s: %s', contactPoint, err);
    return;
  }
  var server = app.listen(8080, function () {
    console.log('App listening at http://%s:%s', 'localhost', server.address().port);
  });
});