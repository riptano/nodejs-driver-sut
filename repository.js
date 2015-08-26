var cassandra = require('cassandra-driver');
/**
 * @param {Client} client
 * @param {MetricsTracker} tracker
 * @constructor
 */
function Repository(client, tracker) {
  this.client = client;
  this.tracker = tracker;
}

Repository.prototype.insertCredentials = function (prepare, email, password, callback) {
  var id = cassandra.types.Uuid.random();
  password = password || email;
  var trackerKey = prepare ? 'prepared-statements-insert-user_credentials' : 'simple-statements-insert-user_credentials';
  var tracker = this.tracker;
  var startTime = process.hrtime();
  this.client.execute(
    'INSERT INTO user_credentials (email, password, userid) VALUES (?, ?, ?)',
    [ email, password || email, id ],
    { prepare: prepare },
    function (err) {
      if (err) {
        return callback(err);
      }
      tracker.update(trackerKey, process.hrtime(startTime), function () {
        callback(null, { email: email, password: password, userid: id});
      });
    });
};

Repository.prototype.getCredentials = function (prepare, email, callback) {
  var trackerKey = prepare ? 'prepared-statements-select-user_credentials' : 'simple-statements-select-user_credentials';
  var tracker = this.tracker;
  var startTime = process.hrtime();
  this.client.execute(
    'SELECT email, password, userid FROM user_credentials WHERE email = ?',
    [ email],
    { prepare: prepare },
    function (err, result) {
      if (err) {
        return callback(err);
      }
      tracker.update(trackerKey, process.hrtime(startTime), function () {
        callback(null, result.first());
      });
    });
};

module.exports = Repository;