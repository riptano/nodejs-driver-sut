var cassandra = require('cassandra-driver');
var Uuid = cassandra.types.Uuid;
var TimeUuid = cassandra.types.TimeUuid;
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
  var id = Uuid.random();
  password = password || email;
  this.execute(
    prepare,
    prepare ? 'prepared-statements-insert-user_credentials' : 'simple-statements-insert-user_credentials',
    'INSERT INTO user_credentials (email, password, userid) VALUES (?, ?, ?)',
    [ email, password, id ],
    function (err) {
      if (err) {
        return callback(err);
      }
      callback(null, { email: email, password: password, userid: id});
    });
};

Repository.prototype.getCredentials = function (prepare, email, callback) {
  this.execute(
    prepare,
    prepare ? 'prepared-statements-select-user_credentials' : 'simple-statements-select-user_credentials',
    'SELECT email, password, userid FROM user_credentials WHERE email = ?',
    [ email ],
    function (err, result) {
      if (err) {
        return callback(err);
      }
      callback(null, result.first());
    });
};

Repository.prototype.insertVideoEvent = function (prepare, videoEvent, callback) {
  this.execute(
    prepare,
    prepare ? 'prepared-statements-insert-video_event' : 'simple-statements-insert-video_event',
    'INSERT INTO video_event (videoid, userid, event, event_timestamp, video_timestamp) VALUES (?, ?, ?, ?, ?)',
    [
      Uuid.fromString(videoEvent.videoid),
      Uuid.fromString(videoEvent.userid),
      videoEvent.event,
      TimeUuid.fromString(videoEvent.eventTimestamp),
      cassandra.types.Long.fromString(videoEvent.videoTimestamp)
    ],
    function (err) {
      if (err) {
        return callback(err);
      }
      callback(null, videoEvent);
    });
};

Repository.prototype.getVideoEvent = function (prepare, videoid, userid, callback) {
  this.execute(
    prepare,
    prepare ? 'prepared-statements-select-video_event' : 'simple-statements-select-video_event',
    'SELECT videoid, userid, event, event_timestamp, video_timestamp FROM video_event WHERE videoid = ? and userid = ?',
    [ videoid, userid ],
    function (err, result) {
      if (err) {
        return callback(err);
      }
      callback(null, result.first());
    });
};

Repository.prototype.execute = function (prepare, trackerKey, query, params, callback) {
  var tracker = this.tracker;
  var startTime = process.hrtime();
  this.client.execute(
    query,
    params,
    { prepare: prepare },
    function (err, result) {
      if (err) {
        return callback(err);
      }
      tracker.update(trackerKey, process.hrtime(startTime), function () {
        callback(null, result);
      });
    });
};

module.exports = Repository;