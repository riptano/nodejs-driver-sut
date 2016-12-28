// Sample schema for Apache Cassandra

// IF EXISTS added for Cassandra 2.1
DROP KEYSPACE IF EXISTS killrvideo;
CREATE KEYSPACE killrvideo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use killrvideo;

// Comments as a many-to-many
// Looking from the video side to many users
CREATE TABLE comments_by_video (
   videoid uuid,
   commentid timeuuid,
   userid uuid,
   comment text,
   PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);
