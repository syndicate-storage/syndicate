-- Create the Syndicate database tables

SET client_encoding = 'UNICODE';

DROP VIEW user_contents CASCADE;
DROP VIEW user_mdservers CASCADE;
DROP VIEW mdserver_users CASCADE;
DROP VIEW view_mdserver CASCADE;
DROP VIEW view_user CASCADE;
DROP VIEW user_mdservers CASCADE;
DROP INDEX user_mdserver_user_id_idx CASCADE;
DROP INDEX user_content_user_id_idx CASCADE;
DROP INDEX mdserver_user_server_id_idx CASCADE;
DROP TABLE user_mdserver CASCADE;
DROP TABLE user_content CASCADE;
DROP TABLE mdserver_user CASCADE;
DROP TABLE mdservers CASCADE;
DROP TABLE users CASCADE;
DROP TABLE contents CASCADE;
DROP SEQUENCE users_user_id_seq;
DROP SEQUENCE mdservers_server_id_seq;
DROP SEQUENCE contents_content_id_seq;
DROP AGGREGATE array_accum(anyelement);

CREATE AGGREGATE array_accum (
   sfunc = array_append,
   basetype = anyelement,
   stype = anyarray,
   initcond = '{}'
);

CREATE SEQUENCE users_user_id_seq INCREMENT BY 1 NO MAXVALUE START 12345 NO CYCLE;
CREATE SEQUENCE mdservers_server_id_seq INCREMENT BY 1 NO MAXVALUE START 1 NO CYCLE;
CREATE SEQUENCE contents_content_id_seq INCREMENT BY 1 NO MAXVALUE START 1 NO CYCLE;

CREATE TABLE users (
   user_id integer DEFAULT nextval('users_user_id_seq') UNIQUE NOT NULL PRIMARY KEY,
   username text UNIQUE NOT NULL,
   password text NOT NULL,
   email text NOT NULL,
   enabled boolean NOT NULL,
   roles text[] NOT NULL,                 -- what roles can this user fulfull?
   max_mdservers integer NOT NULL,     -- maximum allowed number of metadata servers
   max_contents integer NOT NULL      -- maximum allowed number of content servers to register
   --mdserver_ids integer[] NOT NULL,       -- which metadata servers this user owns, if any
   --reg_mdserver_ids integer[] NOT NULL,   -- which metadata server to which this user is subscribed
   --content_ids integer[] NOT NULL        -- which content URLs this user controls
);


CREATE TABLE mdservers (
   server_id integer DEFAULT nextval('mdservers_server_id_seq') UNIQUE NOT NULL PRIMARY KEY,
   host text NOT NULL,
   name text UNIQUE NOT NULL,
   status text NOT NULL,
   portnum integer NOT NULL,
   auth_read boolean NOT NULL,
   auth_write boolean NOT NULL,
   owner integer REFERENCES users NOT NULL    -- the UID of the user that owns this server
   --users integer[] NOT NULL     -- at least the owner is a user
);

CREATE TABLE contents (
   content_id integer DEFAULT nextval('contents_content_id_seq') UNIQUE NOT NULL PRIMARY KEY,
   host_url text NOT NULL,
   owner integer REFERENCES users NOT NULL        -- the UID of the user that registered this content server
);

-- bind users to their mdservers
CREATE TABLE user_mdserver (
   user_id integer REFERENCES users NOT NULL,
   server_id integer REFERENCES mdservers NOT NULL
) WITH OIDS;
CREATE INDEX user_mdserver_user_id_idx ON user_mdserver (user_id);

CREATE OR REPLACE VIEW user_mdservers AS
SELECT user_id, 
array_accum(server_id) AS server_ids
FROM user_mdserver
GROUP BY user_id;

-- bind users to their contents
CREATE TABLE user_content (
   user_id integer REFERENCES users NOT NULL,
   content_id integer REFERENCES contents NOT NULL
) WITH OIDS;
CREATE INDEX user_content_user_id_idx ON user_content (user_id);

CREATE OR REPLACE VIEW user_contents AS
SELECT user_id,
array_accum(content_id) AS content_ids
FROM user_content
GROUP BY user_id;


-- bind mdservers to their registered users
CREATE TABLE mdserver_user (
   server_id integer REFERENCES mdservers NOT NULL,
   user_id integer REFERENCES users NOT NULL
) WITH OIDS;
CREATE INDEX mdserver_user_server_id_idx ON mdserver_user (server_id);

CREATE OR REPLACE VIEW mdserver_users AS
SELECT server_id,
array_accum(user_id) AS user_ids
FROM mdserver_user
GROUP BY server_id;

-- unify a person's information
CREATE OR REPLACE VIEW view_users AS
SELECT
users.user_id,
users.username,
users.password,
users.email,
users.enabled,
users.roles,
users.max_mdservers,
users.max_contents,
COALESCE( (SELECT server_ids FROM user_mdservers WHERE user_mdservers.user_id = users.user_id), '{}') AS my_mdserver_ids,
COALESCE( (SELECT content_ids FROM user_contents WHERE user_contents.user_id = users.user_id), '{}') AS content_ids,
COALESCE( (SELECT array_accum(server_id) AS server_ids FROM mdserver_user WHERE mdserver_user.user_id = users.user_id GROUP BY user_id), '{}') AS sub_mdserver_ids
FROM users;


-- unify a mdserver's information
CREATE OR REPLACE VIEW view_mdservers AS
SELECT
mdservers.server_id,
mdservers.name,
mdservers.host,
mdservers.portnum,
mdservers.status,
mdservers.auth_read,
mdservers.auth_write,
mdservers.owner,
COALESCE( (SELECT user_ids FROM mdserver_users WHERE mdserver_users.server_id = mdservers.server_id), '{}') AS user_ids
FROM mdservers;




INSERT INTO users (username,password,email,enabled,max_mdservers,max_contents,roles) VALUES ('maint', '8469a90996f9ad339431635af53733516ff8a538', 'judecn@gmail.com', TRUE, 30, 15, '{"admin"}');
INSERT INTO users (username,password,email,enabled,max_mdservers,max_contents,roles) VALUES ('jude', '438eb25faee44f676c8084a812344c5964f6131d', 'jcnelson@cs.princeton.edu', TRUE, 30, 15, '{"admin","user"}');

--INSERT INTO users (username,password,email,enabled,max_mdservers,max_contents,roles,mdserver_ids,reg_mdserver_ids,content_ids) VALUES ('maint', '8469a90996f9ad339431635af53733516ff8a538', 'judecn@gmail.com', True, 30, 15, '{"admin"}','{}','{}','{}' );
--INSERT INTO users (username,password,email,enabled,max_mdservers,max_contents,roles,mdserver_ids,reg_mdserver_ids,content_ids) VALUES ('jude', '438eb25faee44f676c8084a812344c5964f6131d', 'jcnelson@cs.princeton.edu', True, 30, 15, '{"admin", "user"}','{}','{}','{}' );
