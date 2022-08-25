DROP TABLE IF EXISTS public.artists;
CREATE TABLE IF NOT EXISTS public.artists (
	artistid varchar(MAX) NOT NULL,
	name varchar(MAX),
	location varchar(MAX),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);
DROP TABLE IF EXISTS public.songplays;
CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(MAX),
	songid varchar(MAX),
	artistid varchar(MAX),
	sessionid int4,
	location varchar(MAX),
	user_agent varchar(MAX),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

DROP TABLE IF EXISTS public.songs;
CREATE TABLE IF NOT EXISTS public.songs (
	songid varchar(MAX) NOT NULL,
	title varchar(MAX),
	artistid varchar(MAX),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

DROP TABLE IF EXISTS public.staging_events;
CREATE TABLE IF NOT EXISTS public.staging_events (
	artist varchar(MAX),
	auth varchar(MAX),
	firstname varchar(MAX),
	gender varchar(MAX),
	iteminsession int4,
	lastname varchar(MAX),
	length numeric(18,0),
	"level" varchar(MAX),
	location varchar(MAX),
	"method" varchar(MAX),
	page varchar(MAX),
	registration numeric(18,0),
	sessionid int4,
	song varchar(MAX),
	status int4,
	ts int8,
	useragent varchar(MAX),
	userid int4
);

DROP TABLE IF EXISTS public.staging_songs;
CREATE TABLE IF NOT EXISTS public.staging_songs (
	num_songs int4,
	artist_id varchar(MAX),
	artist_name varchar(MAX),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(MAX),
	song_id varchar(MAX),
	title varchar(MAX),
	duration numeric(18,0),
	"year" int4
);

DROP TABLE IF EXISTS public.time;
CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

DROP TABLE IF EXISTS public.users;
CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(MAX),
	last_name varchar(MAX),
	gender varchar(MAX),
	"level" varchar(MAX),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);





