CREATE TABLE constructors (
    id_constructor SERIAL PRIMARY KEY,
    des_constructor VARCHAR(50) NOT NULL,
    "year" integer NOT NULL
);

create table teams (
	id_team SERIAL PRIMARY KEY,
	id_constructor integer not null,
	des_team varchar(50) not null,
	bool_independent boolean,
	des_motorcycle varchar(15),
	tyre_supplier varchar(15),
	rounds varchar(15),
	"year" integer not null
);

create table tracks (
	id_track serial primary key,
	des_track varchar(50) not null,
	loc_track varchar(50),
	country_track varchar(50),
	len_track integer,
	num_corners integer
);

create table grand_prix(
	id_grandprix serial primary key,
	id_track integer not null,
	des_grandprix varchar(50) not null,
	"round" integer,
	num_laps integer,
	"year" integer not null,
	"date" varchar(50)
);

create table riders(
	id_rider serial primary key,
	id_team integer not null,
	rider_name varchar(20) not null,
	rider_lastname varchar(20) not null,
	"year" integer not null,
	rider_number integer,
	"class" varchar(10) not null
	
);
