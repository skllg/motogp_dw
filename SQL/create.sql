
CREATE TABLE IF NOT EXISTS  dim_constructors (
    id_constructor SERIAL PRIMARY KEY,
    des_constructor VARCHAR(50) NOT NULL,
    "year" integer NOT NULL
);

/*
select * from dim_constructors c 
*/

create table IF NOT EXISTS dim_teams (
	id_team SERIAL PRIMARY KEY,
	id_constructor_fk integer not null,
	des_team varchar(50) not null,
	bool_independent boolean,
	des_motorcycle varchar(15),
	tyre_supplier varchar(15),
	rounds varchar(15),
	year integer not null,
	constraint fk_id_constructor
		foreign key(id_constructor_fk)
		references dim_constructors(id_constructor)
		ON DELETE SET NULL
);
/*select * from dim_teams t */

create table IF NOT EXISTS dim_tracks (
	id_track serial primary key,
	des_track varchar(50) not null,
	loc_track varchar(50),
	country_track varchar(50),
	len_track integer,
	num_corners integer
);

/*
select * from dim_tracks 
*/


create table IF NOT EXISTS dim_grand_prix(
	id_grandprix serial primary key,
	id_track_fk integer not null,
	des_grandprix varchar(50) not null,
	"round" integer,
	num_laps integer,
	"year" integer not null,
	"date" varchar(50),
	constraint fk_id_track
		foreign key(id_track_fk)
		references dim_tracks(id_track)
		ON DELETE SET NULL
);

/*
select * from dim_grand_prix 
*/



create table IF NOT EXISTS dim_riders(
	id_rider serial primary key,
	id_team_fk integer not null,
	rider_name varchar(20) not null,
	rider_lastname varchar(20) not null,
	"year" integer not null,
	rider_number integer,
	"class" varchar(10) not null,
	constraint fk_id_team
		foreign key(id_team_fk)
		references dim_teams(id_team)
		ON DELETE SET NULL
	
);

drop table dim_constructors cascade;
drop table dim_teams cascade;
drop table dim_riders cascade;
drop table dim_grand_prix  cascade;
drop table dim_tracks cascade;

truncate table dim_constructors cascade;
truncate table dim_teams cascade;
truncate table dim_riders cascade;
truncate table dim_grand_prix  cascade;
truncate table dim_tracks cascade;

select * from dim_constructors;
select * from dim_teams ;
select * from dim_riders ;
select * from dim_grand_prix  ;
select * from dim_tracks ;


