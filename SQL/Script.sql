
CREATE TABLE IF NOT EXISTS  dim_constructors (
    id_constructor SERIAL PRIMARY KEY,
    des_constructor VARCHAR(50) NOT NULL,
    racing_class varchar(10),
    season integer NOT NULL
);

/*
select * from dim_constructors c 
*/

create table IF NOT EXISTS dim_teams (
	id_team SERIAL PRIMARY KEY,
	id_constructor_fk integer not null,
	des_team varchar(50) not null,
	bool_independent boolean,
	des_motorcycle varchar(30),
	type_motorcycle varchar(15),
	tyre_supplier varchar(15),
	rounds_participated varchar(15),
	racing_class varchar(10),
	season integer not null,
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
	des_grandprix varchar(70) not null,
	num_round integer,
	num_laps integer,
	season integer not null,
	gp_date date,
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
	rider_full_name varchar(50) not null,
	rider_name varchar(20) not null,
	rider_lastname varchar(20) not null,
	season integer not null,
	rider_number integer,
	racing_class varchar(10) not null,
	rounds_participated varchar(30),
	constraint fk_id_team
		foreign key(id_team_fk)
		references dim_teams(id_team)
		ON DELETE SET NULL
	
);

create table IF NOT EXISTS fact_results(
	id_result serial primary key,
	id_rider_fk integer not null,
	id_grand_prix_fk integer not null,
	id_position_fk integer not null,
	num_round integer,
	season integer,
	racing_class varchar(10),
	race_type varchar(10),
	pole bool,
	fastest_lap bool,
	constraint fk_id_rider
		foreign key(id_rider_fk)
		references dim_riders(id_rider)
		ON DELETE SET null,
	constraint fk_id_grand_prix
		foreign key(id_grand_prix_fk)
		references dim_grand_prix(id_grandprix)
		ON DELETE SET null,
	constraint fk_id_position
		foreign key(id_position_fk)
		references dim_positions(id_position)
		ON DELETE SET NULL
);



      
drop table dim_constructors cascade;
drop table dim_teams cascade;
drop table dim_riders cascade;
drop table dim_grand_prix  cascade;
drop table dim_tracks cascade;
drop table fact_results cascade;
--drop table dim_positions cascade;


truncate table dim_constructors cascade;
truncate table dim_teams cascade;
truncate table dim_riders cascade;
truncate table dim_grand_prix  cascade;
truncate table dim_tracks cascade;
truncate table fact_results cascade;
--truncate table dim_positions cascade;


select * from dim_constructors;
select * from dim_teams ;
select * from dim_riders where rider_full_name like '%barb%';
select * from dim_grand_prix  ;
select * from dim_tracks ;
select * from fact_results; 
select * from dim_positions  ;



