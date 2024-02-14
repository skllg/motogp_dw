
DROP TABLE IF EXISTS dim_constructors CASCADE;
CREATE TABLE IF NOT EXISTS  dim_constructors (
    id_constructor SERIAL PRIMARY KEY,
    des_constructor VARCHAR(50) NOT NULL,
    racing_class varchar(10),
    season integer NOT NULL
);

/*------------------------------------------------------------------------*/

DROP TABLE IF EXISTS dim_teams CASCADE;

create table IF NOT EXISTS dim_teams (
	id_team SERIAL PRIMARY KEY,
	id_constructor_fk integer not null,
	des_team varchar(50) not null,
	bool_independent boolean,
	des_motorcycle varchar(30),
	type_motorcycle varchar(15),
	tyre_supplier varchar(30),
	rounds_participated varchar(30),
	racing_class varchar(10),
	season integer not null,
	constraint fk_id_constructor
		foreign key(id_constructor_fk)
		references dim_constructors(id_constructor)
		ON DELETE SET NULL
);

/*------------------------------------------------------------------------*/

DROP TABLE IF EXISTS dim_tracks CASCADE;

CREATE TABLE if not exists public.dim_tracks (
	id_track serial4 NOT NULL,
	des_track varchar(50) NOT NULL,
	loc_track varchar(50) NULL,
	country_track varchar(50) NULL,
	len_track int4 NULL,
	num_corners int4 NULL,
	longitude float8,
	latitude float8
);

/*------------------------------------------------------------------------*/

DROP TABLE IF EXISTS dim_grand_prix CASCADE;

create table IF NOT EXISTS dim_grand_prix(
	id_grandprix serial primary key,
	id_track_fk integer not null,
	des_grandprix varchar(70) not null,
	num_round integer,
	num_laps integer,
	season integer not null,
	gp_date date,
	saturday_race boolean,
	night_race boolean,
	constraint fk_id_track
		foreign key(id_track_fk)
		references dim_tracks(id_track)
		ON DELETE SET NULL
);

/*------------------------------------------------------------------------*/


DROP TABLE IF EXISTS dim_riders CASCADE;

CREATE table IF NOT EXISTS  public.dim_riders (
	id_rider serial4 NOT NULL,
	id_team_fk int4 NOT NULL,
	rider_full_name varchar(50) NOT NULL,
	rider_name varchar(20) NOT NULL,
	rider_lastname varchar(20) NOT NULL,
	season int4 NOT NULL,
	rider_number int4 NULL,
	racing_class varchar(10) NOT NULL,
	rounds_participated varchar(30) NULL,
	loc_birth varchar NULL,
	date_birth date NULL
);



/*------------------------------------------------------------------------*/

DROP TABLE IF EXISTS fact_results CASCADE;

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

/*------------------------------------------------------------------------*/



