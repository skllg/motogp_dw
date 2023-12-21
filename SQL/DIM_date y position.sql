-- public.dim_date definition

-- Drop table

-- DROP TABLE public.dim_date;

CREATE TABLE public.dim_date (
	id_date int4 NOT NULL,
	date_actual date NOT NULL,
	epoch int8 NOT NULL,
	day_suffix varchar(4) NOT NULL,
	day_name varchar(15) NOT NULL,
	day_of_week int4 NOT NULL,
	day_of_month int4 NOT NULL,
	day_of_quarter int4 NOT NULL,
	day_of_year int4 NOT NULL,
	week_of_month int4 NOT NULL,
	week_of_year int4 NOT NULL,
	week_of_year_iso bpchar(10) NOT NULL,
	month_actual int4 NOT NULL,
	month_name varchar(15) NOT NULL,
	month_name_abbreviated bpchar(3) NOT NULL,
	quarter_actual int4 NOT NULL,
	quarter_name varchar(9) NOT NULL,
	year_actual int4 NOT NULL,
	first_day_of_week date NOT NULL,
	last_day_of_week date NOT NULL,
	first_day_of_month date NOT NULL,
	last_day_of_month date NOT NULL,
	first_day_of_quarter date NOT NULL,
	last_day_of_quarter date NOT NULL,
	first_day_of_year date NOT NULL,
	last_day_of_year date NOT NULL,
	mmyyyy bpchar(6) NOT NULL,
	mmddyyyy bpchar(10) NOT NULL,
	weekend_indr bool NOT NULL,
	CONSTRAINT dim_date_id_date_pk PRIMARY KEY (id_date)
);
CREATE INDEX dim_date_date_actual_idx ON public.dim_date USING btree (date_actual);



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

create table IF NOT EXISTS dim_tracks (
	id_track serial primary key,
	des_track varchar(50) not null,
	loc_track varchar(50),
	country_track varchar(50),
	len_track integer,
	num_corners integer
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
	constraint fk_id_track
		foreign key(id_track_fk)
		references dim_tracks(id_track)
		ON DELETE SET NULL
);

/*------------------------------------------------------------------------*/


DROP TABLE IF EXISTS dim_riders CASCADE;

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

create table IF NOT EXISTS dim_positions(
	id_position serial primary key,
	final_position varchar(5),
	bool_half_points boolean,
	race_type varchar(10),
	num_points NUMERIC(3, 1)
);



insert into dim_positions(final_position,race_type,bool_half_points,num_points)
values ('1','main',FALSE,25),('2','main',FALSE,20),('3','main',FALSE,16),('4','main',FALSE,13),('5','main',FALSE,11),('6','main',FALSE,10),
('7','main',FALSE,9),('8','main',FALSE,8),('9','main',FALSE,7),
('10','main',FALSE,6),('11','main',FALSE,5),('12','main',FALSE,4),('13','main',FALSE,3),('14','main',FALSE,2),('15','main',FALSE,1),
('16','main',FALSE,0),('17','main',FALSE,0),('18','main',FALSE,0),('19','main',FALSE,0),('20','main',FALSE,0),('21','main',FALSE,0),
('22','main',FALSE,0),('23','main',FALSE,0),('24','main',FALSE,0),('25','main',FALSE,0),('26','main',FALSE,0),('27','main',FALSE,0),
('DNF','main',FALSE,0), ('DNS','main',FALSE,0), ('DSQ','main',FALSE,0),
('WD','main',FALSE,0), ('NC','main',FALSE,0), ('DNPQ','main',FALSE,0),
('DNP','main',FALSE,0), ('DNA','main',FALSE,0), ('EX','main',FALSE,0), ('Ret','main',FALSE,0),(null,'main',FALSE,0), ('C','main',FALSE,0),

('1','sprint',FALSE,12),('2','sprint',FALSE,9),('3','sprint',FALSE,7),('4','sprint',FALSE,6),('5','sprint',FALSE,5),('6','sprint',FALSE,4),
('7','sprint',FALSE,3),('8','sprint',FALSE,2),('9','sprint',FALSE,1),
('10','sprint',FALSE,0),('11','sprint',FALSE,0),('12','sprint',FALSE,0),('13','sprint',FALSE,0),('14','sprint',FALSE,0),('15','sprint',FALSE,0),
('16','sprint',FALSE,0),('17','sprint',FALSE,0),('18','sprint',FALSE,0),('19','sprint',FALSE,0),('20','sprint',FALSE,0),('21','sprint',FALSE,0),
('22','sprint',FALSE,0),('23','sprint',FALSE,0),('24','sprint',FALSE,0),('25','sprint',FALSE,0),('26','sprint',FALSE,0),('27','sprint',FALSE,0),
('DNF','sprint',FALSE,0), ('DNS','sprint',FALSE,0), ('DSQ','sprint',FALSE,0),
('WD','sprint',FALSE,0), ('NC','sprint',FALSE,0), ('DNPQ','sprint',FALSE,0),
('DNP','sprint',FALSE,0), ('DNA','sprint',FALSE,0), ('EX','sprint',FALSE,0), ('Ret','sprint',FALSE,0),(null,'sprint',FALSE,0), ('C','sprint',FALSE,0),

('1','main',TRUE,12.5),('2','main',TRUE,10),('3','main',TRUE,8),('4','main',TRUE,6.5),('5','main',TRUE,5.5),('6','main',TRUE,5),
('7','main',TRUE,4.5),('8','main',TRUE,4),('9','main',TRUE,3.5),
('10','main',TRUE,3),('11','main',TRUE,2.5),('12','main',TRUE,2),('13','main',TRUE,1.5),('14','main',TRUE,1),('15','main',TRUE,0.5),
('16','main',TRUE,0),('17','main',TRUE,0),('18','main',TRUE,0),('19','main',TRUE,0),('20','main',TRUE,0),('21','main',TRUE,0),
('22','main',TRUE,0),('23','main',TRUE,0),('24','main',TRUE,0),('25','main',TRUE,0),('26','main',TRUE,0),('27','main',TRUE,0),
('DNF','main',TRUE,0), ('DNS','main',TRUE,0), ('DSQ','main',TRUE,0),
('WD','main',TRUE,0), ('NC','main',TRUE,0), ('DNPQ','main',TRUE,0),
('DNP','main',TRUE,0), ('DNA','main',TRUE,0), ('EX','main',TRUE,0), ('Ret','main',TRUE,0),(null,'main',TRUE,0), ('C','main',TRUE,0),

('1','sprint',TRUE,6),('2','sprint',TRUE,4.5),('3','sprint',TRUE,3.5),('4','sprint',TRUE,3),('5','sprint',TRUE,2.5),('6','sprint',TRUE,2),
('7','sprint',TRUE,1.5),('8','sprint',TRUE,1),('9','sprint',TRUE,0.5),
('10','sprint',TRUE,0),('11','sprint',TRUE,0),('12','sprint',TRUE,0),('13','sprint',TRUE,0),('14','sprint',TRUE,0),('15','sprint',TRUE,0),
('16','sprint',TRUE,0),('17','sprint',TRUE,0),('18','sprint',TRUE,0),('19','sprint',TRUE,0),('20','sprint',TRUE,0),('21','sprint',TRUE,0),
('22','sprint',TRUE,0),('23','sprint',TRUE,0),('24','sprint',TRUE,0),('25','sprint',TRUE,0),('26','sprint',TRUE,0),('27','sprint',TRUE,0),
('DNF','sprint',TRUE,0), ('DNS','sprint',TRUE,0), ('DSQ','sprint',TRUE,0),
('WD','sprint',TRUE,0), ('NC','sprint',TRUE,0), ('DNPQ','sprint',TRUE,0),
('DNP','sprint',TRUE,0), ('DNA','sprint',TRUE,0), ('EX','sprint',TRUE,0), ('Ret','sprint',TRUE,0),(null,'sprint',TRUE,0), ('C','sprint',TRUE,0);

