
CREATE TABLE IF NOT EXISTS  dim_constructors (
    id_constructor SERIAL PRIMARY KEY,
    des_constructor VARCHAR(50) ,
    season integer 
);

/*
select * from dim_constructors c 
*/

create table IF NOT EXISTS dim_teams (
	id_team SERIAL PRIMARY KEY,
	id_constructor_fk integer ,
	des_team varchar(50) ,
	bool_independent boolean,
	des_motorcycle varchar(30),
	tyre_supplier varchar(15),
	rounds_participated varchar(15),
	season integer /*,
	constraint fk_id_constructor
		foreign key(id_constructor_fk)
		references dim_constructors(id_constructor)
		ON DELETE SET NULL*/
);
/*select * from dim_teams t */

create table IF NOT EXISTS dim_tracks (
	id_track serial primary key,
	des_track varchar(50) ,
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
	id_track_fk integer ,
	des_grandprix varchar(70) ,
	num_round integer,
	num_laps integer,
	season integer ,
	gp_date date/*,
	constraint fk_id_track
		foreign key(id_track_fk)
		references dim_tracks(id_track)
		ON DELETE SET NULL*/
);

/*
select * from dim_grand_prix 
*/



create table IF NOT EXISTS dim_riders(
	id_rider serial primary key,
	id_team_fk integer ,
	rider_full_name varchar(50) ,
	rider_name varchar(20) ,
	rider_lastname varchar(20) ,
	season integer ,
	rider_number integer,
	racing_class varchar(10) /*,
	constraint fk_id_team
		foreign key(id_team_fk)
		references dim_teams(id_team)
		ON DELETE SET NULL*/
	
);

create table IF NOT EXISTS fact_results(
	id_result serial primary key,
	id_rider_fk integer ,
	id_grand_prix_fk integer ,
	id_position_fk integer ,
	num_round integer,
	season integer,
	racing_class varchar(10),
	race_type varchar(10),
	pole bool,
	fastest_lap bool/*,
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
		ON DELETE SET NULL*/
);


create table IF NOT EXISTS dim_positions(
	id_position serial primary key,
	final_position varchar(5),
	race_type varchar(10),
	num_points integer
);


insert into dim_positions(final_position,race_type,num_points)
values ('1','main',25),('2','main',20),('3','main',16),('4','main',13),('5','main',11),('6','main',10),
('7','main',9),('8','main',8),('9','main',7),
('10','main',6),('11','main',5),('12','main',4),('13','main',3),('14','main',2),('15','main',1),
('16','main',0),('17','main',0),('18','main',0),('19','main',0),('20','main',0),('21','main',0),
('22','main',0),('23','main',0),('24','main',0),('25','main',0),('26','main',0),('27','main',0),
('DNF','main',0), ('DNS','main',0), ('DSQ','main',0),
('WD','main',0), ('NC','main',0), ('DNPQ','main',0),
('DNP','main',0), ('DNA','main',0), ('EX','main',0), ('Ret','main',0),(null,'main',0);


      
drop table dim_constructors cascade;
drop table dim_teams cascade;
drop table dim_riders cascade;
drop table dim_grand_prix  cascade;
drop table dim_tracks cascade;
drop table fact_results cascade;
drop table dim_positions cascade;


truncate table dim_constructors cascade;
truncate table dim_teams cascade;
truncate table dim_riders cascade;
truncate table dim_grand_prix  cascade;
truncate table dim_tracks cascade;
truncate table fact_results cascade;
truncate table dim_positions cascade;


select * from dim_constructors;
select * from dim_teams ;
select * from dim_riders ;
select * from dim_grand_prix  ;
select * from dim_tracks ;
select * from fact_results ;
select * from dim_positions  ;



