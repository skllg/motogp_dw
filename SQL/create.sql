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