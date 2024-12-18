CREATE TYPE films AS (
    film text,
    votes integer,
    rating REAL,
    filmid text
);

CREATE TYPE quality_classes AS
ENUM ('bad', 'average', 'good', 'star');

CREATE TABLE actors (
    actorid text,
    filmid text,
    current_year integer,
    quality_class quality_classes,
    films films [],
    is_active bool,
    PRIMARY KEY (actorid, filmid, current_year)
);
