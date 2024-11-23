CREATE TABLE actors (
    actor_id INT PRIMARY KEY,
    actor_name VARCHAR(255) NOT NULL,
    films ARRAY<STRUCT<film_name VARCHAR(255), votes INT, rating FLOAT, film_id INT>>,
    quality_class VARCHAR(10),
    is_active BOOLEAN
);
