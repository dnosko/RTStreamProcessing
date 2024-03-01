#!/bin/bash

export PGPASSWORD=$POSTGRES_PASSWORD;
psql -U "$POSTGRES_USER" <<-EOSQL


  \c $POSTGRES_DB
  
  CREATE TABLE IF NOT EXISTS category_polygons (
    id SERIAL PRIMARY KEY,
    name VARCHAR(30)
   );



  CREATE TABLE IF NOT EXISTS polygons (
    id SERIAL PRIMARY KEY,
    creation TIMESTAMP, fence GEOMETRY(POLYGON), valid BOOLEAN, category INT REFERENCES category_polygons(id)
   );
  CREATE TABLE IF NOT EXISTS users ( id SERIAL PRIMARY KEY, device INT);

  INSERT INTO category_polygons (name) VALUES ('no go zone'), ('safe'), ('incident');

  INSERT INTO polygons (creation, fence, valid, category) VALUES
    (CURRENT_TIMESTAMP, 'POLYGON ((3 3, 4 6, 5 3, 3 3))', TRUE, 1);

    INSERT INTO users (device) VALUES
    (0),
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9),
    (10),
    (11),
    (12),
    (13),
    (14),
    (15),
    (16),
    (17),
    (18),
    (19),
    (20),
    (21),
    (22),
    (23),
    (24),
    (25),
    (26),
    (27),
    (28),
    (29),
    (30),
    (31),
    (32),
    (33),
    (34),
    (35),
    (36),
    (37),
    (38),
    (39),
    (40),
    (41),
    (42),
    (43),
    (44),
    (45),
    (46),
    (47),
    (48),
    (49),
    (50),
    (51),
    (52),
    (53),
    (54),
    (55),
    (56),
    (57),
    (58),
    (59),
    (60),
    (61),
    (62),
    (63),
    (64),
    (65),
    (66),
    (67),
    (68),
    (69),
    (70),
    (71),
    (72),
    (73),
    (74),
    (75),
    (76),
    (77),
    (78),
    (79),
    (80),
    (81),
    (82),
    (83),
    (84),
    (85),
    (86),
    (87),
    (88),
    (89),
    (90),
    (91),
    (92),
    (93),
    (94),
    (95),
    (96),
    (97),
    (98),
    (99),
    (100);
EOSQL
