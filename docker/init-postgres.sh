#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -U "$POSTGRES_USER"  <<-EOSQL
  CREATE DATABASE $POSTGRES_DB;
 
  \c $POSTGRES_DB 
  
  CREATE TABLE IF NOT EXISTS category (
    id SERIAL PRIMARY KEY,
    name VARCHAR(30)
   );
  CREATE TABLE IF NOT EXISTS polygons (
    id SERIAL PRIMARY KEY,
    creation TIMESTAMP, fence GEOMETRY(POLYGON), valid BOOLEAN, category INT REFERENCES category(id)
   );
  CREATE TABLE IF NOT EXISTS users ( id SERIAL PRIMARY KEY, device INT);
  CREATE TABLE IF NOT EXISTS points (id SERIAL PRIMARY KEY, category INT REFERENCES category(id));
EOSQL
