#!/bin/bash

export PGPASSWORD=$POSTGRES_PASSWORD;
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /var/database.sql <<-EOSQL