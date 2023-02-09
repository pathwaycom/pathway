#!/bin/bash

export PGPASSWORD='password'

sleep 3

for LOOP_ID in {1..1000}
do
    psql -d values_db -U user -c "INSERT INTO values VALUES ($LOOP_ID);"
    sleep 0.5
done