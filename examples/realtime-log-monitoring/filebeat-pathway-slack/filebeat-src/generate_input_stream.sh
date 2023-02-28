#!/bin/bash

src="../../../input_stream/example.log"

sleep 1

for LOOP_ID in {1..100}
do
    printf "$LOOP_ID\n" >> $src
    sleep 1
done
for LOOP_ID in {101..200}
do
    printf "$LOOP_ID\n" >> $src
    sleep 0.01
done