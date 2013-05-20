#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay
./execute_eclipse.pl -s -n TwitterNode -f 1 -c scripts/CreateUser -l=blah

