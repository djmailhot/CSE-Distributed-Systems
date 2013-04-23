#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay
./execute.pl -s -n RIOTester -f 1 -c scripts/RIOTest
