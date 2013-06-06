#!/bin/bash

if [ ! $# == 1 ] ; then
  echo "Usage $0 [scripts/testname]"
  exit
fi

rm -rf storage
rm -f *.log
rm -f *.replay
./execute_eclipse.pl -s -n TwitterNode -f 1 -c $1 -l=blah

