#!/bin/sh
iter=1

while true;
do
  go test -run 2A || break;
  echo "PASS ${iter}-th iteration"
  
  iter=$(($iter+1))
done
echo "THE PROGRAM FAILS AT THE ${iter}-TH ITERATION"
