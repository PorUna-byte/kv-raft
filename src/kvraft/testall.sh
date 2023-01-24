#!/bin/bash
int=1
if test $# -ne 1
then
  echo "usage:./testall.sh iteration"
else
  while(($int<=$1))
  do
    go test -run 3A
    go test -run 3B
    let "int++"
  done
fi    