#!/bin/bash

set -eu

rm -f test.bulktree
make
echo -e "put alpha omega\nclose\n" | ./write_bulktree test.bulktree 
echo -e "get alpha\nget missing\nclose\n" | ./read_bulktree test.bulktree

