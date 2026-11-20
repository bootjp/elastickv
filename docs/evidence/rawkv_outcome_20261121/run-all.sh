#!/bin/bash
set -u
cd /home/bootjp/outcome-20261121
for entry in A1:baseline.test B1:release.test B2:release.test A2:baseline.test; do
 trial=${entry%:*}
 binary=${entry#*:}
 if ! bash comparison/run-one.sh "$trial" "$binary"; then echo "$trial failed" > comparison/failed; exit 1; fi
 sleep 5
done
printf "completed\n" > comparison/completed
