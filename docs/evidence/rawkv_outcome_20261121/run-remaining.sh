#!/bin/bash
set -eu
cd /home/bootjp/outcome-20261121
for entry in B1:release.test B2:release.test A2:baseline.test; do
 bash comparison/run-one.sh "${entry%:*}" "${entry#*:}"
 sleep 5
done
echo completed > comparison/completed
