#!/bin/bash
sort part-00000-5844fb23-9cb2-40e8-996d-673ea9fd4e95-c000.csv > sorted-participants.csv

get_seeded_random()
{
  seed="$1"
  openssl enc -aes-256-ctr -pass pass:"$seed" -nosalt \
    </dev/zero 2>/dev/null
}

shuf --random-source=<(get_seeded_random 0) sorted-participants.csv > shuffled-participants.csv

cat shuffled-participants.csv | grep ',true$' | head -n 1000 > shuffled-participants-with-sri-1000.csv
cat shuffled-participants.csv | grep ',false$' | head -n 1000 > shuffled-participants-without-sri-1000.csv
