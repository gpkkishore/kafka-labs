#!/usr/bin/env bash
# KiA 9.1.2 — kcat (formerly kafkacat): the Swiss-army knife. Runs in a container via bin/kcat.
set -x
cd "$(dirname "$0")/.."
bin/kcat -L | head -20                                                   # metadata: brokers, topics, partitions, leaders
printf 'u1:{"plan":"gold"}\nu2:{"plan":"free"}\n' | bin/kcat -P -t kinaction_kcat -K:     # produce key:value
bin/kcat -C -t kinaction_kcat -o beginning -e \
  -f 'partition=%p offset=%o ts=%T key=%k value=%s headers=%h\n'          # consume with a format string
bin/kcat -C -t kinaction_kcat -o -1 -e -c 1                              # last record of each partition
bin/kcat -Q -t kinaction_kcat:0:-1                                       # offset for a timestamp query (-1 = now)
