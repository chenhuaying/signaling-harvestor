#!/bin/sh

pid=`/sbin/pidof signaling-harvestor`
if [ x"$pid" == x ];then
    #nohup ./signaling-harvestor -log_dir logs > /dev/null 2>&1 &
    nohup ./signaling-harvestor -log_dir logs > `date +%Y%m%d%H%M%S` 2>&1 &
fi
