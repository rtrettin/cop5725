#!/bin/bash
rm cc.log
bin/asterixcc -config-file opt/local/conf/cc.conf > cc.log 2>&1 &
