#!/bin/bash
rm blue-service.log
bin/asterixncservice > blue-service.log 2>&1 &
