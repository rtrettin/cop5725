#!/bin/bash
rm red-service.log
bin/asterixncservice > red-service.log 2>&1 &
