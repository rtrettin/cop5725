#!/bin/bash
kill -9 `jps | egrep '(CDriver|NCService)' | awk '{print $1}'`
