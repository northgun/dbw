#!/bin/sh
ps aux | grep python | grep /opt/sites/dbw/write_db_service_mp.py | grep -v grep | kill -9 `awk -F' ' '{print $2}'`
