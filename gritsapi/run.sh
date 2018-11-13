#!/bin/bash
# Purpose: Shell script to execute inside of a docker container

if [[ -f config.py ]];then
  celery worker -A tasks -Q priority --loglevel=INFO --concurrency=2 &
  python server.py
else
  echo "Please create a file named '/config.py' and populate with appropriate settings"
  echo "HINT: Look at /config.sample.py"
  exit 1
fi

