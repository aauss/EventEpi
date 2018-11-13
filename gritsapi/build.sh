#!/bin/bash
# Purpose: Help build the docker image

git clone git@github.com:ecohealthalliance/annie.git
docker build -t grits-api-standalone .
rm -fr annie
