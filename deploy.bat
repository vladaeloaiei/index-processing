@echo off

echo Removing old image..
docker image rm --force index-processing-spark-submitter
echo Rebuilding image..
docker build -t index-processing-spark-submitter .
