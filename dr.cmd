@echo off

SET CONTAINER_TAG=condor_watch_q-tests

ECHO Building condor_watch_q testing container...

docker build --quiet -t %CONTAINER_TAG% --file tests/_inf/Dockerfile .
docker run -it --rm %CONTAINER_TAG% %*
