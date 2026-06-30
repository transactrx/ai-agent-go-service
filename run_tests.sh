#!/bin/sh
set -e
docker-compose up --abort-on-container-exit --exit-code-from test
