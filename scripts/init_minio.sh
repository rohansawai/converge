#!/bin/sh
set -eu

mc alias set local http://minio:9000 minioadmin minioadmin
mc mb --ignore-existing local/warehouse
mc anonymous set download local/warehouse
echo "MinIO bucket warehouse ready."
