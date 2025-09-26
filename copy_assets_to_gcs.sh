#!/usr/bin/env bash

set -euo pipefail
set -x

BUCKET="gs://fields-of-study"

# Files directly under the assets directory
gcloud storage cp --no-clobber -v "assets/*" $BUCKET/assets/
gcloud storage cp --no-clobber -v "assets/fields/" $BUCKET/assets/fields/
