#!/usr/bin/env bash

gcloud compute instances delete fos-runner \
  --delete-disks all \
  --zone=us-east1-c \
  --quiet
