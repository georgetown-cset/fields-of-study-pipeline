#!/usr/bin/env bash

IMAGE="c0-deeplearning-common-cpu-v20241224-debian-11"

# Setting this larger than the image size will generate a warning ~= "Disk size: '1000 GB' is larger than image size:
# '50 GB'. You might need to resize the root repartition manually if the operating system does not support automatic
# resizing." This can be ignored.
SIZE="1000"

gcloud compute instances create fos-runner \
    --project=gcp-cset-projects \
    --zone=us-east1-c \
    --machine-type=n2-standard-8 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=fields-of-study@gcp-cset-projects.iam.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/trace.append,https://www.googleapis.com/auth/bigquery \
    --create-disk=auto-delete=yes,boot=yes,device-name=fos-runner,image=projects/ml-images/global/images/$IMAGE,mode=rw,size=$SIZE,type=pd-standard \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any