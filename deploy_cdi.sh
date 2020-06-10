#!/bin/bash
gcloud functions deploy func_cdi --env-vars-file .env.yaml  --runtime python37 --trigger-topic topico-cdi --allow-unauthenticated

