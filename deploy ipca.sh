#!/bin/bash
gcloud functions deploy func_ipca --env-vars-file .env.yaml  --runtime python37 --trigger-topic topico-ipca --allow-unauthenticated

