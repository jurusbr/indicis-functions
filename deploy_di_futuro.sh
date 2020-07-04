#!/bin/bash
gcloud functions deploy func_di_futuro --env-vars-file .env.yaml  --runtime python37 --trigger-topic topico-di-futuro --allow-unauthenticated

