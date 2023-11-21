#!/bin/bash

# Create the docker image
sudo docker build -t dot-project-backend -f Dockerfile.app .

# Tag & Push the docker image to Google Container Registry
docker tag dot-project-backend gcr.io/gcpsubhrajyoti-test-project/dot-project-backend
docker push gcr.io/gcpsubhrajyoti-test-project/dot-project-backend

# Deploy the docker image to Google Cloud Run
gcloud run deploy dotproject --image gcr.io/gcpsubhrajyoti-test-project/dot-project-backend:latest --region us-central1 --allow-unauthenticated --project gcpsubhrajyoti-test-project
