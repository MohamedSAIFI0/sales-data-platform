#!/bin/bash

CONNECT_URL="http://localhost:8083/connectors"

echo "Déploiement des connecteurs..."

curl -X POST $CONNECT_URL \
  -H "Content-Type: application/json" \
  --data @canaux-vente-source.json

curl -X POST $CONNECT_URL \
  -H "Content-Type: application/json" \
  --data @minio-sink.json

curl -X POST $CONNECT_URL \
  -H "Content-Type: application/json" \
  --data @postgres-source-clients.json

curl -X POST $CONNECT_URL \
  -H "Content-Type: application/json" \
  --data @postgres-source-produits.json

curl -X POST $CONNECT_URL \
  -H "Content-Type: application/json" \
  --data @retours-source.json

echo ""
echo "Tous les connecteurs ont été lancés."
