#!/bin/bash

CONNECT_URL="http://localhost:8083/connectors"

CONNECTORS=(
  "csv-canaux-vente"
  "minio-sink"
  "postgres-source-clients"
  "postgres-source-produits"
  "csv-retours"
)

echo "Statut des connecteurs..."

for NAME in "${CONNECTORS[@]}"; do
  STATUS=$(curl -s "$CONNECT_URL/$NAME/status" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "  $NAME → $STATUS"
done

echo ""
echo "Vérification terminée."
