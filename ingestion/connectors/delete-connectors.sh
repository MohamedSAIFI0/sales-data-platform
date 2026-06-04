#!/bin/bash

CONNECT_URL="http://localhost:8083"

echo "Récupération de la liste des connecteurs..."

connectors=$(curl -s $CONNECT_URL/connectors | jq -r '.[]')

if [ -z "$connectors" ]; then
  echo "Aucun connecteur trouvé."
  exit 0
fi

echo "Connecteurs trouvés:"
echo "$connectors"

echo "Suppression en cours..."

for c in $connectors; do
  echo "Suppression de $c ..."
  curl -s -X DELETE $CONNECT_URL/connectors/$c
  echo "✔ $c supprimé"
done

echo "Terminé."
