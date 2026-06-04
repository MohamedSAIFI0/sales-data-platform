#!/bin/bash
SUPERSET_URL="http://localhost:8088"
COOKIE_JAR=$(mktemp)

# 1. Login → access token + cookie
ACCESS_TOKEN=$(curl -s -c "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin","provider":"db","refresh":true}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "Access token OK"

# 2. CSRF token
CSRF_TOKEN=$(curl -s -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X GET "$SUPERSET_URL/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")
echo "CSRF token OK"

# 3. Mettre à jour la connexion existante (id=1)
RESPONSE=$(curl -s -b "$COOKIE_JAR" -X PUT "$SUPERSET_URL/api/v1/database/1" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "X-CSRFToken: $CSRF_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Referer: $SUPERSET_URL" \
  -d '{
    "database_name": "Trino Lakehouse",
    "sqlalchemy_uri": "trino://trino@trino:8080/lakehouse",
    "expose_in_sqllab": true,
    "allow_run_async": false
  }')
echo "$RESPONSE" | python3 -m json.tool

rm -f "$COOKIE_JAR"
echo ""
echo "✅ Done. Ouvrez http://localhost:8088 → SQL Lab"