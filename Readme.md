
Donc j'ai cree docker compose qui regroupe tous les outils dont on a besoin sauf airflow parce que je l'ai lance avec astro.
donc nous devons connecter airflow avec les outils qu'on a et pour faire ca veuillez suivre les instructions suivants:
pour lancer airflow avec astro :
    telecharger astro sur votre machine(consultez le site officiel de astro)
    verfier en utilisant la commande suivante : astro version
    dans orchestration lancez la commande suivante : astro dev init
    et pour demarrer le conteneur : astro dev start
pour connecter airflow avec les outils : 
    Nous devons creer un netwok qui connecte ce dernier avec kafka, spark ... :
        docker network connect data-platform-network <airflow-container>

Pour etablir la connexion :
    Dans Airflow UI:
        Admin -> Connexion
    
        A: Kafka Connection
            Conn Id: kafka_default
            Conn Type: Generic
            Host: kafka
            Port: 29092
        
        B: Spark
            Conn Id: spark_default
            Conn Type: Spark
            Host: spark://spark-master
            Port: 7077
        
        C: Postgres
            Conn Id: postgres_default
            Host: postgres
            Schema: data_platform
            Login: admin
            Password: ***
            Port: 5432

        D: Minio
            Conn Id: minio_s3
            Conn Type: Amazon S3
            Extra:
            {
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "endpoint_url": "http://minio:9000"
            }
            MINIO_ROOT_USER:	aws_access_key_id
            MINIO_ROOT_PASSWORD:	aws_secret_access_key
        
        E:
            Conn Id: trino_default
            Conn Type: HTTP
            Host: trino
            Port: 8080
    
## Superset
Pour travailler avec superset il faut creer un utilisateur et la configurer:
    cd ~/Desktop/sales-data-plateform/docker

### Crée la base superset_db dans postgres
docker exec -it postgres psql -U saifi -d postgres \
  -c "CREATE DATABASE superset_db;"

### Lance Superset
docker compose up -d superset
sleep 15

### Initialise le schéma et crée l'admin
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin

docker exec -it superset superset init


    acceder sur l'url suivant : http://localhost:8088
                                login: admin / admin


# OpenMetaData

Il faut creer une base de donnees pour openmetadata 
### 1. Recrée la base
docker exec -it postgres psql -U saifi -d postgres \
  -c "CREATE DATABASE openmetadata_db;"

### 2. Accorde les droits
docker exec -it postgres psql -U saifi -d postgres \
  -c "GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO saifi;"

### 3. Vérifie qu'elle existe
docker exec -it postgres psql -U saifi -d postgres -c "\l" | grep openmetadata

### 4. Lance la migration
docker exec -it openmetadata bash -c "cd /opt/openmetadata && ./bootstrap/bootstrap_storage.sh migrate-all"

### 5. Vérifie les tables créées
docker exec -it postgres psql -U saifi -d openmetadata_db -c "\dt" | head -20




# Ajouter la source Postgresql

Nous avons cree une base de donnees postgresl comme source de donnees aves les informations suivants :
username = "admin"
password = "admin"
base de donnees = salesdbsource

# Configuration des connecteurs


# Deploiement des connecteurs 
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @postgres-source-clients.json

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @postgres-source-produits.json

sleep 8

curl http://localhost:8083/connectors/postgres-source-clients/status
curl http://localhost:8083/connectors/postgres-source-produits/status


curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @canaux-vente-source.json

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @retours-source.json

sleep 8

curl http://localhost:8083/connectors/csv-canaux-vente/status
curl http://localhost:8083/connectors/csv-retours/status


# Lancer API
acceder au dossier data et exactement dans data-api
    pip3 install fastapi --break-system-packages
    uvicorn main:app --reload

# Creer producer.py pour fastAPI -> Kafka
    touch producer.py
    pip install kafka-python requests
    python3 producer.py

# Tous les topics sont présents !
    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
        pg_clients
        pg_produits
        canaux_vente
        retours
        ventes
        stocks

# Le traitement des donnees temps reel 
donc j'ai lu les donnees depuis kafka en utilisant spark structured streaming

# Spark to Cassandra
apres le traitement on a persister la data dans cassandra 
  keyspace : ventes_platform
  la configuration existe dans le fichier CassandraConfig.java
  
  la schema de notre base de donnees existe dans : resources/schema.cql

  on a travaille avec la classe CassandraWriter qui permet d'ecrire les donnees dans cassandra

  