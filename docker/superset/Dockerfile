FROM apache/superset:3.1.0
USER root
RUN pip install "trino[sqlalchemy]==0.328.0" "sqlalchemy-trino==0.5.0"
USER superset
