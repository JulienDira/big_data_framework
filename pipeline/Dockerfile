FROM spark:3.5.0-python3

USER root

# Mettre à jour et installer les dépendances nécessaires
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installer les packages Python nécessaires
RUN pip install psycopg2-binary numpy

# Créer les répertoires nécessaires
RUN mkdir -p /spark/jars /app/log /app/checkpoints /app/application /app/datasource /opt/spark/conf

# Télécharger les JARs spécifiques nécessaires
RUN cd /spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.23/postgresql-42.2.23.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Définir la variable d'environnement pour les JARS
ENV SPARK_KAFKA_VERSION=3.3.1
ENV SPARK_VERSION=3.5.0
ENV SPARK_JARS_DIR=/spark/jars

# Copier les fichiers XML de configuration Hadoop / Hive dans le répertoire Spark conf
COPY config/hive-site.xml /opt/spark/conf/hive-site.xml
COPY config/core-site.xml /opt/spark/conf/core-site.xml
COPY config/hdfs-site.xml /opt/spark/conf/hdfs-site.xml
COPY config/yarn-site.xml /opt/spark/conf/yarn-site.xml

# Variables d'environnement pour que Spark trouve les configs Hadoop/Hive
ENV HADOOP_CONF_DIR=/opt/spark/conf
ENV HIVE_CONF_DIR=/opt/spark/conf
ENV SPARK_CONF_DIR=/opt/spark/conf

# Copier les fichiers de l'application
COPY ../ /app/application

# Définir le répertoire de travail
WORKDIR /app

# Rendre le script de démarrage exécutable
RUN chmod +x /app/application/entrypoint.sh

# Par défaut, tu peux ajouter un entrypoint si tu veux :
# ENTRYPOINT ["./consumer-raw/entrypoint_raw.sh"]
