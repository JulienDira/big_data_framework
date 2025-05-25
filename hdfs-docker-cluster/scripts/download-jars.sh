#!/bin/bash

# Définir l'URL Maven Central et le fichier JAR à télécharger
JAR_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.5/postgresql-42.2.5.jar"
TARGET_DIR="/opt/hive/lib"  # Répertoire cible où le JAR sera copié dans le conteneur

# Créer le répertoire si nécessaire
mkdir -p $TARGET_DIR

# Télécharger le JAR à partir de Maven Central
wget $JAR_URL -P $TARGET_DIR

# Afficher un message une fois le téléchargement terminé
echo "PostgreSQL JDBC Driver downloaded to $TARGET_DIR"
