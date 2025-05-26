import subprocess
import os
import math
import logging

# Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_local_file_size_bytes(path: str) -> int:
    """
    Récupère la taille d'un fichier local en bytes.
    Le chemin peut commencer par 'file://', ce qui sera ignoré.
    """
    local_path = path.replace("file://", "")
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Fichier local non trouvé : {local_path}")
    size = os.path.getsize(local_path)
    logger.info(f"📦 Taille fichier local '{local_path}' : {size} bytes")
    return size


def get_hdfs_file_size_bytes(path: str, spark=None) -> int:
    """
    Récupère la taille d'un fichier HDFS en bytes en utilisant l'API Hadoop via Spark.
    Requiert un objet SparkSession (`spark`) pour accéder au FileSystem Java.
    """
    if spark is None:
        raise ValueError("L'objet SparkSession est requis pour lire les fichiers HDFS sans subprocess.")
    
    if not path.startswith("hdfs://"):
        raise ValueError("Chemin HDFS invalide : doit commencer par 'hdfs://'")

    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    
    if not fs.exists(hdfs_path):
        raise FileNotFoundError(f"Fichier HDFS non trouvé : {path}")
    
    size = fs.getFileStatus(hdfs_path).getLen()
    logger.info(f"📦 Taille fichier HDFS '{path}' : {size} bytes")
    return size



def calculate_partitions(file_size_bytes: int, target_partition_size_mb=128) -> int:
    """
    Calcule le nombre de partitions nécessaires pour découper un fichier
    en partitions d'une taille cible (en MB).
    Arrondi toujours vers le haut pour ne pas sous-estimer.
    """
    target_bytes = target_partition_size_mb * 1024 * 1024
    partitions = math.ceil(file_size_bytes / target_bytes)
    logger.info(f"🔢 Nombre de partitions calculé : {partitions} (taille cible par partition : {target_partition_size_mb}MB)")
    return max(1, partitions)


def get_file_size_bytes(path: str, spark=None) -> int:
    """
    Retourne la taille du fichier, localement ou sur HDFS.
    """
    if path.startswith("hdfs://"):
        return get_hdfs_file_size_bytes(path, spark)
    else:
        return get_local_file_size_bytes(path)

