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


def get_hdfs_file_size_bytes(path: str) -> int:
    """
    Récupère la taille d'un fichier sur HDFS en bytes.
    Le chemin doit commencer par 'hdfs://namenode:9000'.
    Utilise la commande shell 'hdfs dfs -du -s'.
    """
    if not path.startswith("hdfs://namenode:9000"):
        raise ValueError("Chemin HDFS doit commencer par 'hdfs://namenode:9000'")
    hdfs_path = path.replace("hdfs://namenode:9000", "")
    cmd = ["hdfs", "dfs", "-du", "-s", hdfs_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        size_str = result.stdout.split()[0]
        size = int(size_str)
        logger.info(f"📦 Taille fichier HDFS '{path}' : {size} bytes")
        return size
    else:
        raise RuntimeError(f"Erreur lors de la récupération de la taille HDFS : {result.stderr.strip()}")


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


def get_file_size_bytes(path: str) -> int:
    """
    Wrapper qui appelle la fonction adaptée selon que le chemin
    est local ou HDFS.
    """
    if path.startswith("hdfs://"):
        return get_hdfs_file_size_bytes(path)
    else:
        return get_local_file_size_bytes(path)
