import logging
import subprocess
from models import MySQL_cluster
from mysqlconf import CLUSTER_NAMES

for cluster_name in CLUSTER_NAMES:

    cluster_instance = MySQL_cluster(cluster_name)    

    if cluster_instance.status_cluster():
        logging.info(f"MySQL service is active")
        try:
            if cluster_instance.stop_cluster():
                logging.info(f"MySQL service started. Cluster {cluster_name}")
        except subprocess.CalledProcessError as e:
            logging.error(e)

        
