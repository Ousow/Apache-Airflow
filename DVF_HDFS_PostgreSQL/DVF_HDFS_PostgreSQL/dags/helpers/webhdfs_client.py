"""
Client WebHDFS pour interagir avec le cluster HDFS via l'API REST.
Documentation : https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
"""

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(
        self,
        base_url: str = WEBHDFS_BASE_URL,
        user: str = WEBHDFS_USER,
        timeout: int = 60,
    ):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout

    
    # Méthodes internes

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une opération donnée."""
        if not path.startswith("/"):
            path = "/" + path
        base = f"{self.base_url}{path}?op={op}&user.name={self.user}"
        for key, value in params.items():
            base += f"&{key}={value}"
        return base

   
    # Opérations HDFS

    def mkdirs(self, hdfs_path: str) -> bool:
        """
        Crée un répertoire (et ses parents) dans HDFS.
        Retourne True si succès, lève une exception sinon.
        """
        url = self._url(hdfs_path, "MKDIRS")
        resp = requests.put(url, timeout=self.timeout)
        resp.raise_for_status()
        result = resp.json()
        if not result.get("boolean"):
            raise RuntimeError(f"MKDIRS a échoué pour {hdfs_path} : {result}")
        logger.info("HDFS mkdirs OK : %s", hdfs_path)
        return True

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """
        Uploade un fichier local vers HDFS.
        Retourne le chemin HDFS du fichier uploadé.

        WebHDFS upload = 2 étapes :
          1. PUT sur le NameNode (allow_redirects=False) -> 307 + URL de redirection
          2. PUT sur le DataNode avec le contenu binaire du fichier
        """
        # Étape 1 — Initier l'upload sur le NameNode
        init_url = self._url(hdfs_path, "CREATE", overwrite="true")
        resp1 = requests.put(init_url, allow_redirects=False, timeout=self.timeout)

        if resp1.status_code != 307:
            raise RuntimeError(
                f"Étape 1 WebHDFS CREATE : code inattendu {resp1.status_code}"
            )

        datanode_url = resp1.headers["Location"]
        logger.info("WebHDFS redirect -> %s", datanode_url)

        # Étape 2 — Uploader les données vers le DataNode
        with open(local_file_path, "rb") as fh:
            resp2 = requests.put(
                datanode_url,
                data=fh,
                headers={"Content-Type": "application/octet-stream"},
                timeout=self.timeout * 10,   # fichiers volumineux
            )

        if resp2.status_code != 201:
            raise RuntimeError(
                f"Étape 2 WebHDFS CREATE : code inattendu {resp2.status_code}"
            )

        logger.info("Upload HDFS réussi : %s", hdfs_path)
        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        """
        Lit le contenu d'un fichier HDFS.
        Retourne les données brutes (bytes).
        """
        url = self._url(hdfs_path, "OPEN")
        # allow_redirects=True par défaut : requests suit la redirection vers le DataNode
        resp = requests.get(url, allow_redirects=True, timeout=self.timeout * 5)
        resp.raise_for_status()
        logger.info(
            "HDFS open OK : %s (%d octets)", hdfs_path, len(resp.content)
        )
        return resp.content

    def exists(self, hdfs_path: str) -> bool:
        """Vérifie si un fichier ou répertoire existe dans HDFS."""
        url = self._url(hdfs_path, "GETFILESTATUS")
        try:
            resp = requests.get(url, timeout=self.timeout)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def list_status(self, hdfs_path: str) -> list:
        """Liste le contenu d'un répertoire HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        logger.info(
            "HDFS liststatus %s : %d entrée(s)", hdfs_path, len(statuses)
        )
        return statuses

    def delete(self, hdfs_path: str, recursive: bool = False) -> bool:
        """Supprime un fichier ou répertoire dans HDFS."""
        url = self._url(hdfs_path, "DELETE", recursive=str(recursive).lower())
        resp = requests.delete(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("boolean", False)
