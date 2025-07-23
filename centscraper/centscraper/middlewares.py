import logging
import socket
import requests

class LogUserAgentAndIPMiddleware:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ip_checked = False  # pour éviter de faire une requête IP à chaque fois
        self.public_ip = None

    def process_request(self, request, spider):
        ua = request.headers.get('User-Agent', b'').decode()
        
        if not self.ip_checked:
            try:
                self.public_ip = requests.get('https://api.ipify.org', timeout=3).text
                self.ip_checked = True
            except Exception as e:
                self.public_ip = 'IP inconnue'
                self.logger.warning(f"Erreur en récupérant l'IP publique : {e}")

        self.logger.info(f"🌐 UA: {ua}")
        self.logger.info(f"🌍 IP publique (via VPN) : {self.public_ip}")
