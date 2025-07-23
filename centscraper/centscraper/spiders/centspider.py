import scrapy
import json 
import os 
import re 
import unicodedata
import pandas as pd

class CentrisSpider(scrapy.Spider):
    name = "centspider"

    def start_requests(self):
        file_path = os.path.join(os.path.dirname(__file__), "../start_urls.txt")

        # Charger les IDs déjà exportés (via out.csv)
        try:
            df = pd.read_csv("out.csv")
            processed_ids = set(df["ID"].astype(str))
            self.logger.info(f"{len(processed_ids)} IDs déjà présents dans out.csv.")
        except FileNotFoundError:
            processed_ids = set()
            self.logger.info("Aucun fichier out.csv trouvé. On commence à zéro.")

        with open(file_path, "r") as f:
            for url in f:
                url = url.strip()
                if not url:
                    continue
                id_ = url.rstrip("/").split("/")[-1]
                if id_ not in processed_ids:
                    yield scrapy.Request(url=url, callback=self.parse, meta={"id": id_}, dont_filter=True)
                else:
                    self.logger.info(f"[SKIP] ID déjà traité : {id_}")

    
    def clean_text(self, text):
        return text.replace('\xa0', ' ').strip()

    def normalize_text(self, text):
        return unicodedata.normalize("NFC", text)

    def extract_financial_data(self, response):
        data = {}

        def get_valid_number(td):
            value = td.xpath('.//text()').get()
            if value:
                clean = self.clean_text(value)
                if clean.lower() not in ["non émise", "-", ""]:
                    try:
                        number_str = clean.replace(" ", "").replace("$", "").replace(",", "")
                        return int(number_str)
                    except ValueError:
                        pass
            return None
        
        
        rows = response.xpath('//div[contains(@class, "financial-details-table")][.//th[contains(text(), "Évaluation municipale")]]//tr')
        for row in rows:
            label = row.xpath('./td[1]/text()').get()
            td_value = row.xpath('./td[2]')
            value = get_valid_number(td_value)
            if label and value is not None:
                label = self.clean_text(label.lower())
                if "terrain" in label:
                    data['Évaluation terrain'] = value
                elif "bâtiment" in label:
                    data['Évaluation bâtiment'] = value

        rows = response.xpath('//div[contains(@class, "financial-details-table-yearly")][.//th[contains(text(), "Taxes")]]//tr')
        if not rows:
            rows = response.xpath('//div[contains(@class, "financial-details-table-monthly")][.//th[contains(text(), "Taxes")]]//tr')
        for row in rows:
            label = row.xpath('./td[1]/text()').get()
            td_value = row.xpath('./td[2]')
            value = get_valid_number(td_value)
            if label and value is not None:
                label = self.clean_text(label.lower())
                if "municipales" in label:
                    data['Taxes municipales'] = value
                elif "scolaires" in label:
                    data['Taxes scolaires'] = value

        rows = response.xpath('//div[contains(@class, "financial-details-table-yearly")][.//th[contains(text(), "Dépenses")]]//tr')
        if not rows:
            rows = response.xpath('//div[contains(@class, "financial-details-table-monthly")][.//th[contains(text(), "Dépenses")]]//tr')
        for row in rows:
            label = row.xpath('./td[1]/text()').get()
            td_value = row.xpath('./td[2]')
            value = get_valid_number(td_value)
            if label and value is not None:
                label = self.clean_text(label.lower())
                if "copropriété" in label:
                    data['Frais de copropriété'] = value

        return data
    
    def parse(self, response):
        id_ = response.meta.get("id")
        caracteristiques = {}

        caracteristiques['ID'] = id_

        type_de_propriete = response.css('span[data-id="PageTitle"]::text').get()
        adresse_complete = response.css('h2[itemprop="address"]::text').get()
        prix = response.css('meta[itemprop="price"]::attr(content)').get()

        if type_de_propriete:
            caracteristiques['Type de propriété'] = type_de_propriete.strip()
        if adresse_complete:
            caracteristiques['Adresse'] = adresse_complete.strip()
            match = re.search(r'\((.*?)\)', adresse_complete)
            if match:
                caracteristiques['Ville'] = match.group(1).strip()
        if prix:
            caracteristiques['Prix'] = prix

        teaser_block = response.css('div.row.teaser')
        if teaser_block:
            pieces = teaser_block.css('.piece::text').get()
            chambres = teaser_block.css('.cac::text').get()
            salles_de_bain = teaser_block.css('.sdb::text').get()
            if pieces:
                caracteristiques['Nombre de pièces'] = pieces.strip()
            if chambres:
                caracteristiques['Nombre de chambres'] = chambres.strip()
            if salles_de_bain:
                caracteristiques['Nombre de salles de bain'] = salles_de_bain.strip()

        for info in response.css('div.row div.carac-container'):
            titre = info.css('div.carac-title::text').get()
            valeur = info.css('div.carac-value span::text').getall()
            if titre:
                titre = self.normalize_text(titre.strip())
                valeur_nettoyee = " ".join(v.strip() for v in valeur if v.strip())
                caracteristiques[titre] = valeur_nettoyee

            walkscore = info.css('div.walkscore span::text').get()
            if walkscore:
                caracteristiques['Walkscore'] = walkscore.strip()

        description = response.css('div[itemprop="description"]::text').getall()
        if description:
            caracteristiques['Description'] = " ".join(d.strip() for d in description if d.strip())

        match_score = response.css('.row.teaser .lifestyle .ll-match-score')
        latitude = match_score.attrib.get('data-lat')
        longitude = match_score.attrib.get('data-lng')
        if latitude:
            caracteristiques['Latitude'] = latitude
        if longitude:
            caracteristiques['Longitude'] = longitude

        caracteristiques.update(self.extract_financial_data(response))

         # --- Photos ---
        caracteristiques["photos"] = []
        script_node = response.css("script::text").re_first(r"MosaicPhotoUrls\s*=\s*(\[.*?\]);")
        if script_node:
            try:
                caracteristiques["photos"] = json.loads(script_node)
            except:
                pass

        # limiter à un nombre fixe de colonnes (ex: 30 photos max)
        max_photos = 35
        for i in range(max_photos):
            try:
                caracteristiques[f"photo_{i+1}"] = caracteristiques["photos"][i]
            except IndexError:
                caracteristiques[f"photo_{i+1}"] = None  # Laisse vide si pas de photo

        caracteristiques.pop("photos", None)

        COLONNES_FIXES = [
            'ID', 'url', 'Type de propriété', 'Adresse', 'Ville', 'Prix', 'Nombre de pièces', 'Nombre de chambres', 'Nombre de salles de bain',
            'Latitude', 'Longitude', 
            'Type de copropriété', 'Superficie nette','Superficie brute', 'Année de construction', 'Stationnement total', 
            'Date d’emménagement', 'Caractéristiques additionnelles', 'Foyer / Poêle', 'Piscine', 'Superficie du terrain', 'Style de bâtiment',
            'Revenus bruts potentiels', 'Unité principale', "Nombre d’unités", 'Unités résidentielles', 'Stationnement exclus du prix', 
            'Superficie habitable', 'Superficie du bâtiment (au sol)', 'Superficie commerciale disponible', 'Utilisation de la propriété',
            'Étage', 'Accessibilité', 'Zonage', 'Walkscore', 
            'Description',
            'Évaluation terrain', 'Évaluation bâtiment',
            'Taxes municipales', 'Taxes scolaires', 'Frais de copropriété',
            #'photo_1', 'photo_2', 'photo_3', 'photo_4', 'photo_5',
        ]

        result = {}
        for key in COLONNES_FIXES:
            if key == 'url':
                result[key] = response.url
            else:
                result[key] = caracteristiques.get(key, None)

        for key, value in caracteristiques.items():
            if key not in COLONNES_FIXES:
                result[key] = value

        yield result