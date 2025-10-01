from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')


import requests
import time
import math
import argparse
import pandas as pd
from datetime import datetime, timedelta
from dateutil import tz
import xml.etree.ElementTree as ET
import os
import sys
import logging
import re
from bs4 import BeautifulSoup

# Selenium fallback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException


# ---------- CONFIG ----------
CITYBIKE_URL = "https://www.citybikelima.com/es#the-map"
# Google My Maps KML (usa el MID que diste)
GMAPS_KML = "https://www.google.com/maps/d/kml?mid=12PUl4VbbO3IBWRSaXrCMHH0u_NI&hl=es"

# Muestreo
INTERVAL_SECONDS = 30 * 60  # cada 30 minutos


# Output
OUTPUT_EXCEL = "citybike_lima_5days.xlsx"
OUTPUT_CSV = "citybike_lima_5days.csv"

# Timezone de Lima
LIMA_TZ = tz.gettz("America/Lima")

# OpenWeatherMap - para clima por coordenadas (fallback)
OWM_BASE = "https://api.openweathermap.org/data/2.5/weather"

# Clima.com Miraflores (fuente principal para temp_C y clima)
CLIMA_MIRAFLORES_URL = "https://www.clima.com/peru/lima/miraflores-4"

# Centro aproximado de Miraflores (usado para saber si una estación está en Miraflores)
# Coordenadas de referencia (lat, lon) - fuente pública (ej. latlong.net)
MIRAFLORES_CENTER = (-12.117880, -77.033043)  # lat, lon
MIRAFLORES_RADIUS_KM = 2.0  # radio para considerar "en Miraflores" (ajustable)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- UTILIDADES ----------
def now_ts():
    return datetime.now(tz=LIMA_TZ)

def periodo_del_dia(dt):
    h = dt.hour
    if 5 <= h < 12:
        return "mañana"
    if 12 <= h < 18:
        return "tarde"
    return "noche"

def fetch_kml_gmaps(kml_url):
    r = requests.get(kml_url, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}
    placemarks = []
    for pm in root.findall('.//kml:Placemark', ns):
        name_el = pm.find('kml:name', ns)
        name = name_el.text if name_el is not None else None
        desc_el = pm.find('kml:description', ns)
        desc = desc_el.text if desc_el is not None else None
        coord_el = pm.find('.//kml:coordinates', ns)
        if coord_el is not None:
            lonlatalt = coord_el.text.strip()
            lon, lat, *_ = lonlatalt.split(',')
            placemarks.append({
                'name': name,
                'description': desc,
                'lat': float(lat),
                'lon': float(lon)
            })
    return placemarks

# Haversine: distancia en km entre 2 coordenadas
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


# ---------- CITYBIKE: intentos de extracción ----------
def try_citybikes_api():
    logging.info("Intentando API pública de CityBikes (api.citybik.es)...")
    try:
        api_root = "https://api.citybik.es/v2/networks"
        resp = requests.get(api_root, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        networks = data.get('networks', [])
        target = None
        for net in networks:
            nname = (net.get('name') or "").lower()
            city = (net.get('location', {}).get('city') or "").lower()
            if 'lima' in nname or 'lima' in city or 'citybike' in nname:
                target = net.get('id')
                break
        if not target:
            logging.info("No se encontró red clara para Lima en api.citybik.es")
            return None
        logging.info(f"Encontrada red: {target}. Descargando estaciones...")
        r2 = requests.get(f"https://api.citybik.es/v2/networks/{target}", timeout=20)
        r2.raise_for_status()
        netdata = r2.json().get('network', {})
        stations = netdata.get('stations') or []
        out = []
        for s in stations:
            out.append({
                'id': s.get('id'),
                'name': s.get('name'),
                'lat': s.get('latitude'),
                'lon': s.get('longitude'),
                'capacity': s.get('extra', {}).get('slots') or s.get('capacity') or None,
                'free_bikes': s.get('free_bikes'),
                'empty_slots': s.get('empty_slots'),
                'timestamp': s.get('timestamp')
            })
        return out
    except Exception as e:
        logging.warning(f"CityBikes API fallo: {e}")
        return None

def try_gbfs_direct(base_url_candidates):
    gbfs_paths = [
        "/gbfs/gbfs.json", "/gbfs.json", "/gbfs/en/station_information.json", "/gbfs/en/station_status.json",
        "/station_information.json", "/system_information.json"
    ]
    for base in base_url_candidates:
        for path in gbfs_paths:
            url = base.rstrip("/") + path
            try:
                r = requests.get(url, timeout=10)
                if r.status_code != 200:
                    continue
                j = r.json()
                if 'data' in j and ('stations' in j['data']):
                    stations = j['data']['stations']
                    out = []
                    for s in stations:
                        out.append({
                            'id': s.get('station_id') or s.get('id'),
                            'name': s.get('name'),
                            'lat': s.get('lat') or s.get('latitude'),
                            'lon': s.get('lon') or s.get('longitude'),
                            'capacity': s.get('capacity'),
                        })
                    return out
            except Exception:
                continue
    return None

def selenium_scrape_citybike(url=CITYBIKE_URL, headless=True):
    logging.info("Usando Selenium para renderizar y extraer estaciones del mapa (fallback)...")
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except WebDriverException as e:
        logging.error("No se pudo iniciar Chrome/Chromedriver: " + str(e))
        return None
    try:
        driver.set_page_load_timeout(30)
        driver.get(url)
        time.sleep(6)  # esperar carga JS; ajustar si es necesario
        candidates = driver.find_elements("css selector", "[class*='station'], [class*='marker'], [class*='leaflet-marker'], [data-lat]")
        stations = []
        for el in candidates:
            try:
                name = el.get_attribute("title") or el.get_attribute("data-name") or el.text
                lat = el.get_attribute("data-lat")
                lon = el.get_attribute("data-lon")
                if lat and lon:
                    stations.append({'name': name, 'lat': float(lat), 'lon': float(lon)})
            except Exception:
                continue
        # Si no se encontraron candidatos, intentar buscar JSON embebido en scripts
        if not stations:
            scripts = driver.find_elements("tag name", "script")
            for s in scripts:
                txt = s.get_attribute("innerHTML")
                if not txt:
                    continue
                if "stations" in txt.lower() or "markers" in txt.lower():
                    import json
                    try:
                        # heurística para extraer JSON arrays
                        matches = re.findall(r'(\[\s*{(?:[^{}]|(?R))*}\s*\])', txt, flags=re.S)
                    except re.error:
                        matches = []
                    for m in matches:
                        try:
                            arr = json.loads(m)
                            for entry in arr:
                                if 'lat' in entry and 'lon' in entry:
                                    stations.append({'id': entry.get('id'), 'name': entry.get('name'), 'lat': entry.get('lat'), 'lon': entry.get('lon')})
                        except Exception:
                            continue
        driver.quit()
        if not stations:
            logging.warning("No se encontraron estaciones con Selenium (estructura inesperada).")
            return None
        logging.info(f"Extraídas {len(stations)} estaciones vía Selenium.")
        return stations
    except Exception as e:
        logging.error("Error Selenium: " + str(e))
        try:
            driver.quit()
        except Exception:
            pass
        return None




# ---------- CLIMA ----------
def get_weather_for_coord(lat, lon, owm_key):
    """Fallback: OpenWeatherMap (si no hay datos de clima.com o si estación fuera de Miraflores)"""
    if not owm_key:
        return None
    params = {"lat": lat, "lon": lon, "appid": owm_key, "units": "metric", "lang": "es"}
    try:
        r = requests.get(OWM_BASE, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        return {
            'weather_main': j.get('weather', [{}])[0].get('main'),
            'weather_desc': j.get('weather', [{}])[0].get('description'),
            'temp_C': j.get('main', {}).get('temp'),
            'wind_speed': j.get('wind', {}).get('speed'),
        }
    except Exception as e:
        logging.warning(f"OWM fallo para {lat},{lon}: {e}")
        return None

def scrape_clima_miraflores():
    """
    Extrae temperatura y descripción del tiempo desde Clima.com (Miraflores).
    Nota: la página puede cambiar la estructura; uso una estrategia por pasos:
    1) intentar localizar el bloque principal cerca del header 'Miraflores'
    2) heurística: buscar 'Image: ...' seguido de '##°' en el HTML
    3) fallback: primer número seguido de '°' en la página
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
        resp = requests.get(CLIMA_MIRAFLORES_URL, timeout=20, headers=headers)
        resp.raise_for_status()
        text = resp.text
        soup = BeautifulSoup(text, "html.parser")

        temp = None
        clima_desc = None

        # 1) Intentar encontrar un header que contenga 'Miraflores' y buscar el primer número con '°' cercano
        header = soup.find(lambda tag: tag.name in ['h1','h2','h3','div'] and 'Miraflores' in (tag.get_text() or ""))
        if header:
            # buscar texto próximo con grados
            candidate = header.find_next(string=re.compile(r'\d{1,2}(?:\.\d+)?\s*°'))
            if candidate:
                m = re.search(r'(\d{1,2}(?:\.\d+)?)\s*°', candidate)
                if m:
                    temp = float(m.group(1))
            # intentar sacar descripción del clima desde una imagen alt cercana
            img = header.find_next('img', alt=True)
            if img and img.get('alt'):
                clima_desc = img.get('alt').strip()

        # 2) fallback por regex buscando patrones como 'Image: Nuboso 16°' o 'Image: Icon 18°'
        if (temp is None) or (clima_desc is None):
            m = re.search(r'Image:\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)[^\d\S\r\n]{0,40}(\d{1,2}(?:\.\d+)?)\s*°', text, flags=re.S)
            if m:
                if clima_desc is None:
                    clima_desc = m.group(1).strip()
                if temp is None:
                    temp = float(m.group(2))

        # 3) último fallback: primer número° en la página
        if temp is None:
            m2 = re.search(r'(\d{1,2}(?:\.\d+)?)\s*°', text)
            if m2:
                temp = float(m2.group(1))

        # Normalizar descripción mínima (si está presente)
        if clima_desc:
            clima_desc = re.sub(r'\s+', ' ', clima_desc).strip()

        return {'temp_C': temp, 'clima': clima_desc}
    except Exception as e:
        logging.warning(f"No se pudo scrapear Clima.com Miraflores: {e}")
        return None




# ---------- PRINCIPAL ----------
def collect_snapshot(owm_key=None):
    """
    Intenta obtener estaciones con: CityBikes API -> GBFS en sitio -> Selenium fallback.
    Extrae clima de Clima.com (Miraflores) y lo asigna a estaciones dentro de Miraflores.
    """
    stations = try_citybikes_api()
    if stations:
        logging.info("Usando datos de api.citybik.es")
    else:
        stations = try_gbfs_direct(["https://www.citybikelima.com", "https://citybikelima.com", "https://www.citybikelima.com/es"])
        if stations:
            logging.info("Usando GBFS directo desde sitio")
        else:
            stations = selenium_scrape_citybike(CITYBIKE_URL)
            if stations:
                logging.info("Usando extracción via Selenium (fallback)")
    if not stations:
        logging.error("No se pudo obtener lista de estaciones por ninguna vía.")
        return []

    ts = now_ts()
    # extraer clima para Miraflores (una única llamada por snapshot)
    clima_miraf = scrape_clima_miraflores()  # {'temp_C': float, 'clima': str} o None
    if clima_miraf:
        logging.info(f"Clima.com Miraflores: temp={clima_miraf.get('temp_C')}°C, desc='{clima_miraf.get('clima')}'")
    else:
        logging.info("No se obtuvo clima desde Clima.com (fallback a OWM por estación si se proporcionó o None)")

    rows = []
    for s in stations:
        lat = s.get('lat')
        lon = s.get('lon')
        capacity = s.get('capacity')
        free_bikes = s.get('free_bikes') if 'free_bikes' in s else None
        empty_slots = s.get('empty_slots') if 'empty_slots' in s else None

        # fallback: obtener clima por coordenadas (OpenWeatherMap) si no hay clima_miraf o estación fuera de Miraflores
        weather = None
        if (not clima_miraf) and lat and lon and owm_key:
            weather = get_weather_for_coord(lat, lon, owm_key)

        # determinar si la estación está en Miraflores (usa Haversine)
        in_miraflores = False
        try:
            if lat is not None and lon is not None:
                dkm = haversine_km(float(lat), float(lon), MIRAFLORES_CENTER[0], MIRAFLORES_CENTER[1])
                in_miraflores = (dkm <= MIRAFLORES_RADIUS_KM)
        except Exception:
            in_miraflores = False

        # asignación de temperatura y descripción:
        if in_miraflores and clima_miraf:
            temp_assigned = clima_miraf.get('temp_C')
            clima_assigned = clima_miraf.get('clima')
        else:
            # fuera de Miraflores o no se obtuvo clima_miraf: usar OWM si existe o None
            temp_assigned = weather.get('temp_C') if weather else (clima_miraf.get('temp_C') if clima_miraf else None)
            clima_assigned = weather.get('weather_desc') if weather else (clima_miraf.get('clima') if clima_miraf else None)

        row = {
            'scrape_timestamp': ts.isoformat(),
            'station_id': s.get('id'),
            'station_name': s.get('name'),
            'lat': lat,
            'lon': lon,
            'capacity': capacity,
            'free_bikes': free_bikes,
            'empty_slots': empty_slots,
            'day_of_week': ts.strftime("%A"),
            'periodo_dia': periodo_del_dia(ts),
            # Si hay OWM quedará en weather_main/desc; temp_C prioriza clima.com para Miraflores
            'weather_main': (weather.get('weather_main') if weather else None),
            'weather_desc': (weather.get('weather_desc') if weather else None),
            'temp_C': temp_assigned,
            'wind_speed': (weather.get('wind_speed') if weather else None),
            # Clima específico extraído de Clima.com (si disponible) para Miraflores
            'clima_miraflores': (clima_miraf.get('clima') if clima_miraf else None),
            'temp_miraflores': (clima_miraf.get('temp_C') if clima_miraf else None),
            'in_miraflores': in_miraflores,
            # Placeholder: inferir 'zona' (oficinas/universidad/turistica) y 'densidad_poblacional' con datos externos
            'zona_inferida': None,
            'densidad_poblacional': None
        }
        rows.append(row)
    return rows

def run_collector(owm_key=None, out_excel=OUTPUT_EXCEL, out_csv=OUTPUT_CSV):
    all_rows = []
    logging.info(f"Ejecutando")
    snapshot = collect_snapshot(owm_key)
            if snapshot:
                all_rows.extend(snapshot)
                df = pd.DataFrame(all_rows)
                df.to_csv(out_csv, index=False)
                df.to_excel(out_excel, index=False)
                logging.info(f"Guardado {len(all_rows)} registros (CSV y Excel).")
            else:
                logging.warning("Snapshot vacío en esta ejecución.")
    except KeyboardInterrupt:
        logging.info("Detenido por usuario (KeyboardInterrupt).")
    except Exception as e:
        logging.error("Error en run_collector: " + str(e))
    finally:
        df = pd.DataFrame(all_rows)
        df.to_csv(out_csv, index=False)
        df.to_excel(out_excel, index=False)
        logging.info(f"Finalizando. Guardados {len(all_rows)} registros en {out_csv} y {out_excel}.")# ---------- PRINCIPAL ----------
def collect_snapshot(owm_key=None):
    """
    Intenta obtener estaciones con: CityBikes API -> GBFS en sitio -> Selenium fallback.
    Extrae clima de Clima.com (Miraflores) y lo asigna a estaciones dentro de Miraflores.
    """
    stations = try_citybikes_api()
    if stations:
        logging.info("Usando datos de api.citybik.es")
    else:
        stations = try_gbfs_direct(["https://www.citybikelima.com", "https://citybikelima.com", "https://www.citybikelima.com/es"])
        if stations:
            logging.info("Usando GBFS directo desde sitio")
        else:
            stations = selenium_scrape_citybike(CITYBIKE_URL)
            if stations:
                logging.info("Usando extracción via Selenium (fallback)")
    if not stations:
        logging.error("No se pudo obtener lista de estaciones por ninguna vía.")
        return []

    ts = now_ts()
    # extraer clima para Miraflores (una única llamada por snapshot)
    clima_miraf = scrape_clima_miraflores()  # {'temp_C': float, 'clima': str} o None
    if clima_miraf:
        logging.info(f"Clima.com Miraflores: temp={clima_miraf.get('temp_C')}°C, desc='{clima_miraf.get('clima')}'")
    else:
        logging.info("No se obtuvo clima desde Clima.com (fallback a OWM por estación si se proporcionó o None)")

    rows = []
    for s in stations:
        lat = s.get('lat')
        lon = s.get('lon')
        capacity = s.get('capacity')
        free_bikes = s.get('free_bikes') if 'free_bikes' in s else None
        empty_slots = s.get('empty_slots') if 'empty_slots' in s else None

        # fallback: obtener clima por coordenadas (OpenWeatherMap) si no hay clima_miraf o estación fuera de Miraflores
        weather = None
        if (not clima_miraf) and lat and lon and owm_key:
            weather = get_weather_for_coord(lat, lon, owm_key)

        # determinar si la estación está en Miraflores (usa Haversine)
        in_miraflores = False
        try:
            if lat is not None and lon is not None:
                dkm = haversine_km(float(lat), float(lon), MIRAFLORES_CENTER[0], MIRAFLORES_CENTER[1])
                in_miraflores = (dkm <= MIRAFLORES_RADIUS_KM)
        except Exception:
            in_miraflores = False

        # asignación de temperatura y descripción:
        if in_miraflores and clima_miraf:
            temp_assigned = clima_miraf.get('temp_C')
            clima_assigned = clima_miraf.get('clima')
        else:
            # fuera de Miraflores o no se obtuvo clima_miraf: usar OWM si existe o None
            temp_assigned = weather.get('temp_C') if weather else (clima_miraf.get('temp_C') if clima_miraf else None)
            clima_assigned = weather.get('weather_desc') if weather else (clima_miraf.get('clima') if clima_miraf else None)

        row = {
            'scrape_timestamp': ts.isoformat(),
            'station_id': s.get('id'),
            'station_name': s.get('name'),
            'lat': lat,
            'lon': lon,
            'capacity': capacity,
            'free_bikes': free_bikes,
            'empty_slots': empty_slots,
            'day_of_week': ts.strftime("%A"),
            'periodo_dia': periodo_del_dia(ts),
            # Si hay OWM quedará en weather_main/desc; temp_C prioriza clima.com para Miraflores
            'weather_main': (weather.get('weather_main') if weather else None),
            'weather_desc': (weather.get('weather_desc') if weather else None),
            'temp_C': temp_assigned,
            'wind_speed': (weather.get('wind_speed') if weather else None),
            # Clima específico extraído de Clima.com (si disponible) para Miraflores
            'clima_miraflores': (clima_miraf.get('clima') if clima_miraf else None),
            'temp_miraflores': (clima_miraf.get('temp_C') if clima_miraf else None),
            'in_miraflores': in_miraflores,
            # Placeholder: inferir 'zona' (oficinas/universidad/turistica) y 'densidad_poblacional' con datos externos
            'zona_inferida': None,
            'densidad_poblacional': None
        }
        rows.append(row)
    return rows

def run_collector(owm_key=None, out_excel=OUTPUT_EXCEL, out_csv=OUTPUT_CSV):
    all_rows = []
    logging.info(f"Ejecutando snapshot")
    snapshot = collect_snapshot(owm_key)
            if snapshot:
                all_rows.extend(snapshot)
                df = pd.DataFrame(all_rows)
                df.to_csv(out_csv, index=False)
                df.to_excel(out_excel, index=False)
                logging.info(f"Guardado {len(all_rows)} registros (CSV y Excel).")
            else:
                logging.warning("Snapshot vacío en esta ejecución.")

    except KeyboardInterrupt:
        logging.info("Detenido por usuario (KeyboardInterrupt).")
    except Exception as e:
        logging.error("Error en run_collector: " + str(e))
    finally:
        df = pd.DataFrame(all_rows)
        df.to_csv(out_csv, index=False)
        df.to_excel(out_excel, index=False)
        logging.info(f"Finalizando. Guardados {len(all_rows)} registros en {out_csv} y {out_excel}.")




# ---------- Configuración manual para Colab ----------
OWM_KEY = None   # Si tienes clave de OpenWeatherMap, ponla aquí
# INTERVAL_MINUTES = 30
# DAYS = 5

# INTERVAL_SECONDS = INTERVAL_MINUTES * 60
# TOTAL_RUN_SECONDS = DAYS * 24 * 3600

run_collector(owm_key=OWM_KEY)





