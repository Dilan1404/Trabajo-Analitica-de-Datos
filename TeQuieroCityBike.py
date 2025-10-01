from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import sys
import requests
import math
import pandas as pd
from datetime import datetime
from dateutil import tz
import xml.etree.ElementTree as ET
import logging
import re
from bs4 import BeautifulSoup

# ---------- CONFIG ----------
CITYBIKE_URL = "https://www.citybikelima.com/es#the-map"
GMAPS_KML = "https://www.google.com/maps/d/kml?mid=12PUl4VbbO3IBWRSaXrCMHH0u_NI&hl=es"

# Output (no limitado a 5 días)
OUTPUT_EXCEL = "citybike_lima.xlsx"
OUTPUT_CSV = "citybike_lima.csv"

# Timezone de Lima
LIMA_TZ = tz.gettz("America/Lima")

# OpenWeatherMap - para clima por coordenadas
OWM_BASE = "https://api.openweathermap.org/data/2.5/weather"
CLIMA_MIRAFLORES_URL = "https://www.clima.com/peru/lima/miraflores-4"

MIRAFLORES_CENTER = (-12.117880, -77.033043)  # lat, lon
MIRAFLORES_RADIUS_KM = 2.0

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

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# ---------- CITYBIKE ----------
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

# ---------- CLIMA ----------
def get_weather_for_coord(lat, lon, owm_key):
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
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(CLIMA_MIRAFLORES_URL, timeout=20, headers=headers)
        resp.raise_for_status()
        text = resp.text
        soup = BeautifulSoup(text, "html.parser")
        # Buscar primer número con °
        m2 = re.search(r'(\d{1,2}(?:\.\d+)?)\s*°', text)
        temp = float(m2.group(1)) if m2 else None
        return {'temp_C': temp, 'clima': "Desconocido"}
    except Exception as e:
        logging.warning(f"No se pudo scrapear Clima.com Miraflores: {e}")
        return None

# ---------- PRINCIPAL ----------
def collect_snapshot(owm_key=None):
    stations = try_citybikes_api()
    if not stations:
        logging.error("No se pudo obtener lista de estaciones.")
        return []

    ts = now_ts()
    clima_miraf = scrape_clima_miraflores()

    rows = []
    for s in stations:
        lat = s.get('lat')
        lon = s.get('lon')

        # Clima asignado
        weather = get_weather_for_coord(lat, lon, owm_key) if (not clima_miraf and lat and lon) else None

        row = {
            'scrape_timestamp': ts.isoformat(),
            'station_id': s.get('id'),
            'station_name': s.get('name'),
            'lat': lat,
            'lon': lon,
            'capacity': s.get('capacity'),
            'free_bikes': s.get('free_bikes'),
            'empty_slots': s.get('empty_slots'),
            'day_of_week': ts.strftime("%A"),
            'periodo_dia': periodo_del_dia(ts),
            'weather_main': weather.get('weather_main') if weather else None,
            'weather_desc': weather.get('weather_desc') if weather else None,
            'temp_C': clima_miraf.get('temp_C') if clima_miraf else (weather.get('temp_C') if weather else None),
            'wind_speed': weather.get('wind_speed') if weather else None,
            'clima_miraflores': clima_miraf.get('clima') if clima_miraf else None
        }
        rows.append(row)
    return rows

def run_collector(owm_key=None, out_excel=OUTPUT_EXCEL, out_csv=OUTPUT_CSV):
    logging.info("Ejecutando snapshot único (workflow programado cada 30 min).")
    snapshot = collect_snapshot(owm_key)
    if snapshot:
        df = pd.DataFrame(snapshot)
        df.to_csv(out_csv, mode="a", header=not os.path.exists(out_csv), index=False)
        df.to_excel(out_excel, index=False)
        logging.info(f"Guardado {len(snapshot)} registros (CSV y Excel).")
    else:
        logging.warning("Snapshot vacío.")

# ---------- Ejecutar ----------
OWM_KEY = None
run_collector(owm_key=OWM_KEY)






