from datetime import datetime, timedelta
import json
import requests
from time import sleep
import pandas as pd
from pymongo.mongo_client import MongoClient
from db_methods import DbMethods
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()


API_URL = os.getenv('FIRE_DEP_URL')
T_API = os.getenv('TELEGRAM_API_TOKEN')
T_ID = os.getenv('TELEGRAM_GROUP_ID')
T_URL = f"https://api.telegram.org/bot{T_API}/sendMessage?chat_id={T_ID}&text="
MONGO_DB_URL_READ_WRITE = os.getenv('MONGO_DB_URL_READ_WRITE')
DEBUG = True




def db_template(
    id: str,
    status: str,
    symbol_type: str,
    type: str,
    date: str,
    info: str,
    lon: float,
    lat: float,
    city: str,
    district: str,
    street: str,
    cnt_fire_dep: int
):
    return {"id":id,
            "status":status,
            "symbol_type":symbol_type,
            "type":type,
            "date":date,
            "info":info,
            "lon":lon,
            "lat":lat,
            "city":city,
            "district":district,
            "street":street,
            "cnt_fire_dep": cnt_fire_dep
    }

def keep_only_letters(input_string):
    return ''.join([char for char in input_string if char.isalpha()]).lower()

def format_city(x):
    try:
        return city_to_city[keep_only_letters(x)]
    except:
        return '-'

districts_to_cities = {
    'Braunau am Inn': [
        'Altheim', 'Aspach', 'Auerbach', 'Braunau am Inn', 'Burgkirchen',
        'Eggelsberg', 'Feldkirchen bei Mattighofen', 'Franking', 'Geretsberg',
        'Gilgenberg am Weilhart', 'Haigermoos', 'Handenberg', 'Helpfau-Uttendorf',
        'Hochburg-Ach', 'Höhnhart', 'Jeging', 'Kirchberg bei Mattighofen', 'Lengau',
        'Lochen am See', 'Maria Schmolln', 'Mattighofen', 'Mauerkirchen', 'Mining',
        'Moosbach', 'Moosdorf', 'Munderfing', 'Neukirchen an der Enknach',
        'Ostermiething', 'Palting', 'Perwang am Grabensee', 'Pfaffstätt',
        'Pischelsdorf am Engelbach', 'Polling im Innkreis', 'Roßbach', 'Schalchen',
        'Schwand im Innkreis', 'St. Georgen am Fillmannsbach', 'St. Johann am Walde',
        'St. Pantaleon', 'St. Peter am Hart', 'St. Radegund', 'St. Veit im Innkreis',
        'Tarsdorf', 'Treubach', 'Überackern', 'Weng im Innkreis'
    ],
    'Eferding': [
        'Alkoven', 'Aschach an der Donau', 'Eferding', 'Fraham',
        'Haibach ob der Donau', 'Hartkirchen', 'Hinzenbach', 'Prambachkirchen',
        'Pupping', 'Scharten', 'St. Marienkirchen an der Polsenz', 'Stroheim'
    ],
    'Freistadt': [
        'Bad Zell', 'Freistadt', 'Grünbach', 'Gutau',
        'Hagenberg im Mühlkreis', 'Hirschbach im Mühlkreis', 'Kaltenberg', 'Kefermarkt',
        'Königswiesen', 'Lasberg', 'Leopoldschlag', 'Liebenau',
        'Neumarkt im Mühlkreis', 'Pierbach', 'Pregarten', 'Rainbach im Mühlkreis',
        'Sandl', 'Schönau im Mühlkreis', 'St. Leonhard bei Freistadt', 'St. Oswald bei Freistadt',
        'Tragwein', 'Unterweißenbach', 'Unterweitersdorf', 'Waldburg',
        'Wartberg ob der Aist', 'Weitersfelden', 'Windhaag bei Freistadt'
    ],
    'Gmunden': [
        'Altmünster', 'Bad Goisern am Hallstättersee', 'Bad Ischl', 'Ebensee am Traunsee',
        'Gmunden', 'Gosau', 'Grünau im Almtal', 'Gschwandt',
        'Hallstatt', 'Kirchham', 'Laakirchen', 'Obertraun',
        'Ohlsdorf', 'Pinsdorf', 'Roitham am Traunfall', 'Scharnstein',
        'St. Konrad', 'St. Wolfgang im Salzkammergut', 'Traunkirchen', 'Vorchdorf'
    ],
    'Grieskirchen': [
        'Aistersheim', 'Bad Schallerbach', 'Eschenau im Hausruckkreis', 'Gallspach',
        'Gaspoltshofen', 'Geboltskirchen', 'Grieskirchen', 'Haag am Hausruck',
        'Heiligenberg', 'Hofkirchen an der Trattnach', 'Kallham', 'Kematen am Innbach',
        'Meggenhofen', 'Michaelnbach', 'Natternbach', 'Neukirchen am Walde',
        'Neumarkt im Hausruckkreis', 'Peuerbach', 'Pollham', 'Pötting',
        'Pram', 'Rottenbach', 'Schlüßlberg', 'St. Agatha',
        'St. Georgen bei Grieskirchen', 'St. Thomas', 'Steegen', 'Taufkirchen an der Trattnach',
        'Tollet', 'Waizenkirchen', 'Wallern an der Trattnach', 'Weibern', 'Wendling'
    ],
    'Kirchdorf': [
        'Edlbach', 'Grünburg', 'Hinterstoder', 'Inzersdorf im Kremstal',
        'Kirchdorf an der Krems', 'Klaus an der Pyhrnbahn', 'Kremsmünster', 'Micheldorf in Oberösterreich',
        'Molln', 'Nußbach', 'Oberschlierbach', 'Pettenbach',
        'Ried im Traunkreis', 'Rosenau am Hengstpaß', 'Roßleithen', 'Schlierbach',
        'Spital am Pyhrn', 'St. Pankraz', 'Steinbach am Ziehberg', 'Steinbach an der Steyr',
        'Vorderstoder', 'Wartberg an der Krems', 'Windischgarsten'
    ],
    'Linz-Land': [
        'Allhaming', 'Ansfelden', 'Asten', 'Eggendorf im Traunkreis',
        'Enns', 'Hargelsberg', 'Hofkirchen im Traunkreis', 'Hörsching',
        'Kematen an der Krems', 'Kirchberg-Thening', 'Kronstorf', 'Leonding',
        'Neuhofen an der Krems', 'Niederneukirchen', 'Oftering', 'Pasching',
        'Piberbach', 'Pucking', 'St. Florian', 'St. Marien',
        'Traun', 'Wilhering'
    ],
    'Perg': [
        'Allerheiligen im Mühlkreis', 'Arbing', 'Bad Kreuzen', 'Baumgartenberg',
        'Dimbach', 'Grein', 'Katsdorf', 'Klam',
        'Langenstein', 'Luftenberg an der Donau', 'Mauthausen', 'Mitterkirchen im Machland',
        'Münzbach', 'Naarn im Machlande', 'Pabneukirchen', 'Perg',
        'Rechberg', 'Ried in der Riedmark', 'Saxen', 'Schwertberg',
        'St. Georgen am Walde', 'St. Georgen an der Gusen', 'St. Nikola an der Donau', 'St. Thomas am Blasenstein',
        'Waldhausen im Strudengau', 'Windhaag bei Perg'
    ],
    'Ried': [
        'Andrichsfurt', 'Antiesenhofen', 'Aurolzmünster', 'Eberschwang',
        'Eitzing', 'Geiersberg', 'Geinberg', 'Gurten',
        'Hohenzell', 'Kirchdorf am Inn', 'Kirchheim im Innkreis', 'Lambrechten',
        'Lohnsburg am Kobernaußerwald', 'Mehrnbach', 'Mettmach', 'Mörschwang',
        'Mühlheim am Inn', 'Neuhofen im Innkreis', 'Obernberg am Inn', 'Ort im Innkreis',
        'Pattigham', 'Peterskirchen', 'Pramet', 'Reichersberg',
        'Ried im Innkreis', 'Schildorn', 'Senftenbach', 'St. Georgen bei Obernberg am Inn',
        'St. Marienkirchen am Hausruck', 'St. Martin im Innkreis', 'Taiskirchen im Innkreis', 'Tumeltsham',
        'Utzenaich', 'Waldzell', 'Weilbach', 'Wippenham'
    ],
    'Rohrbach': [
        'Aigen-Schlägl', 'Altenfelden', 'Arnreit', 'Atzesberg',
        'Auberg', 'Haslach an der Mühl', 'Helfenberg', 'Hofkirchen im Mühlkreis',
        'Hörbich', 'Julbach', 'Kirchberg ob der Donau', 'Klaffer am Hochficht',
        'Kleinzell im Mühlkreis', 'Kollerschlag', 'Lembach im Mühlkreis', 'Lichtenau im Mühlkreis',
        'Nebelberg', 'Neufelden', 'Neustift im Mühlkreis', 'Niederkappel',
        'Niederwaldkirchen', 'Oberkappel', 'Oepping', 'Peilstein im Mühlviertel',
        'Pfarrkirchen im Mühlkreis', 'Putzleinsdorf', 'Rohrbach-Berg', 'Sarleinsbach',
        'Schwarzenberg am Böhmerwald', 'St. Johann am Wimberg', 'St. Martin im Mühlkreis', 'St. Oswald bei Haslach',
        'St. Peter am Wimberg', 'St. Stefan-Afiesl', 'St. Ulrich im Mühlkreis', 'St. Veit im Mühlkreis',
        'Ulrichsberg'
    ],
    'Schärding': [
        'Altschwendt', 'Andorf', 'Brunnenthal', 'Diersbach',
        'Dorf an der Pram', 'Eggerding', 'Engelhartszell an der Donau', 'Enzenkirchen',
        'Esternberg', 'Freinberg', 'Kopfing im Innkreis', 'Mayrhof',
        'Münzkirchen', 'Raab', 'Rainbach im Innkreis', 'Riedau',
        'Schardenberg', 'Schärding', 'Sigharting', 'St. Aegidi',
        'St. Florian am Inn', 'St. Marienkirchen bei Schärding', 'St. Roman', 'St. Willibald',
        'Suben', 'Taufkirchen an der Pram', 'Vichtenstein', 'Waldkirchen am Wesen',
        'Wernstein am Inn', 'Zell an der Pram'
    ],
    'Steyr-Land': [
        'Adlwang', 'Aschach an der Steyr', 'Bad Hall', 'Dietach',
        'Gaflenz', 'Garsten', 'Großraming', 'Laussa',
        'Losenstein', 'Maria Neustift', 'Pfarrkirchen bei Bad Hall', 'Reichraming',
        'Rohr im Kremstal', 'Schiedlberg', 'Sierning', 'St. Ulrich bei Steyr',
        'Ternberg', 'Waldneukirchen', 'Weyer', 'Wolfern'
    ],
    'Urfahr-Umgebung': [
        'Alberndorf in der Riedmark', 'Altenberg bei Linz', 'Bad Leonfelden', 'Eidenberg',
        'Engerwitzdorf', 'Feldkirchen an der Donau', 'Gallneukirchen', 'Goldwörth',
        'Gramastetten', 'Haibach im Mühlkreis', 'Hellmonsödt', 'Herzogsdorf',
        'Kirchschlag bei Linz', 'Lichtenberg', 'Oberneukirchen', 'Ottenschlag im Mühlkreis',
        'Ottensheim', 'Puchenau', 'Reichenau im Mühlkreis', 'Reichenthal',
        'Schenkenfelden', 'Sonnberg im Mühlkreis', 'St. Gotthard im Mühlkreis', 'Steyregg',
        'Vorderweißenbach', 'Walding', 'Zwettl an der Rodl'
    ],
    'Vöcklabruck': [
        'Ampflwang im Hausruckwald', 'Attersee am Attersee', 'Attnang-Puchheim', 'Atzbach',
        'Aurach am Hongar', 'Berg im Attergau', 'Desselbrunn', 'Fornach',
        'Frankenburg am Hausruck', 'Frankenmarkt', 'Gampern', 'Innerschwand am Mondsee',
        'Lenzing', 'Manning', 'Mondsee', 'Neukirchen an der Vöckla', 'Niederthalheim',
        'Nußdorf am Attersee', 'Oberhofen am Irrsee', 'Oberndorf bei Schwanenstadt', 'Oberwang',
        'Ottnang am Hausruck', 'Pfaffing', 'Pilsbach', 'Pitzenberg', 'Pöndorf',
        'Puchkirchen am Trattberg', 'Pühret', 'Redleiten', 'Redlham', 'Regau', 'Rüstorf',
        'Rutzenham', 'Schlatt', 'Schörfling am Attersee', 'Schwanenstadt', 'Seewalchen am Attersee',
        'St. Georgen im Attergau', 'St. Lorenz', 'Steinbach am Attersee', 'Straß im Attergau',
        'Tiefgraben', 'Timelkam', 'Ungenach', 'Unterach am Attersee', 'Vöcklabruck',
        'Vöcklamarkt', 'Weißenkirchen im Attergau', 'Weyregg am Attersee', 'Wolfsegg am Hausruck',
        'Zell am Moos', 'Zell am Pettenfirst'
    ],
    'Wels-Land': [
        'Aichkirchen', 'Bachmanning', 'Bad Wimsbach-Neydharting', 'Buchkirchen',
        'Eberstalzell', 'Edt bei Lambach', 'Fischlham', 'Gunskirchen', 
        'Holzhausen', 'Krenglbach', 'Lambach', 'Marchtrenk', 
        'Neukirchen bei Lambach', 'Offenhausen', 'Pennewang', 'Pichl bei Wels', 
        'Sattledt', 'Schleißheim', 'Sipbachzell', 'Stadl-Paura',
        'Steinerkirchen an der Traun', 'Steinhaus', 'Thalheim bei Wels', 'Weißkirchen an der Traun'
    ],
    'Wels': [
        'Wels'
    ],
    'Linz': [
        'Linz'
    ]
}
city_to_district = {}
for district, cities in districts_to_cities.items():
    for city in cities:
        city_to_district[keep_only_letters(city)] = district

city_to_city = {}
for district, cities in districts_to_cities.items():
    for city in cities:
        city_to_city[keep_only_letters(city)] = city

type_symbol_dict = {'Brandmeldealarm': '🚨',
                    'Brand': '🔥', 
                    'Person': '👷', 
                    'Unwetter': '☁', 
                    'Verkehrsunfall': '🚗', 
                    'Fahrzeugbergung': '🚙',
                    'Öl': '🛢️',
                    'Andere': '❗'}

symbol_type_dict = dict((v,k) for k,v in type_symbol_dict.items())
type_colors = {
    'Brand': 'darkred',
    'Person': 'blue',
    'Unwetter': 'green',
    'Verkehrsunfall': 'orange',
    'Fahrzeugbergung': 'beige',
    'Brandmeldealarm': 'purple',
    'Öl': 'lightblue',
    'Andere': 'gray'
}
type_icons = {
    'Brandmeldealarm': 'fire',
    'Brand': 'fire',
    'Person': 'user',
    'Unwetter': 'cloud',
    'Verkehrsunfall': 'dashboard',
    'Fahrzeugbergung': 'car',
    'Öl': 'oil drum',
    'Andere': 'alert'
}

district_abr_full = {
    "BR": "Braunau am Inn",
    "EF": "Eferding",
    "FR": "Freistadt",
    "GM": "Gmunden",
    "GR": "Grieskirchen",
    "KI": "Kirchdorf an der Krems",
    "L": "Linz",
    "LL": "Linz-Land",
    "PE": "Perg",
    "RI": "Ried im Innkreis",
    "RO": "Rohrbach im Mühlkreis",
    "SD": "Schärding",
    "SE": "Steyr-Land",
    "SR": "Steyr",
    "UU": "Urfahr-Umgebung",
    "VB": "Vöcklabruck",
    "WE": "Wels",
    "WL": "Wels-Land"
}
district_full_abr = dict((v,k) for k,v in district_abr_full.items())

def _notifyUser(url, msg):
        requests.get(url+msg).json()

def getTaskType(task: dict) -> tuple:
    try:
        if "Brandmeldealarm" in task['einsatztyp']['text'].lower():
                return '🚨', 'Brandmeldealarm'
        elif "Brandmeldealarm" in task['einsatzsubtyp']['text'].lower():
            return '🚨', 'Brandmeldealarm'
        else:
            return [type_symbol_dict[task['einsatzart'].capitalize()], task['einsatzart'].capitalize()]
    except KeyError:
        for key, value in type_symbol_dict.items():
            if "Brandmeldealarm" in task['einsatztyp']['text'].lower():
                return '🚨', 'Brandmeldealarm'
            elif key.lower() in task['einsatztyp']['text'].lower():
                return value, key
            elif key.lower() in task['einsatzsubtyp']['text'].lower():
                return value, key
        return '❗', 'Andere'


def getNewTasks(url: str, debug: bool = False):
    response = requests.get(url)
    db_new_list = []
    if response.status_code != 200:
        print("API Error")
    resp_json = json.loads(response.content.decode('utf8').replace("'", '"')[9:-2])
    if "einsaetze" not in resp_json.keys():
        return []
    for _, task in resp_json['einsaetze'].items():
        data = {}
        task = task['einsatz']
        data['id'] = task['num1']
        data['status'] = 'Aktiv'
        type_symbol, type_text = getTaskType(task)
        data['symbol_type'] = type_symbol
        data['type'] = type_text
        data['date'] = datetime.strptime(task['startzeit'], '%a, %d %b %Y %H:%M:%S %z')
        data['info'] = f"{task['einsatztyp']['text'].capitalize()} | {task['einsatzsubtyp']['text'].capitalize()}"
        data['lon'] = task["wgs84"]["lng"]
        data['lat'] = task["wgs84"]["lat"]
        district, city = task['einsatzort'].split(' - ')
        data['city'] = format_city(city)
        data['district'] = district
        try:
            street_list = task['adresse']['default'].split(' ')
            street_list[0] = street_list[0].capitalize()
            street_list[-2] = street_list[-2].capitalize()
            data['street'] = ' '.join(street_list)
        except:
            data['street'] = ''
        data['cnt_fire_dep'] = int(task['cntfeuerwehren'])
        db_new_list.append(data)

    return db_new_list


def updateTasks(active_tasks: list, old_tasks: pd.DataFrame, t_url: str, debug: bool = False):
    active_ids = [d["id"] for d in active_tasks]
    for id_ in old_tasks['id'].values:
        if id_ not in active_ids and old_tasks[old_tasks['id'] == id_]['status'].values[0] != 'Abgeschlossen':
            if debug:
                print(f"Task {id_} is finished")
            dbm.dbUpdateOne({'id': id_}, {'status': 'Abgeschlossen'})
            task = old_tasks[old_tasks['id'] == id_].to_dict('records')[0]
            msg = f"ABGESCHLOSSEN: {task['symbol_type']} {task['info'].split(' | ')[1]} in {task['city']} ({task['district']})\n({task['id']})"
            _notifyUser(t_url, msg)


def postNewTasks(new_tasks: list, old_tasks: pd.DataFrame, dbm: classmethod, t_url: str, debug: bool = False):
    for task in new_tasks:
        if task['id'] not in old_tasks['id'].values:
            if debug:
                print(f"New Task: {task}")
            maps_tag = f'https://maps.google.com/?q={task["lat"]},{task["lon"]}'
            city = "--" if len(task['city']) == 0 else task['city']
            msg =   f"{task['symbol_type']} {task['info'].split(' | ')[1]} in {city} ({task['district']})" + \
                    f"\nEinsatzart: {task['type']}\nEinsatz ID:{task['id']}\nFeuerwehren im Einsatz:{task['cnt_fire_dep']}\n{maps_tag}"
            _notifyUser(t_url, msg)
            dbm.dbPost(task)


def runTaskPipeline(dbm: classmethod, debug: bool = False):
    new_tasks = getNewTasks(API_URL)
    old_tasks = dbm.dbGetAll()
    postNewTasks(new_tasks, old_tasks, dbm, T_URL, debug)
    updateTasks(new_tasks, old_tasks, T_URL, debug)



if __name__ == '__main__':
    dbm = DbMethods()
    while True:
        try:
            runTaskPipeline(dbm, DEBUG)
            logging.info("Run Successful")
            sleep(60)  # Wait for 60 seconds before next execution
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            sleep(60)