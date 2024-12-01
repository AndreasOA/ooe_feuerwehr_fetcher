from pymongo.mongo_client import MongoClient
import json
import pandas as pd
from db_template import db_template
import requests
from misc import *
from db_methods import *
from datetime import datetime, timezone, timedelta


def uploadDataFromCsv(dbm: DbMethods):
    map_url = f'https://maps.google.com/?q='
    df = pd.read_csv("misc/notion_data.csv")
    for _, row in df.iterrows():
        lat, lon = row['Maps'].replace(map_url, '').split(',')
        try:
            street, city = row['Addresse'].split(', ')
        except:
            street = ''
            city = ''
            parsed_date = datetime.strptime(row['Date'], '%B %d, %Y %H:%M')
            formatted_date = parsed_date.replace(tzinfo=timezone(timedelta(hours=2))).strftime('%a, %d %b %Y %H:%M:%S %z')

            document = db_template(id=row['Enum'], status='abgeschlossen', symbol_type=row['Type'], 
                            type=symbol_type_dict[row['Type']], date=formatted_date, info=row['Info'], 
                            lon=float(lon), lat=float(lat), district='', city=city, street=street, cnt_fire_dep=1)
            dbm.dbDeleteOne({"id": row['Enum']})
            insert_result = dbm.dbPost(document)


def fixTaskTypes(DbMethods):
    data = dbm.dbGetAll()
    for _, row in data.iterrows():
        for key, value in symbol_type_dict.items():
            if value in row['info']:
                symbol = type_symbol_dict[value]
                type_text = value
                if 'Brandmeldealarm' in row['info']:
                    symbol = type_symbol_dict['Brandmeldealarm']
                    type_text = 'Brandmeldealarm'
                dbm.dbUpdateOne({"id": row['id']}, {"symbol_type": symbol, "type": type_text})
        if len(row['district']) != 2 and row['city'] != '':
            try:
                dbm.dbUpdateOne({"id": row['id']}, {"district":  district_full_abr[city_to_district[keep_only_letters(row['city'])]]})
            except KeyError:
                try:
                    name_1 = row['city'].split(' ')[0]
                    for key, value in city_to_district.items():
                        if name_1 in key:
                            dbm.dbUpdateOne({"id": row['id']}, {"district": district_full_abr[value]})
                            break
                except:
                    print('failed')
                    pass

def keep_only_letters(input_string):
    return ''.join([char for char in input_string if char.isalpha()]).lower()

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
    'Brandmeldealarm': 'purple',
    'Brand': 'darkred',
    'Person': 'blue',
    'Unwetter': 'green',
    'Verkehrsunfall': 'orange',
    'Fahrzeugbergung': 'beige',
    'Öl': 'lightblue',
    'Andere': 'gray'
}
type_icons = {
    'Brandmeldealarm': 'fire',
    'Brand': 'fire',
    'Person': 'user',
    'Unwetter': 'cloud',
    'Verkehrsunfall': 'exclamation-sign',
    'Fahrzeugbergung': 'exclamation-sign',
    'Öl': 'exclamation-sign',
    'Andere': 'exclamation-sign'
}

district_abr_full = {
    "BR": "Braunau am Inn",
    "EF": "Eferding",
    "FR": "Freistadt",
    "GM": "Gmunden",
    "GR": "Grieskirchen",
    "KI": "Kirchdorf",
    "L": "Linz",
    "LL": "Linz-Land",
    "PE": "Perg",
    "RI": "Ried",
    "RO": "Rohrbach",
    "SD": "Schärding",
    "SE": "Steyr-Land",
    "SR": "Steyr",
    "UU": "Urfahr-Umgebung",
    "VB": "Vöcklabruck",
    "WE": "Wels",
    "WL": "Wels-Land"
}
district_full_abr = dict((v,k) for k,v in district_abr_full.items()) 

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

if __name__ == '__main__':
    f = open("credentials.json")
    data = json.load(f)
    f.close()
    url = data['mongo_db']['url']
    dbm = DbMethods(url)
    fixTaskTypes(dbm)