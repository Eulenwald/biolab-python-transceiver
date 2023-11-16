###############################################################################
####  transceiver.py
####
####  Ein Gerät, das sowohl als Sender als auch als Empfänger fungieren kann,
####  wird oft als "Transceiver" bezeichnet. Das Wort "Transceiver" ist eine 
####  Kombination der Wörter "Transmitter" und "Receiver" und wird häufig in 
####  den Bereichen der drahtlosen Kommunikation, Funktechnik und 
####  Telekommunikation verwendet.
# 
# Hier nehmen wir Sensordaten via Mqtt entgegen.
# Diese werden, im Prinzip 1 zu 1 via Api-Schnittselle and das Server-Backend 
# weitergeleitet. 
# 
# Des Weiteren nehme ich hier auch config-Anfragen fuer die ESPs oder und
# Sensoren an. In diesem Fall hole ich mir die passenden Ddaten ueber eine Api
# und Schicke die via Mqtt an den jeweiligen ESP (z. B. espXXX_btsXXX##payload)
# 
# In einem Intervall von z. B. 300 Sek hole ich mir alle config-daten und sende
# diese an die jeweiligen ESPs. Hier schicke ich pro Sensor oder aktor einzeln
# Befehle. 
###############################################################################
import time
import paho.mqtt.client as mqtt
import requests
import json
from SensorValue import SensorValue 

###############################################################################
# Konstanten Block
from var_bio_lab import URL_WEBSERVER_ONLINE, MQTT_HOST_PI, MQTT_PASSWORD, MQTT_USER

URL_WEBSERVER = URL_WEBSERVER_ONLINE

URL_NEW_VALUE =  URL_WEBSERVER + "/esp/sensor/value/new"
URL_UPDATE_VALUE = URL_WEBSERVER + "/esp/sensor/value/update"
URL_SENSOR_CONFIG = URL_WEBSERVER + "/esp/sensor/byespname?name="

JSON_HEADER = {'Content-Type': 'application/json'}
TOPIC = "values/sensors/#"

MQTT_CLIENT_ID = "TRC_001"
MQTT_HOST = MQTT_HOST_PI

PRINT_IT = 1
PRINT_IT_NOT = 0
MQTT_CLIENT = mqtt.Client(MQTT_CLIENT_ID)
# End Konstanten Block
###############################################################################

###############################################################################
# Hilfsvariablen Block
valueList = []
timeToSleep = 300
# End Hilfsvariablen Block
###############################################################################

###############################################################################
# def on_message(client, userdata, message):
# 
# Callback-Funktion, die reagiert, sobald eine Nachricht empfangen wurde. 
# Nur das Argument "message" findet weitere Verwendung
# 
# Part I
# Die Nachricht "message" wird dekodiert, und via JSON.loads in eine Python-Obeject 
# übertragen.
#
# Part II
# Steht im Datenstrom hinter config ein true so handelt es sich um eine
# Aufforderung zum update des ESPs. Ansonsten handelt es sich um Messwerte!
###############################################################################
def on_message(client, userdata, message):
  # Part I
  # Text decodieren um ihn richtig zu verstehen
  dec_message = str(message.payload.decode("utf-8"))
  
  # Damit ich sehe was passiert spaeter ausblenden
  # print(dec_message)  
  dicObj = json.loads(dec_message)
  
  # Part II sortieren
  if(dicObj["config"]):    
    updateESP(dicObj["espName"])
  else:
    decodeMessage(dicObj["sensors"])
# End def on_message(client, userdata, message):
###############################################################################


###############################################################################
# def updateESP(espName):
#
# @espName => Kennung des Esp's als String. Beispiel "esp001"
# 
# Aufrufe erfolgen aus transceiver() und on_message()
# 
# Abfrage an die Api des Webservers.
#
# Part II
# Der vordefinierten Abfrage URL_SENSOR_CONFIG wird die ESP-Kennung angehängt
# und in einer url gespeichert. Diese url nutzen wir zur Request-Anfrage. 
# 
# Part III 
# Bei erfolgreicher Anfrage 200 wird die Rückgabe in einem Python-Objekt
# gespeichert. Im weiteren Verlauf wird hier ein String mit vordefinierten
# Seperatoren aufgebaut.
# SensorName##isAktiv_intervall_positiveThreshold usw.
# ??????##?_???_????_????_????_????
# 
# Part IV
# der String sOrder wird via Mqtt versand
# 
# Part V
# Im Fehlerfall wir der update-Aufruf verworfen und es folgt eine Fehlermeldung
###############################################################################
def updateESP(espName):  
    
  # Part I
  # Order for Your self spaeter soll sich der Tranceiver auch updaten koennen
  if(espName == "transc"):
    pass

  # Part II
  url = URL_SENSOR_CONFIG + espName
  
  # Es ist nicht Klar, ob der Server läuft
  # Im Fehlerfall, schmeißt request eine 
  # requests.exceptions.RequestException  
  try:
    
    response = requests.get(url)
    
    # Status 200 => OK
    if(response.status_code == 200):
      
      # Part III
      jsonObj = json.loads(response.content)
        
      # Config Daten für den ESP: Für jeden Sensor einzeln aufgelöst 
      for item in jsonObj:        
                
        sOrder = item["name"] + "##"
          
        cAktiv = '0'
        if(item["isAktiv"]):
          cAktiv = '1'
        sOrder += cAktiv
          
        sTemp = str(item["intervall"]).zfill(3)      
        sOrder +='_' + sTemp
          
        sTemp =  str(item["positiveThreshold"]).zfill(4)
        sOrder += '_' + sTemp
          
        sTemp =  str(item["negativeThreshold"]).zfill(4)
        sOrder += '_' + sTemp

        # Zeitfenster an die ESP weiterleiten 
        sTemp =  str(item["timeWindowStart"]).zfill(5)
        sOrder += '_' + sTemp

        sTemp =  str(item["timeWindowEnd"]).zfill(5)
        sOrder += '_' + sTemp

        sTopic = "params/" + espName
        if PRINT_IT:
          print(sOrder)
        # Part IV
        MQTT_CLIENT.publish(sTopic, sOrder)
          
  except requests.exceptions.RequestException as error:
    # Part V
    print(espName + " update temporaer nicht moeglich \n")   
    print(error)   
# End def updateESP(espName):
###############################################################################

###############################################################################
# def decodeMessage(dec_messages):
# 
# @dec_messages = Liste mit Nachrichten-Objekten (Sensorwerte)
# 
# Der Aufruf erfolgt nur über on_message()
#
# Part I
# Hier geht eine liste mit Sensor-Messwerten ein. Dies durchlaeuft eine 
# Schleife ider der jedes item bearbeitet wird.
# 
# Part II
# jedes Item wird darau geprueft (isNewValue), ob sich um einen neuen Messwert,
# oder breits bekannten Messwert handelt. Der Rueckgabewert von isNewValue ist
# entweder die ID eines bereits bekannten Messwertes oder aber 0 für ein neuen
# Messwert.
# 
# Part III
# Auf grundlage der ID nimmt der Datensatz seinen weg, über sendNewValue(item)
# oder sendUpdateValue(item)
def decodeMessage(dec_messages):

  # Part I
  for item in dec_messages:
    
    # Part II
    iValueId = isNewValue(item)
    item['id'] = iValueId
    
    if(iValueId < 1):      
      sendNewValue(item)
    else:
      sendUpdateValue(item)
  return 
# End def decodeMessage(dec_messages):
###############################################################################

###############################################################################
# def isNewValue(data):
# 
# @data => ein einzelner Messwert
# @returnValue <= Id eines bereist bekannten Values oder 0
# 
# Aufruf erfolgt einzig von decode Message.
# 
# Ein neuer Wert landet in einer Liste, während eine bekannte Wert dessen ID
# zurück gibt.
# 
# Part I
# Der Sensor ist neu, dann auch der aktuelle Wert 
# Davon gehen erst einmal aus!
# Dann ist auch die ID = 0
# 
# Part II
# Wenn die Liste leer ist, kennt wir auch den Sensor nicht und dessen Wert
# nicht. Also keine Änderungen an isNewSenor und returnValue.
# 
# PartIII
# Ansonsten schauen wir über eine For-Schleife, ob es den Sensor bereits in der Liste
# gibt. Für den Fall ist isNewSensor = false.
# 
# Part IV
# Wenn auch der Wert bekannt ist setze ich hier die ValueId aus der Liste als
# Rueckgabewert ein. somit ist der Sensor und auch dessen Messwert bereits bekannt.
# 
# Part V
# Ist der Messwert neue, überschreibe ich den Messwert in der Liste und belasse
# den Rückgabe wert returnValue = 0 und kennzeichne damit den Messwert als neu. 
# 
# Part VI
# Ist der Sensor zuvor unbekannt, füge ich ihn der Liste hinzu.
def isNewValue(data): 
  
  # Part I
  isNewSensor = True  
  returnValue = 0
  
  # Part II
  if(len(valueList) == 0):
    print("valueList ist noch leer")
  else:
    for item in valueList:
      # Part III
      if(item.getName() == data["sensorName"]):
        # founded! so it's not newSensor for our SensorvalueList
        isNewSensor = False       
        
        # Part IV
        if item.getValue() == data["sensorValue"]:
          print("Alter Werte", item.getValue(), data["sensorValue"], " also alte Id:", item.getId())
          returnValue = item.getId()
        else:
          # Part V
          print("Neue Werte", item.getValue(), data["sensorValue"], " also alte Id:", item.getId())
          item.setValue((data["sensorValue"]))
        
        break
  # Part VI
  if(isNewSensor):        
    valueList.append(SensorValue(0, int(data["sensorValue"]), data["sensorName"]))
    
  return returnValue
# End def isNewValue(data):
###############################################################################

###############################################################################
# def sendNewValue(data):
#
# @data => ein einzelner Messwert
# 
# Aufruf erfolgt einzig von decode Message.
# 
# Part I
# die daten werden in das JSON Format gebracht und an die Wepapi URL_NEW_VALUE
# gesendet.
# 
# Part II
# Im Erfolgsfall 200 decodiere ich die Antwort und verfrachte ide Json-Daten in
# ein Python-Objekt.
# 
# Part III
# aus dem Python-Ojekt hole ich mir den Namen und ide Id, um dem Sensorwert in
# der Liste die neu generierte ID zu geben. Nun hat der letzte gespeicherte
# Wert dieses Sensosr eine ID und kann ein falls nötig ein update erfahren. 
# 
# Part IV
# Bei Fehleranwort vom server ungelich 200 verwerfe ich den an sendNewValue,
# übergebenen Wert und gebe die Fehlermeldung aus
# 
# Part IV
# Im allgemeinem Fehlerfall verwerfe ich den an sendNewValue, übergebenen Wert
# und gebe die Errormeldung aus
def sendNewValue(data):

  print("sendNewValue 1", data, " ist vom Typ: ", type(data))
  
  # Part I
  try:
    jsonData = json.dumps(data)
    # Content-Type to application/json
    response = requests.post(URL_NEW_VALUE, data=jsonData, headers=JSON_HEADER)

    if response.status_code == 200:
      # Part II
      dec_message = str(response.content.decode("utf-8"))
      obj = json.loads(dec_message)

      # Part III
      sensorName = obj["sensorName"]
      sensorId = obj["id"]

      for item in valueList:
        if item.getName() == sensorName:
          item.setId(sensorId)
          break
    else:
      # Part IV
      print(response.status_code)

  except requests.exceptions.RequestException as error:
    # Part V
    print("Datentranfer nicht moeglich \n")
    print(error)
# End def sendNewValue(data):
###############################################################################



###############################################################################
# def sendUpdateValue(data):
#
# @data => ein einzelner Messwert
# 
# Aufruf erfolgt einzig von decode Message.
# 
# Part I
# die daten werden in das JSON Format gebracht und an die Wepapi 
# URL_UPDATE_VALUE gesendet.
# 
# Part II
# Im Erfolgsfall 200 decodiere ich die Antwort und verfrachte ide Json-Daten in
# ein Python-Objekt.
# 
# Part III
# aus dem Python-Ojekt hole ich mir den Namen und die Id.Dies ist notwendig,
# weil der Webserver die Messung neu anlegt, falls er keine Messung der der
# übergeben id hatte. In diesem Fall muss ich die Wert in der Liste anpassen.
# 
# Part IV
# Bei Fehleranwort vom server ungelich 200 verwerfe ich den an sendNewValue,
# übergebenen Wert und gebe die Fehlermeldung aus
# 
# Part IV
# Im allgemeinem Fehlerfall verwerfe ich den an sendNewValue, übergebenen Wert
# und gebe die Errormeldung aus
def sendUpdateValue(data):

  print("updateNewValue", data, " ist vom Typ: ", type(data))
  
  # Past I
  try:
    jsonData = json.dumps(data)
    # Content-Type to application/json
    response = requests.put(URL_UPDATE_VALUE, data=jsonData, headers=JSON_HEADER)

    if response.status_code == 200:
      # Part II
      dec_message = str(response.content.decode("utf-8"))
      obj = json.loads(dec_message)
      
      # Part III
      sensorName = obj["sensorName"]
      
      sensorId = obj["id"]

      for item in valueList:
        if item.getName() == sensorName:
          item.setId(sensorId)
          break
    else:
      # Part IV
      print(response.status_code)

  except requests.exceptions.RequestException as error:
    # Part V
    print("Datentranfer nicht moeglich \n")
    print(error)
# End def sendUpdateValue(data):
###############################################################################



###############################################################################
# def transceiver():
# 
# Das Herzstück des Programms 
# 
# Part I
# Der Client zum Mqtt-Broker wird Konfiguriert,
# dem Client wird eine callback-Funktion zugewiesen,
# es wird eine Verbindung zum Mqtt-Server aufgebaut,
# der Client startet eine Endlosschleife
# und meldet sich für den Empfang bestimmter Überschriften an.
# In diesem Fall, für alle Sensor Daten der ESPs 001 und 002
#
# Part II
# Hier läuft nun eine Endlosschleife die solange eine mqtt Verbindung aktiv
# ist dafür sorgt, dass die beiden ESPs konfiguriert werden.
# timeToSleep ist mit 300 Sekunden vordefiniert.
def transceiver():
  # Part I ########
  MQTT_CLIENT.username_pw_set(MQTT_USER, MQTT_PASSWORD)
  MQTT_CLIENT.on_message = on_message
  MQTT_CLIENT.connect(MQTT_HOST)
  MQTT_CLIENT.loop_start()

  MQTT_CLIENT.subscribe(TOPIC)

  print("Transceiver aktiviert")
  
  # Part II #######
  while True:
    if(MQTT_CLIENT.is_connected):
      updateESP("esp001")
      updateESP("esp002")
      time.sleep(timeToSleep)
    else:
      MQTT_CLIENT.connect(MQTT_HOST)      
  
# End def transceiver():
###############################################################################



###############################################################################
# start the engine
transceiver()
#asyncio.run(transceiver())
###############################################################################
