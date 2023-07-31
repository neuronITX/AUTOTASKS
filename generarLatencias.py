import mongodb_latencias.cls_mongodb_latencias as latencia
import pandas as pd
import logging
from datetime import datetime
from ping3 import ping
import time
import subprocess
import nltk #procesamiento de lenguaje natural
nltk.download('punkt')
from nltk.tokenize import word_tokenize  #Separar el parrafo en tokens.

now = datetime.now()
mes_anio = now.strftime("%m-%Y")
filename=f"logs/{mes_anio}-Latencias.log"

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(threadName)s - %(processName)s - %(levelname)s - %(message)s')

logger= logging.getLogger("Latencias_principal")
file_handler=logging.FileHandler(filename)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

def timerOnLatencias():
    logger.info(f"-----Inicio de latencias-----")
    date_ac = now.strftime("%d-%m-%Y")
    with open('datosLatencias.txt', 'w') as archivo_txt:
        archivo_txt.write('')
    for hora in range(1,289):
        date_now = now.strftime("%d-%m-%Y")
        if date_now!=date_ac:
            break
        else:
            instLat=latencia.consultas_latencia(None)
            db=instLat.instancia_cnn_ltcUrls()
            listaColl=db.list_collection_names()
            for nameUrl in listaColl:
                coll=db[nameUrl]
                num=coll.count_documents({})
                if num==0:
                    pass
                else:
                    try:
                        all_device= list(coll.find())
                        dfUrls= pd.DataFrame(all_device)
                        url=str(dfUrls.iloc[0,2])       
                        if ping(url) ==False:
                            dicDatosLatencia={"NOMBRE":dfUrls.iloc[0,1],
                                        "URL":dfUrls.iloc[0,2],
                                        "PING":"Fail",
                                        "PPERDIDOS":None,
                                        "LATENCIA(M)":0,
                                        "FRACCION":hora}
                            instInsertar=latencia.consultas_latencia(dicDatosLatencia)
                            instInsertar.insertarDatosLatenc()
                        elif ping(url) ==None:
                            dicDatosLatencia={"NOMBRE":dfUrls.iloc[0,1],
                                        "URL":dfUrls.iloc[0,2],
                                        "PING":"Timeout",
                                        "PPERDIDOS":None,
                                        "LATENCIA(M)":0,
                                        "FRACCION":hora}
                            instInsertar=latencia.consultas_latencia(dicDatosLatencia)
                            instInsertar.insertarDatosLatenc()
                        else:
                            try:
                                response = subprocess.run(["ping","-c","10","-q",url],
                                                    capture_output=True,
                                                    text=True)
                                nmap_lines = response.stdout.splitlines()

                                pscPquetes= str(nmap_lines[3])
                                pscPquetes=pscPquetes.replace(",", "")
                                pscPquetes=pscPquetes.replace("%", "")
                                tokenPquetes = word_tokenize(pscPquetes)
                                pakPerdidos= tokenPquetes[5]

                                pscEstadisticas= str(nmap_lines[4])
                                pscEstadisticas=pscEstadisticas.replace("/", " ")
                                pscEstadisticas=pscEstadisticas.replace("=", "")
                                tokenEstadisticas = word_tokenize(pscEstadisticas)
                                latMed=tokenEstadisticas[6]

                                instInsertar=latencia.consultas_latencia({"NOMBRE":dfUrls.iloc[0,1],"URL":dfUrls.iloc[0,2],"PING":True,
                                        "PPERDIDOS":pakPerdidos,"LATENCIA(M)":latMed,"FRACCION":hora})
                                instInsertar.insertarDatosLatenc()     
                            except Exception as error:
                                logger.info(f"-----ERROR LATENCIAS {error}-----")
                    except Exception as error:
                        logger.info(f"-----ERROR LATENCIAS {error}-----")
            time.sleep(300)#5min 
    instLat.insertarHistLat()                

timerOnLatencias()