import streamlit as st
import pandas as pd
import class_conexion.cls_conexion as cnn
import cls_process_auto as cmongo
import mongodb_backups.cls_mongodb_backups as back_e
import mongodb_latencias.cls_consultas_mongodb as latencia
import threading
from cryptography.fernet import Fernet
from decouple import config
import logging
from datetime import datetime
from ping3 import ping
import time
import subprocess
import nltk #procesamiento de lenguaje natural
nltk.download('punkt')
from nltk.tokenize import word_tokenize  #Separar el parrafo en tokens.

class cls_Update_Backups:
    def __init__(self):
        self.main()

    def main(self):
        now = datetime.now()
        mes_anio = now.strftime("%m-%Y")
        filename=f"logs/{mes_anio}.log"

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(threadName)s - %(processName)s - %(levelname)s - %(message)s')

        logger= logging.getLogger("Backups_principal")
        file_handler=logging.FileHandler(filename)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        def devicesSolarAdd():
            inst=cmongo.consultas_mongodb(None)
            df_final=inst.checkDevicesSolar()

            if df_final.empty:
                pass
            else:
                clave=Fernet.generate_key()
                f=Fernet(clave)
                passw_cred=f"{config('tacas_pass_1')}"
                user = config('tacas_user_1').encode()
                cod_user= f.encrypt(user)
                passw = passw_cred.encode()
                cod_passw= f.encrypt(passw)

                for i in range(0,len(df_final)):
                    fila_index= df_final.loc[i,:]
                    if str(fila_index["MARCA"])=="Alcatel":
                        type_device="alcatel_sros"
                    elif str(fila_index["MARCA"])=="Cisco":
                        type_device="cisco_ios"
                    elif str(fila_index["MARCA"])=="HUAWEI Technology Co.,Ltd":
                        type_device="huawei"
                    elif str(fila_index["MARCA"])=="Juniper Networks, Inc.":
                        type_device="juniper"
                    elif str(fila_index["MARCA"])=="MikroTik":
                        type_device="mikrotik_routeros"
                    elif str(fila_index["MARCA"])=="Beijing Raisecom Scientific & Technology Development Co., Ltd.":
                        type_device="cisco_ios_telnet"
                    elif str(fila_index["MARCA"])=="Shenzhen Zhongxing Telecom Co.,ltd.":
                        type_device="zte_zxros_telnet"
                    dicDatosMongoDB={"NOMBRE":str(fila_index['NOMBRE']),
                            "IP":str(fila_index["IP"]),
                            "REGION":str(fila_index["REGION"]),
                            "PAIS":str(fila_index["PAIS"]),
                            "MARCA":str(fila_index["MARCA"]),
                            "CLIENTE":str(fila_index["CLIENTE"]),
                            "RED":str(fila_index["RED"]),
                            "ESTADO":True,
                            "USUARIO":cod_user,
                            "APODO":clave,
                            "PASSWORD":cod_passw,
                            "DEVICE":type_device,
                            "PORT":None}
                    inst=cmongo.consultas_mongodb(dicDatosMongoDB)
                    inst.addDevicesSolar()   
            #devicesUpdateSolar
            inst_c=cmongo.consultas_mongodb(None)
            inst_c.updataMongodb()

        def backupsUpdate():
            global msjOutput
            inst_act=cmongo.consultas_mongodb(None)
            dia_now, mes_now, anio_now, date_ac=inst_act.fecha_actual()
            db_a= inst_act.instancia_cnn_dateTime()
           
            date = now.strftime("%d-%m-%Y")
            logger.info(f"-----FECHA {date}-----")
            q=1
            while q<=3:
                logger.info(f"-----CICLO {q}-----")
                db_a.drop_collection('Antiguos')

                inst_back=back_e.consultas_backups_equipos(None,None,None)
                db_b = inst_back.instancia_cnn_backups()
                colecciones=db_b.list_collection_names()
                import datetime
                for coleccion in colecciones:
                    coll= db_b[coleccion]
                    cantidad_doc=coll.count_documents({})
                    if cantidad_doc==0:
                        pass
                    else:
                        for documento in coll.find({}).batch_size(5):
                            date_b=str(documento.get("FECHA"))
                            index = date_b.index('-')
                            dia_b=date_b[0:index]
                            date_b= date_b[index+1:]
                            index = date_b.index('-')
                            mes_b=date_b[0:index]
                            date_b= date_b[index+1:]
                            anio_b=date_b[0:4]
                            fecha_b=dia_b+'-'+mes_b+'-'+anio_b
                            if fecha_b==date_ac:
                                pass
                            else:
                                if documento.get("RED")=="Access":
                                    inst=cmongo.consultas_mongodb({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                    msjOutput=inst.updateBackups()
                                    logger.info(msjOutput)
                                elif documento.get("RED")=="Aggregation":
                                    inst=cmongo.consultas_mongodb({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                    msjOutput=inst.updateBackups()
                                    logger.info(msjOutput)
                                elif documento.get("RED")=="Backbone":
                                    inst=cmongo.consultas_mongodb({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                    msjOutput=inst.updateBackups()
                                    logger.info(msjOutput)
                                    
                                elif documento.get("RED")=="Customer":
                                    inst=cmongo.consultas_mongodb({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                    a = datetime.datetime(anio_now, mes_now,dia_now) 
                                    b = datetime.datetime(int(anio_b), int(mes_b), int(dia_b))
                                    c = a-b  
                                    if c>=datetime.timedelta(days=7):
                                        msjOutput=inst.updateBackups()
                                        logger.info(msjOutput)
                                    else:
                                        pass
                                elif documento.get("RED")=="Management":  
                                    inst=cmongo.consultas_mongodb({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})              
                                    a = datetime.datetime(anio_now, mes_now,dia_now) 
                                    b = datetime.datetime(int(anio_b), int(mes_b), int(dia_b))
                                    c = a-b  
                                    if c>=datetime.timedelta(days=7):
                                        msjOutput=inst.updateBackups()
                                        logger.info(msjOutput)
                                    else:
                                        pass
                                else:
                                    inst=cmongo.consultas_mongodb({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                    msjOutput=inst.updateBackups()
                                    logger.info(msjOutput) 
                if q==3:   
                    msjOutput=inst_act.errorUpdateBackups()
                    logger.info(msjOutput)
                    break
                else:
                    pass 
                q +=1   
            inst_act.updateHistoric()   
     
        def timerOnLatencias():
            instLat=latencia.consultas_latencia(None)
            instLat.deleteDatosLatenc()

            for k in range(0,5):
                for hora in range(1,9):
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
                                                "LATENCIA(M)":None,
                                                "FRACCION":hora}
                                    instInsertar=latencia.consultas_latencia(dicDatosLatencia)
                                    instInsertar.insertarDatosLatenc()
                                elif ping(url) ==None:
                                    dicDatosLatencia={"NOMBRE":dfUrls.iloc[0,1],
                                                "URL":dfUrls.iloc[0,2],
                                                "PING":"Timeout",
                                                "PPERDIDOS":None,
                                                "LATENCIA(M)":None,
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
                    time.sleep(900)#15min-900s                   
                instLat=latencia.consultas_latencia(None)
                instLat.deleteDatosLatenc()

        devicesSolarAdd()
        backupsUpdate()
        timerOnLatencias()
        
cls_Update_Backups()