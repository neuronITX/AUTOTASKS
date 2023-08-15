import class_conexion.cls_conexion as cnn
import mongodb_equiposred.cls_equipos_red as equiposRed
import mongodb_backups.cls_mongodb_backups as back_e
import mongodb_backups.cls_mongodb_error as error_e
import mongodb_backups.cls_mongodb_historial as historial_e 

import pandas as pd
import csv
from cryptography.fernet import Fernet
from decouple import config
import logging
from datetime import datetime
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

now = datetime.now()
mes_anio = now.strftime("%m-%Y")
filename=f"/home/AUTOTASKS/logs/{mes_anio}.log"

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(threadName)s - %(processName)s - %(levelname)s - %(message)s')

logger= logging.getLogger("Backups_principal")
file_handler=logging.FileHandler(filename)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

def instancia_cnn_tnl_equiposTuneles():
    inst=cnn.conexion()
    session=inst.conexion_mongodb()
    db = session['tnl_equiposTuneles']
    return db 
    
def fecha_actual():
    now=datetime.now()
    date = now.strftime("%d-%m-%Y")
    date_ac = now.strftime("%d-%m-%Y")

    index = date.index('-')
    dia_now=int(date[0:index])
    
    date_now= date[index+1:]
    index = date_now.index('-')
    mes_now=int(date_now[0:index])
    
    date_now= date_now[index+1:]
    anio_now=int(date_now[0:4])   
    return dia_now, mes_now, anio_now, date_ac

#Verificacion de la CANTIDAD de dispositivos que faltan en mongo y estan en solar
def checkDevicesSolar():
    inst_s=cnn.conexion()
    dfSolar= inst_s.conexion_solar()        
    marcas = dfSolar['Vendor'].drop_duplicates().tolist()
    l_faltan=list()

    for i in range(0,len(marcas)):
        solar_df = dfSolar.groupby('Vendor').get_group(str(marcas[i]))#solar
        solar_df.reset_index(inplace=True, drop=True)
        
        inst=equiposRed.cls_consultas_equipos_red({"MARCA":str(marcas[i])})
        dfEquiposRed=inst.contenido_equiposRed()

        if dfEquiposRed.empty:
            if len(solar_df)>0:
                for k in range(0,len(solar_df)):
                    l_faltan.append([solar_df.iloc[k,1],solar_df.iloc[k,2],solar_df.iloc[k,7],solar_df.iloc[k,6],solar_df.iloc[k,3],
                        solar_df.iloc[k,8],solar_df.iloc[k,0]])#nombre,ip,region,pais,marca,cliente,red
            else:
                pass
        elif len(dfEquiposRed)>0:
            ip_mongo_marca=list(dfEquiposRed["IP"])
            ip_solar=list(solar_df["IP_Address"])
            for k in range(0,len(solar_df)):
                if ip_solar[k] in ip_mongo_marca:
                    pass
                else:
                    l_faltan.append([solar_df.iloc[k,1],solar_df.iloc[k,2],solar_df.iloc[k,7],solar_df.iloc[k,6],solar_df.iloc[k,3],
                    solar_df.iloc[k,8],solar_df.iloc[k,0]])#nombre,ip,region,pais,marca,cliente,red
                
    col = ["NOMBRE","IP","REGION","PAIS","MARCA","CLIENTE","RED"]
    df_faltan= pd.DataFrame(l_faltan, columns=col)
    df_faltan.reset_index(inplace=True, drop=True)
    return df_faltan

#Agregar dispositivos nuevos, generar backups
def addDevicesSolar(dicDatosMongoDB):
    marca=dicDatosMongoDB.get("MARCA")
    nombre=dicDatosMongoDB.get("NOMBRE")
    inst_back=back_e.consultas_backups_equipos(marca,nombre,dicDatosMongoDB)
    inst_error=error_e.consultas_error_equipos(marca,nombre,dicDatosMongoDB)
    inst_red=equiposRed.cls_consultas_equipos_red(dicDatosMongoDB)#agregar

    dicDatosMongoDB.setdefault("ORIGEN","addDevicesSolar")
    date,output,mss,msjOutput = inst_back.generar_backup()
    dicDatosMongoDB.pop("ORIGEN")
    if mss==None:
        lista_words=['nocadmin','admin','cwlmsmed','readonly','gestion',nombre]
        for word in lista_words:    
            if word in output:
                dicDatosMongoDB.setdefault("BACKUP",output)
                dicDatosMongoDB.setdefault("FECHA",date)
                dicDatosMongoDB.setdefault("BACKUPS",True)
                inst_red.insertar_equipo()
                inst_back.insertar_backup()
                dicDatosMongoDB.pop("BACKUP")
                dicDatosMongoDB.pop("FECHA")

        lista_words=['Invalid input detected','CLI Command not allowed for this user']
        for word in lista_words:    
            if word in output:
                dicDatosMongoDB.setdefault("INSERT_ERROR","Invalid input detected")
                dicDatosMongoDB.setdefault("FECHA",date)
                dicDatosMongoDB.setdefault("BACKUPS",False)
                inst_red.insertar_equipo()
                inst_error.insertar_error()
                dicDatosMongoDB.pop("INSERT_ERROR")
                dicDatosMongoDB.pop("FECHA")

        dicDatosMongoDB.setdefault("BACKUP",output)
        dicDatosMongoDB.setdefault("FECHA",date)
        dicDatosMongoDB.setdefault("BACKUPS",True)
        inst_red.insertar_equipo()
        inst_back.insertar_backup()
        dicDatosMongoDB.pop("BACKUP")
        dicDatosMongoDB.pop("FECHA")
    else:
        dicDatosMongoDB.setdefault("INSERT_ERROR",mss)
        dicDatosMongoDB.setdefault("FECHA",date)
        dicDatosMongoDB.setdefault("BACKUPS",False)
        inst_error.insertar_error()
        inst_red.insertar_equipo()
        dicDatosMongoDB.pop("INSERT_ERROR")
        dicDatosMongoDB.pop("FECHA")

#Actualizacion de backups
def updateBackups(dicDatosMongoDB):
    marca=dicDatosMongoDB.get("MARCA")
    ip=dicDatosMongoDB.get("IP")

    inst_equiposRed=equiposRed.cls_consultas_equipos_red({"IP":ip,"MARCA":marca})
    mss,documento=inst_equiposRed.buscar_equipo()

    if documento==None:
        return mss
    else:
        dicUpdateBackups={"IP":documento.get("IP"),"MARCA":marca,"ORIGEN":"updateBackups"}
        inst_back=back_e.consultas_backups_equipos(documento.get('MARCA'),documento.get('NOMBRE'),dicUpdateBackups)
        date,output,mss,msjOutput = inst_back.generar_backup()
        if mss==None:
            lista_words=['nocadmin','admin','cwlmsmed','readonly','gestion', 'internexa',str(documento.get("NOMBRE"))]
            for word in lista_words:    
                if word in output:
                    dicUpdateBackups.setdefault("FECHA",date)
                    inst_his=historial_e.consultas_historial_equipos(documento.get('MARCA'),documento.get("NOMBRE"),dicUpdateBackups)
                    mjs=inst_his.insertar_histo()
                    dicUpdateBackups.setdefault("BACKUP",output)
                    mjs=inst_back.update_backup()
                    break
                else:
                    pass
        else:
            marcaDev=str(documento.get("MARCA"))
            modMarca = marcaDev.replace('"', "")
            modMarca = modMarca.replace(",", "")
            datos = [
            [documento.get("NOMBRE"),documento.get("IP"),documento.get("REGION"),
             documento.get("PAIS"),modMarca,documento.get("CLIENTE"),
             date,mss]]

            with open('/home/AUTOTASKS/sinActualizar.txt', 'a', newline='') as archivo:
                escritor_csv = csv.writer(archivo)
                escritor_csv.writerows(datos)
        return msjOutput

def errorUpdateBackups():
    date_ac = now.strftime("%d-%m-%Y")
    try:               
        filas = []
        with open('/home/AUTOTASKS/sinActualizar.txt', 'r') as archivo:
            for linea in archivo:
                fila = linea.strip().split(',')
                filas.append(fila)
        
        if not filas:
            msjOutput="-----Archivo vacio-----"
        else:
            dfLatencias = pd.DataFrame(filas, columns=['NOMBRE','IP', 'REGION', 'PAIS','MARCA','CLIENTE','FECHA','ERROR'])
            """Excel"""
            writer = pd.ExcelWriter(f"no_actualizados.xlsx")
            dfLatencias.to_excel(writer, sheet_name="devices", index=False)
            writer.save() 

            """Correo"""
            subject = f"Dispositivos no actualizados {date_ac}"
            body = f"Archivo con los equipos que no se actualizaron el día {date_ac}"
            sender_email = config('sender_email')
            receiver_email = config('receiver_email')
            password =config('email_password')

            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = receiver_email
            message["Subject"] = subject
            message["Bcc"] = receiver_email  
            message.attach(MIMEText(body, "plain"))
            filename = r"no_actualizados.xlsx"
            with open(filename, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            message.attach(part)
            text = message.as_string()
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, text)

            msjOutput="-----Correo enviado-----"
        return msjOutput
    
    except FileNotFoundError:
            msjOutput="-----Archivo no existe-----"#archivo no existe
            return msjOutput
    except IOError:
        msjOutput="-----Error al leer los datos-----"
        return msjOutput

#Actualizacion y limpieza de los datos
def updataMongodb():
    inst_c=cnn.conexion()
    dfSolar=inst_c.conexion_solar()#df solar
    ip_add=list(dfSolar["IP_Address"])#solar

    inst_red=equiposRed.cls_consultas_equipos_red(None)
    db=inst_red.instancia_cnn_equipos_red()
    colecciones=db.list_collection_names()
    for coleccion in colecciones:
        coll= db[coleccion]
        for documento in coll.find({}):
            if str(documento.get("IP")) in ip_add:#solar
                index=ip_add.index(str(documento.get("IP")))
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"NOMBRE": str(dfSolar.iloc[index,1])}})
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"REGION": str(dfSolar.iloc[index,7])}})
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"PAIS": str(dfSolar.iloc[index,6])}})
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"MARCA": str(dfSolar.iloc[index,3])}})
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"CLIENTE": str(dfSolar.iloc[index,8])}})
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"RED": str(dfSolar.iloc[index,0])}})
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"ESTADO": True}})
            else:
                coll.update_one({"IP": str(documento.get("IP"))},{"$set":{"ESTADO": False}})
                coll.delete_one({"IP":str(documento.get("IP"))})

    inst_error=error_e.consultas_error_equipos(None,None,None)
    db_e= inst_error.instancia_cnn_error()
    colecciones_e=db_e.list_collection_names()
    for coleccion in colecciones_e:
        coll_e= db_e[coleccion]
        for documento in coll_e.find({}):
            if str(documento.get("IP")) in ip_add:#solar
                index=ip_add.index(str(documento.get("IP")))
                coll_e.update_one({"IP": str(documento.get("IP"))},{"$set":{"NOMBRE": str(dfSolar.iloc[index,1])}})
                coll_e.update_one({"IP": str(documento.get("IP"))},{"$set":{"PAIS": str(dfSolar.iloc[index,6])}})
                coll_e.update_one({"IP": str(documento.get("IP"))},{"$set":{"MARCA": str(dfSolar.iloc[index,3])}})
                coll_e.update_one({"IP": str(documento.get("IP"))},{"$set":{"CLIENTE": str(dfSolar.iloc[index,8])}})
                coll_e.update_one({"IP": str(documento.get("IP"))},{"$set":{"RED": str(dfSolar.iloc[index,0])}})
            else:
                coll_e.update_one({"IP": str(documento.get("IP"))},{"$set":{"ESTADO": False}})
                coll_e.delete_one({"IP":str(documento.get("IP"))})

    inst_b=back_e.consultas_backups_equipos(None,None,None)
    db_b= inst_b.instancia_cnn_backups()
    colecciones_b=db_b.list_collection_names()
    for coleccion in colecciones_b:
        coll_b= db_b[coleccion]
        for documento in coll_b.find({}):
            if str(documento.get("IP")) in ip_add:#solar
                index=ip_add.index(str(documento.get("IP")))
                coll_b.update_one({"IP": str(documento.get("IP"))},{"$set":{"NOMBRE": str(dfSolar.iloc[index,1])}})
                coll_b.update_one({"IP": str(documento.get("IP"))},{"$set":{"PAIS": str(dfSolar.iloc[index,6])}})
                coll_b.update_one({"IP": str(documento.get("IP"))},{"$set":{"MARCA": str(dfSolar.iloc[index,3])}})
                coll_b.update_one({"IP": str(documento.get("IP"))},{"$set":{"CLIENTE": str(dfSolar.iloc[index,8])}})
                coll_b.update_one({"IP": str(documento.get("IP"))},{"$set":{"RED": str(dfSolar.iloc[index,0])}})
            else:
                coll_b.update_one({"IP": str(documento.get("IP"))},{"$set":{"ESTADO": False}})
                coll_b.delete_one({"IP":str(documento.get("IP"))})

    db_t=instancia_cnn_tnl_equiposTuneles()
    colecciones_t=db_t.list_collection_names()
    for coleccion in colecciones_t:
        coll_t= db_t[coleccion]
        for documento in coll_t.find({}):
            if str(documento.get("IP")) in ip_add:#solar
                index=ip_add.index(str(documento.get("IP")))
                coll_t.update_one({"IP": str(documento.get("IP"))},{"$set":{"NOMBRE": str(dfSolar.iloc[index,1])}})
                coll_t.update_one({"IP": str(documento.get("IP"))},{"$set":{"PAIS": str(dfSolar.iloc[index,6])}})
                coll_t.update_one({"IP": str(documento.get("IP"))},{"$set":{"MARCA": str(dfSolar.iloc[index,3])}})
            else:
                coll_t.delete_one({"IP":str(documento.get("IP"))})

#Actualizacion historial y eliminar documentos
def updateHistoric():
    listMarcas=["Alcatel","Cisco","HUAWEI Technology Co.,Ltd","Juniper Networks, Inc.",
                "Beijing Raisecom Scientific & Technology Development Co., Ltd.",
                "Shenzhen Zhongxing Telecom Co.,ltd."]
    #Agregar MikroTik
    for vendor in listMarcas:
        inst=historial_e .consultas_historial_equipos(str(vendor),None,None)
        db=inst.instancia_cnn_historial()
        colecciones=db.list_collection_names()
    
        #Eliminar las carpetas de dispositivos inactivos
        inst_c=cnn.conexion()
        dfSolar=inst_c.conexion_solar()#df solar
        nombreDevice=list(dfSolar["SysName"])#solar

        for coleccion in colecciones:
            if coleccion in nombreDevice:
                #Eliminar documentos de los dispositivos que tiene mas de 720 doc
                coll=db[str(coleccion)]
                countColl=coll.count_documents({})
                if countColl>720:
                    deleteCount=countColl-720
                    i=0
                    for documento in coll.find({}):
                        coll.delete_one({"IP":documento.get("IP")}) 
                        i=i+1
                        if i==deleteCount:
                            break
                        else:
                            pass
                else:
                    pass
            else:
                #Dejar el ultimo backup de historial de un dispositvo eliminado
                coll=db[str(coleccion)]
                countColl=coll.count_documents({}) 
                if countColl==1:
                    pass
                else:
                    countDelete=countColl-1
                    i=0
                    for documento in coll.find({}):
                        coll.delete_one({"IP":documento.get("IP")}) 
                        i=i+1
                        if i==countDelete:
                            break
                        else:
                            pass


#PROCESOS PRINCIPALES
def devicesSolarAdd():
    logger.info(f"-----Actualizando informacion equipos-----")
    df_final=checkDevicesSolar()

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
            addDevicesSolar(dicDatosMongoDB)   
    updataMongodb()

def backupsUpdate():
    logger.info(f"-----Actualizando backups-----")
    global msjOutput
    dia_now, mes_now, anio_now, date_ac=fecha_actual()
    logger.info(f"-----FECHA {date_ac}-----")
    q=1
    while q<=3:
        logger.info(f"-----CICLO {q}-----")
        with open('/home/AUTOTASKS/sinActualizar.txt', 'w') as archivo_txt:
            archivo_txt.write('')

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
                            msjOutput=updateBackups({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                            logger.info(msjOutput)
                        elif documento.get("RED")=="Aggregation":
                            msjOutput=updateBackups({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                            logger.info(msjOutput)
                        elif documento.get("RED")=="Backbone":
                            msjOutput=updateBackups({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                            logger.info(msjOutput)
                            
                        elif documento.get("RED")=="Customer":
                            a = datetime.datetime(anio_now, mes_now,dia_now) 
                            b = datetime.datetime(int(anio_b), int(mes_b), int(dia_b))
                            c = a-b  
                            if c>=datetime.timedelta(days=7):
                                msjOutput=updateBackups({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                logger.info(msjOutput)
                            else:
                                pass
                        elif documento.get("RED")=="Management":              
                            a = datetime.datetime(anio_now, mes_now,dia_now) 
                            b = datetime.datetime(int(anio_b), int(mes_b), int(dia_b))
                            c = a-b  
                            if c>=datetime.timedelta(days=7):
                                msjOutput=updateBackups({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                                logger.info(msjOutput)
                            else:
                                pass
                        else:
                            msjOutput=updateBackups({"IP":documento.get("IP"),"MARCA":documento.get("MARCA")})
                            logger.info(msjOutput) 
        if q==3:   
            msjOutput=errorUpdateBackups()
            logger.info(msjOutput)
            break
        else:
            pass 
        q +=1   
    updateHistoric() 

devicesSolarAdd()
backupsUpdate()       