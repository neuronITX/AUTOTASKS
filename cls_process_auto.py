import pandas as pd
from decouple import config
from datetime import datetime
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import class_conexion.cls_conexion as cnn
import mongodb_equiposred.cls_equipos_red as equiposRed
import mongodb_backups.cls_mongodb_backups as back_e
import mongodb_backups.cls_mongodb_error as error_e
import mongodb_backups.cls_mongodb_historial as historial_e 
import mongodb_tuneles.cls_mongodb_loopback as loopback

class consultas_mongodb:
    def __init__(self, dicDatosMongoDB):
        self.dicDatosMongoDB= dicDatosMongoDB 

    def instancia_cnn_dateTime(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session["dateTime"]
        return db  

    def fecha_actual(self):
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
    def checkDevicesSolar(self):
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
    def addDevicesSolar(self):
        marca=self.dicDatosMongoDB.get("MARCA")
        nombre=self.dicDatosMongoDB.get("NOMBRE")
        inst_back=back_e.consultas_backups_equipos(marca,nombre,self.dicDatosMongoDB)
        inst_error=error_e.consultas_error_equipos(marca,nombre,self.dicDatosMongoDB)
        inst_red=equiposRed.cls_consultas_equipos_red(self.dicDatosMongoDB)#agregar

        self.dicDatosMongoDB.setdefault("ORIGEN","addDevicesSolar")
        date,output,mss,msjOutput = inst_back.generar_backup()
        self.dicDatosMongoDB.pop("ORIGEN")
        if mss==None:
            lista_words=['nocadmin','admin','cwlmsmed','readonly','gestion',nombre]
            for word in lista_words:    
                if word in output:
                    self.dicDatosMongoDB.setdefault("BACKUP",output)
                    self.dicDatosMongoDB.setdefault("FECHA",date)
                    self.dicDatosMongoDB.setdefault("BACKUPS",True)
                    inst_red.insertar_equipo()
                    inst_back.insertar_backup()
                    self.dicDatosMongoDB.pop("BACKUP")
                    self.dicDatosMongoDB.pop("FECHA")

            lista_words=['Invalid input detected','CLI Command not allowed for this user']
            for word in lista_words:    
                if word in output:
                    self.dicDatosMongoDB.setdefault("INSERT_ERROR","Invalid input detected")
                    self.dicDatosMongoDB.setdefault("FECHA",date)
                    self.dicDatosMongoDB.setdefault("BACKUPS",False)
                    inst_red.insertar_equipo()
                    inst_error.insertar_error()
                    self.dicDatosMongoDB.pop("INSERT_ERROR")
                    self.dicDatosMongoDB.pop("FECHA")

            self.dicDatosMongoDB.setdefault("BACKUP",output)
            self.dicDatosMongoDB.setdefault("FECHA",date)
            self.dicDatosMongoDB.setdefault("BACKUPS",True)
            inst_red.insertar_equipo()
            inst_back.insertar_backup()
            self.dicDatosMongoDB.pop("BACKUP")
            self.dicDatosMongoDB.pop("FECHA")
        else:
            self.dicDatosMongoDB.setdefault("INSERT_ERROR",mss)
            self.dicDatosMongoDB.setdefault("FECHA",date)
            self.dicDatosMongoDB.setdefault("BACKUPS",False)
            inst_error.insertar_error()
            inst_red.insertar_equipo()
            self.dicDatosMongoDB.pop("INSERT_ERROR")
            self.dicDatosMongoDB.pop("FECHA")

#Actualizacion de backups
#IP,MARCA
    def updateBackups(self):
        db_a= self.instancia_cnn_dateTime()
        marca=self.dicDatosMongoDB.get("MARCA")
        ip=self.dicDatosMongoDB.get("IP")

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
                coll_a= db_a['Antiguos']
                coll_a.insert_one({
                "NOMBRE": documento.get("NOMBRE"),
                "IP": documento.get("IP"),
                "REGION": documento.get("REGION"), 
                "PAIS": documento.get("PAIS"), 
                "MARCA": documento.get("MARCA"), 
                "CLIENTE": documento.get("CLIENTE"), 
                "FECHA": date,
                "ERROR": mss})
            return msjOutput
    
    def errorUpdateBackups(self):
        dia_now, mes_now, anio_now, date_ac=self.fecha_actual()
        db= self.instancia_cnn_dateTime()
        coll=db["Antiguos"]

        num=coll.count_documents({})
        if num==0:
            df_coll= pd.DataFrame()
            writer = pd.ExcelWriter(f"no_actualizados.xlsx")
            df_coll.to_excel(writer, sheet_name="devices", index=False)
            writer.save() 
            pass
        else:
            all_device= list(coll.find())
            df_coll= pd.DataFrame(all_device)
            df_coll=df_coll.drop(['_id'], axis=1)
            df_coll=df_coll.fillna('Ninguno')

            """Excel"""
            writer = pd.ExcelWriter(f"no_actualizados.xlsx")
            df_coll.to_excel(writer, sheet_name="devices", index=False)
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
 
 #Actualizacion y limpieza de los datos
    def updataMongodb(self):
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

        inst_loopback=loopback.consultas_loopback(None)
        db_t=inst_loopback.instancia_cnn_tnl_equiposTuneles()
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
    def updateHistoric(self):
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
        