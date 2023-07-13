import pandas as pd
import class_conexion.cls_conexion as cnn
import mongodb_backups.cls_generar_backups as bkp
import mongodb_equiposred.cls_equipos_red as equiposRed
from cryptography.fernet import Fernet

class consultas_backups_equipos:
    def __init__(self,marca,nombre,dic_info):
        self.marca = marca   
        self.nombre = nombre 
        self.dic_info= dic_info  
    
    def colecciones_marcas(self):
        if self.marca=="HUAWEI Technology Co.,Ltd":
            marca="HUAWEI"
        elif self.marca=="Juniper Networks, Inc.":
            marca="Juniper"
        elif self.marca=="Beijing Raisecom Scientific & Technology Development Co., Ltd.":
            marca="Raisecom"
        elif self.marca=="Shenzhen Zhongxing Telecom Co.,ltd.":
            marca="Telecom"
        else:
            marca=self.marca

        return marca    

    def instancia_cnn_backups(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['bck_equipos']
        return db

    def coll_mongodb_backup(self):
        db = self.instancia_cnn_backups()
        marca=self.colecciones_marcas()

        coll = db[marca]
        num=coll.count_documents({})
        if num==0:
            df_coll= pd.DataFrame()
        else:
            all_device= list(coll.find())
            df_coll= pd.DataFrame(all_device)
            df_coll=df_coll.drop(['_id'], axis=1)
            df_coll=df_coll.drop(['BACKUP'], axis=1)
            df_coll.rename(columns={'RED':'ROL'},inplace=True)
            df_coll=df_coll.fillna('Ninguno')    
        return df_coll

    def df_database(self):
        self.marca="Alcatel"
        df_alcatel=self.coll_mongodb_backup()
        self.marca="Cisco"
        df_cisco=self.coll_mongodb_backup()
        self.marca="HUAWEI"
        df_huawei=self.coll_mongodb_backup()
        self.marca="Juniper"
        df_juniper=self.coll_mongodb_backup()
        self.marca="Raisecom"
        df_raisecom=self.coll_mongodb_backup()
        self.marca="MikroTik"
        df_mikrotik=self.coll_mongodb_backup()
        self.marca="Telecom"
        df_telecom=self.coll_mongodb_backup()

        df=pd.concat([df_alcatel,df_cisco,df_huawei,df_juniper,df_raisecom,df_mikrotik,df_telecom])
        df.reset_index(inplace=True, drop=True)
        return df

    def db_Alcatel(self):
        self.marca="Alcatel"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel

    def db_Cisco(self):
        self.marca="Cisco"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel

    def db_Huawei(self):
        self.marca="HUAWEI"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel

    def db_Juniper(self):
        self.marca="Juniper"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel

    def db_Raisecom(self):
        self.marca="Raisecom"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel
    
    def db_MikroTik(self):
        self.marca="MikroTik"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel

    def db_Telecom(self):
        self.marca="Telecom"
        df_alcatel=self.coll_mongodb_backup()
        return df_alcatel

    def buscar_backup(self):
        db = self.instancia_cnn_backups()
        marca=self.colecciones_marcas()    
        coll = db[marca]
        doc_pym= coll.find({"IP":self.dic_info.get("IP")}).limit(1)  
        for doc in doc_pym:
            if doc==None:
                mss=f"No existe el dispositivo: {self.nombre}"
            else:
                mss=None              
   
        return mss,doc

    def generar_backup(self):
        marca=self.colecciones_marcas()

        if self.dic_info.get("ORIGEN")=="updateBackups":
            inst=equiposRed.cls_consultas_equipos_red({"MARCA":str(self.dic_info.get("MARCA")),"IP":str(self.dic_info.get("IP"))})
            mss,doc=inst.buscar_equipo()
            clave=doc.get("APODO")
            f=Fernet(clave)
            cod_user=doc.get("USUARIO")
            user= f.decrypt(cod_user)
            cod_passw=doc.get("PASSWORD")
            passw= f.decrypt(cod_passw)

            dicDatosBackup={"IP":doc.get("IP"), 
                            "NOMBRE":doc.get("NOMBRE"),
                            "MARCA":str(marca),
                            "USER":str(user,'utf-8'),
                            "PASSW":str(passw,'utf-8'),
                            "DEVICE": doc.get("DEVICE"),
                            "PORT":doc.get("PORT")}

        elif self.dic_info.get("ORIGEN")=="addDevicesSolar":
            clave=self.dic_info.get("APODO")
            f=Fernet(clave)
            cod_user=self.dic_info.get("USUARIO")
            user= f.decrypt(cod_user)
            cod_passw=self.dic_info.get("PASSWORD")
            passw= f.decrypt(cod_passw)

            dicDatosBackup={"IP":str(self.dic_info.get("IP")), 
                            "NOMBRE":str(self.dic_info.get("NOMBRE")),
                            "MARCA":str(marca),
                            "USER":str(user,'utf-8'),
                            "PASSW":str(passw,'utf-8'),
                            "DEVICE": str(self.dic_info.get("DEVICE")),
                            "PORT":str(self.dic_info.get("PORT"))}
        
        inst_bkp=bkp.cls_generar_backups(dicDatosBackup)
        date,output,mss,msjOutput=inst_bkp.backupequip()
        return date,output,mss,msjOutput
    
    def insertar_backup(self): 
        db = self.instancia_cnn_backups()
        marca=self.colecciones_marcas()

        coll_backups = db[marca]
        coll_backups.insert_one({
                    "NOMBRE":self.dic_info.get("NOMBRE"),
                    "IP":self.dic_info.get("IP"),
                    "PAIS":self.dic_info.get("PAIS"),
                    "MARCA":self.dic_info.get("MARCA"),
                    "CLIENTE":self.dic_info.get("CLIENTE"),
                    "RED":self.dic_info.get("RED"),
                    "BACKUP":self.dic_info.get("BACKUP"),#dic
                    "FECHA":self.dic_info.get("FECHA")#dic
                    })

    def update_backup(self):
        db = self.instancia_cnn_backups()
        marca=self.colecciones_marcas()
        mss,doc=self.buscar_backup()

        if doc==None:
            pass
        else:
            coll = db[marca]
            coll.update_one({"IP":doc.get("IP")},{"$set":{"BACKUP": self.dic_info.get("BACKUP")}})#dic
            coll.update_one({"IP":doc.get("IP")},{"$set":{"FECHA": self.dic_info.get("FECHA")}})#dic
            mss="Backup actualizado"
   
        return mss
