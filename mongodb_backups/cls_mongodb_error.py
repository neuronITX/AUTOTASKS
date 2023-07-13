import pandas as pd
import class_conexion.cls_conexion as cnn
import mongodb_backups.cls_mongodb_backups as back_e

class consultas_error_equipos:
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
 
    def instancia_cnn_error(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['bck_error']
        return db
    
    def coll_mongodb_error(self):
        db = self.instancia_cnn_error()
        marca=self.colecciones_marcas()

        coll = db[marca]
        num=coll.count_documents({})
        if num==0:
            df_coll= pd.DataFrame()
        else:
            all_device= list(coll.find())
            df_coll= pd.DataFrame(all_device)
            df_coll=df_coll.drop(['_id'], axis=1)
            df_coll=df_coll.fillna('Ninguno')
        return df_coll

    def df_database(self):
        self.marca="Alcatel"
        df_alcatel=self.coll_mongodb_error()
        self.marca="Cisco"
        df_cisco=self.coll_mongodb_error()
        self.marca="HUAWEI"
        df_huawei=self.coll_mongodb_error()
        self.marca="Juniper"
        df_juniper=self.coll_mongodb_error()
        self.marca="Raisecom"
        df_raisecom=self.coll_mongodb_error()
        self.marca="MikroTik"
        df_mikrotik=self.coll_mongodb_error()
        self.marca="Telecom"
        df_telecom=self.coll_mongodb_error()

        df=pd.concat([df_alcatel,df_cisco,df_huawei,df_juniper,df_raisecom,df_mikrotik,df_telecom])
        df.reset_index(inplace=True, drop=True)
        return df,df_alcatel,df_cisco,df_huawei,df_juniper,df_raisecom,df_mikrotik,df_telecom

    def buscar_error(self):
        db = self.instancia_cnn_error()
        marca=self.colecciones_marcas()
        
        coll = db[marca]
        doc= coll.find_one({"IP":self.dic_info.get("IP")})
        if doc==None:
            mss=f"No existe el dispositivo: {self.nombre}"
        else:
            mss=None          
  
        return mss,doc

    def insertar_error(self): 
        db = self.instancia_cnn_error()
        marca=self.colecciones_marcas()

        coll_error = db[marca]
        coll_error.insert_one({
                    "NOMBRE":self.dic_info.get("NOMBRE"),
                    "IP":self.dic_info.get("IP"),
                    "PAIS":self.dic_info.get("PAIS"),
                    "MARCA":self.dic_info.get("MARCA"),
                    "CLIENTE":self.dic_info.get("CLIENTE"),
                    "ERROR":self.dic_info.get("INSERT_ERROR"),
                    "FECHA":self.dic_info.get("FECHA")})
