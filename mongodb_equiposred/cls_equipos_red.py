import pandas as pd
import class_conexion.cls_conexion as cnn

class cls_consultas_equipos_red:
    def __init__(self,dicDatos ):
        self.dicDatos= dicDatos 

    def colecciones_marcas(self):
        marca=self.dicDatos.get("MARCA")
        if marca=="HUAWEI Technology Co.,Ltd":
            marca="HUAWEI"
        elif marca=="Juniper Networks, Inc.":
            marca="Juniper"
        elif marca=="Beijing Raisecom Scientific & Technology Development Co., Ltd.":
            marca="Raisecom"
        elif marca=="Shenzhen Zhongxing Telecom Co.,ltd.":
            marca="Telecom"
        else:
            pass
        return marca 

    def instancia_cnn_equipos_red(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['Equipos_Red']
        return db   
    
    def contenido_equiposRed(self):#marca
        db = self.instancia_cnn_equipos_red()
        marca=self.colecciones_marcas()
        coll=db[marca]

        num=coll.count_documents({})
        if num==0:
            dfEquiposRed= pd.DataFrame()
        else:
            all_device= list(coll.find())
            dfEquiposRed= pd.DataFrame(all_device)
            dfEquiposRed=dfEquiposRed.drop(['_id'], axis=1)
            dfEquiposRed=dfEquiposRed.drop(['REGION'], axis=1)
            dfEquiposRed=dfEquiposRed.drop(['APODO'], axis=1)
            dfEquiposRed=dfEquiposRed.drop(['USUARIO'], axis=1)
            dfEquiposRed=dfEquiposRed.drop(['PASSWORD'], axis=1)
            dfEquiposRed=dfEquiposRed.drop(['DEVICE'], axis=1)
            dfEquiposRed.rename(columns={'RED':'ROL'},inplace=True)          
            dfEquiposRed= dfEquiposRed.groupby("ESTADO").get_group(True) 
            dfEquiposRed=dfEquiposRed.drop(['ESTADO'], axis=1)
            dfEquiposRed=dfEquiposRed.fillna('Ninguno')           
        return dfEquiposRed 
    
    def buscar_equipo(self):
        db= self.instancia_cnn_equipos_red()
        marca=self.colecciones_marcas()    
        coll = db[marca]
        doc_pym= coll.find({"IP":self.dicDatos.get("IP")}).limit(1)  
        for doc in doc_pym:
            if doc==None:
                mss=f'''No existe el dispositivo: {self.dicDatos.get("IP")}'''
            else:
                mss=None              
        return mss,doc

    def insertar_equipo(self): 
        db= self.instancia_cnn_equipos_red()
        marca=self.colecciones_marcas()
        coll = db[marca]
        coll.insert_one({
                    "NOMBRE":self.dicDatos.get("NOMBRE"),
                    "IP":self.dicDatos.get("IP"),
                    "REGION":self.dicDatos.get("REGION"),
                    "PAIS":self.dicDatos.get("PAIS"),
                    "MARCA":self.dicDatos.get("MARCA"),
                    "CLIENTE":self.dicDatos.get("CLIENTE"),
                    "RED":self.dicDatos.get("RED"),
                    "ESTADO":self.dicDatos.get("ESTADO"),
                    "APODO":self.dicDatos.get("APODO"),
                    "USUARIO":self.dicDatos.get("USUARIO"),
                    "PASSWORD":self.dicDatos.get("PASSWORD"),
                    "DEVICE":self.dicDatos.get("DEVICE"),
                    "PORT":self.dicDatos.get("PORT"),
                    "BACKUPS":self.dicDatos.get("BACKUPS"),#dic
                    "TUNELES":False }) 
    
    def actualizar_equipo(self):#marca,nombre,backups,tuneles
        db = self.instancia_cnn_equipos_red()
        marca=self.colecciones_marcas()
        coll=db[marca]
        if "BACKUPS" in self.dicDatos.keys():
            coll.update_one({"NOMBRE": self.dicDatos.get("NOMBRE")},{"$set":{"BACKUPS": self.dicDatos.get("BACKUPS")}})
        elif "TUNELES" in self.dicDatos.keys():
            coll.update_one({"NOMBRE": self.dicDatos.get("NOMBRE")},{"$set":{"TUNELES": self.dicDatos.get("TUNELES")}})
        else:
            pass