import pandas as pd
import class_conexion.cls_conexion as cnn

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
                    "RED":self.dic_info.get("RED"),
                    "ERROR":self.dic_info.get("INSERT_ERROR"),
                    "FECHA":self.dic_info.get("FECHA")})
