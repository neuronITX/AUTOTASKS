import pandas as pd
import class_conexion.cls_conexion as cnn
import mongodb_backups.cls_mongodb_backups as back_e  

class consultas_historial_equipos:
    def __init__(self,marca,nombre,dic_info):
        self.marca = marca   
        self.nombre = nombre 
        self.dic_info= dic_info  

    def instancia_cnn_historial(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb() 
        if self.marca=="HUAWEI Technology Co.,Ltd":
            db = session["hist_HUAWEI"]
            return db
        elif self.marca=="HUAWEI":
            db = session["hist_HUAWEI"]
            return db
        elif self.marca=="Juniper Networks, Inc.":
            db = session["hist_Juniper"]
            return db
        elif self.marca=="Juniper":
            db = session["hist_Juniper"]
            return db
        elif self.marca=="Beijing Raisecom Scientific & Technology Development Co., Ltd.":
            db = session["hist_Raisecom"]
            return db
        elif self.marca=="Raisecom":
            db = session["hist_Raisecom"]
            return db
        elif self.marca=="Shenzhen Zhongxing Telecom Co.,ltd.":
            db = session["hist_Telecom"]
            return db
        elif self.marca=="Telecom":
            db = session["hist_Telecom"]
            return db
        elif self.marca=="Alcatel":
            db = session["hist_Alcatel"]
            return db
        elif self.marca=="Cisco":
            db = session["hist_Cisco"]
            return db
        elif self.marca=="MikroTik":
            db = session["hist_MikroTik"]
            return db,session

    def insertar_histo(self):
        inst_back=back_e.consultas_backups_equipos(self.marca,self.nombre,self.dic_info)
        db = self.instancia_cnn_historial()

        mss,doc=inst_back.buscar_backup()
        if doc==None:
            pass
        else:
            coll = db[str(doc.get("NOMBRE"))]
            dev= coll.find_one({"FECHA":self.dic_info.get("FECHA")})
            if dev==None:
                coll = db[str(doc.get("NOMBRE"))]
                coll.insert_one({
                    "NOMBRE":doc.get("NOMBRE"),
                    "IP":doc.get("IP"),
                    "PAIS":doc.get("PAIS"),
                    "MARCA":doc.get("MARCA"),
                    "CLIENTE":doc.get("CLIENTE"),
                    "BACKUP":doc.get("BACKUP"),
                    "FECHA":doc.get("FECHA"),
                    "ESTADO": doc.get("ESTADO")})
                mss="Backup generado"
            else:
                mss=None  
        return mss