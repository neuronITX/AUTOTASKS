import pandas as pd
import class_conexion.cls_conexion as cnn
import cls_process_auto as cmongo
from datetime import datetime

class consultas_latencia:
    def __init__(self,dicDatosLatencia):
        self.dicDatosLatencia= dicDatosLatencia

    def instancia_cnn_ltcUrls(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['ltc_urls']
        return db  
    
    def instancia_cnn_ltcDatosUrls(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['ltc_datosUrls']
        return db  
    
    def instancia_cnn_histUrsl(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['hist_Urls']
        return db
    
#ltcUrls
    def contenido_ltcUrls(self):
        db = self.instancia_cnn_ltcUrls()
        nameUrl= self.dicDatosLatencia.get("NOMBRE")
        coll=db[nameUrl]

        num=coll.count_documents({})
        if num==0:
            dfUrls= pd.DataFrame()
        else:
            all_device= list(coll.find())
            dfUrls= pd.DataFrame(all_device)
            dfUrls=dfUrls.drop(['_id'], axis=1)
        return dfUrls
    
    def updateLtcUrls(self):
        db = self.instancia_cnn_ltcUrls()
        nameUrl= self.dicDatosLatencia.get("NOMBRE")
        coll=db[nameUrl]
        coll.update_one({"NOMBRE": self.dicDatosLatencia.get("NOMBRE")},{"$set":{"ESTADO": self.dicDatosLatencia.get("ESTADO")}})
  
#histUrsl
    def insertarHistLat(self):
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")

        db = self.instancia_cnn_histUrsl()
        nameUrl= self.dicDatosLatencia.get("NOMBRE")
        coll=db[nameUrl]
        coll.insert_one({
                    "URL":self.dicDatosLatencia.get("URL"),
                    "PPERDIDOS":self.dicDatosLatencia.get("PPERDIDOSMED"),
                    "LATENCIA(M)":self.dicDatosLatencia.get("LATENCIA(M)MED"),
                    "FECHA":date,
                    "ESTADO":self.dicDatosLatencia.get("ESTADO")})

#ltcDatosUrls 
    def insertarDatosLatenc(self): 
        db= self.instancia_cnn_ltcDatosUrls()
        nameUrl= self.dicDatosLatencia.get("NOMBRE")
        fraccion=int(self.dicDatosLatencia.get("FRACCION"))
        latenciaMed=float(self.dicDatosLatencia.get("LATENCIA(M)"))
        pperdidos=int(self.dicDatosLatencia.get("PPERDIDOS"))
        coll=db[nameUrl]
        coll.insert_one({
                    "URL":self.dicDatosLatencia.get("URL"),
                    "PING":self.dicDatosLatencia.get("PING"),
                    "PPERDIDOS":pperdidos,
                    "LATENCIA(M)":latenciaMed,
                    "FRACCION":fraccion,
                    })
        if fraccion==1:
            pass
        elif fraccion==8:
            sumLatencia=0
            sumPperdidos=0
            countError=0
            count=0
            for i in range(fraccion,0,-1):   
                doc= coll.find_one({"FRACCION":i})
                if doc.get("PING")==True:
                    sumLatencia=sumLatencia+latenciaMed
                    sumPperdidos=sumPperdidos+pperdidos
                    count=count+1
                else:
                    countError=countError+1  

            if (countError>count)or(sumLatencia==0):
                self.dicDatosLatencia.setdefault("PPERDIDOSMED",0)
                self.dicDatosLatencia.setdefault("LATENCIA(M)MED",0)
                self.dicDatosLatencia.setdefault("ESTADO","Inestable")
                self.insertarHistLat()        
            else:
                medSumLatencia=sumLatencia/count
                promVariacion=medSumLatencia/10
                variacMax=medSumLatencia+promVariacion
                variacMin=medSumLatencia-promVariacion

                if latenciaMed>variacMax:
                    self.dicDatosLatencia.setdefault("PPERDIDOSMED",round(sumPperdidos,2))
                    self.dicDatosLatencia.setdefault("LATENCIA(M)MED",round(medSumLatencia,2))
                    self.dicDatosLatencia.setdefault("ESTADO","Incremento")
                    self.insertarHistLat()
                elif latenciaMed<variacMin:
                    self.dicDatosLatencia.setdefault("PPERDIDOSMED",round(sumPperdidos,2))
                    self.dicDatosLatencia.setdefault("LATENCIA(M)MED",round(medSumLatencia,2))
                    self.dicDatosLatencia.setdefault("ESTADO","Decremento")
                    self.insertarHistLat()
                else:
                    self.dicDatosLatencia.setdefault("PPERDIDOSMED",round(sumPperdidos,2))
                    self.dicDatosLatencia.setdefault("LATENCIA(M)MED",round(medSumLatencia,2))
                    self.dicDatosLatencia.setdefault("ESTADO","Estable")
                    self.insertarHistLat()

        elif fraccion>2:         
            sumLatencia=0
            count=0
            countError=0
            for i in range(fraccion-1,0,-1):   
                doc= coll.find_one({"FRACCION":i})
                if doc.get("PING")==True:
                    sumLatencia=sumLatencia+latenciaMed
                    count=count+1
                else:
                    countError=countError+1 

            if (countError>count)or(sumLatencia==0):
                self.dicDatosLatencia.setdefault("ESTADO","Inestable")
                self.updateLtcUrls() 
            else:    
                medSumLatencia=sumLatencia/count
                promVariacion=medSumLatencia/10
                variacMax=medSumLatencia+promVariacion
                variacMin=medSumLatencia-promVariacion

                if latenciaMed>variacMax:
                    self.dicDatosLatencia.setdefault("ESTADO","Incremento")
                    self.updateLtcUrls()
                elif latenciaMed<variacMin:
                    self.dicDatosLatencia.setdefault("ESTADO","Decremento")
                    self.updateLtcUrls()
                else:
                    self.dicDatosLatencia.setdefault("ESTADO","Estable")
                    self.updateLtcUrls() 
 
    def deleteDatosLatenc(self):
        db = self.instancia_cnn_ltcDatosUrls()
        listaColl=db.list_collection_names()   
        for nameUrl in listaColl:
            coll=db[nameUrl]
            for documento in coll.find({}):
                coll.delete_one({"_id":documento.get("_id")})
