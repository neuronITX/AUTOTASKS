import class_conexion.cls_conexion as cnn
import pandas as pd
import csv
from datetime import datetime

class consultas_latencia:
    def __init__(self,dicDatosLatencia):
        self.dicDatosLatencia= dicDatosLatencia

    def instancia_cnn_ltcUrls(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['ltc_urls']
        return db  
    
    def instancia_cnn_histUrsl(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['hist_Urls']
        return db
    
#ltcUrls
    def updateLtcUrls(self):
        db = self.instancia_cnn_ltcUrls()
        nameUrl= self.dicDatosLatencia.get("NOMBRE")
        coll=db[nameUrl]
        coll.update_one({"NOMBRE": self.dicDatosLatencia.get("NOMBRE")},{"$set":{"ESTADO": self.dicDatosLatencia.get("ESTADO")}})
  
#histUrsl
    def insertarHistLat(self):
        now=datetime.now()
        dateHist = now.strftime("%d-%m-%Y") #SIN HORA
        filas = []
        with open('datosLatencias.txt', 'r') as archivo:
            for linea in archivo:
                fila = linea.strip().split(',')
                filas.append(fila)

        dfLatencias = pd.DataFrame(filas, columns=['NOMBRE','URL', 'PING', 'PPERDIDOS','LATENCIA(M)','FRACCION','FECHA','HORA'])

        dbUrl=self.instancia_cnn_ltcUrls()
        dbHist=self.instancia_cnn_histUrsl()
        listaColl=dbUrl.list_collection_names()
        for nameUrl in listaColl:
            dfname=dfLatencias.groupby('NOMBRE').get_group(nameUrl)
            dfname=dfname.groupby('FECHA').get_group(dateHist)
            dfname.reset_index(inplace=True, drop=True)
            diccionario = dfname.to_dict(orient='records')
        
            collHist=dbHist[nameUrl]               
            collHist.insert_one({"FECHA":dateHist,"CONTENIDO":diccionario})

#ltcDatosUrls 
    def insertarDatosLatenc(self): 
        now=datetime.now()
        date = now.strftime("%d-%m-%Y") 
        hora = now.strftime("%H:%M")        
        nameUrl= self.dicDatosLatencia.get("NOMBRE")
        fraccion=int(self.dicDatosLatencia.get("FRACCION"))
        latenciaMed=float(self.dicDatosLatencia.get("LATENCIA(M)"))
        pperdidos=int(self.dicDatosLatencia.get("PPERDIDOS"))

        datos = [
            [nameUrl,self.dicDatosLatencia.get("URL"), 
                self.dicDatosLatencia.get("PING"),
                pperdidos,latenciaMed,fraccion,date,hora]]

        with open('datosLatencias.txt', 'a', newline='') as archivo:
            escritor_csv = csv.writer(archivo)
            escritor_csv.writerows(datos)

        #ESTADO
        if fraccion < 5:
            pass
        else:
            filas = []
            with open('datosLatencias.txt', 'r') as archivo:
                for linea in archivo:
                    fila = linea.strip().split(',')
                    filas.append(fila)

            dfLatencias = pd.DataFrame(filas, columns=['NOMBRE','URL', 'PING', 'PPERDIDOS','LATENCIA(M)','FRACCION','FECHA','HORA'])

            db=self.instancia_cnn_ltcUrls()
            listaColl=db.list_collection_names()
            for nameUrl in listaColl:
                dfname=dfLatencias.groupby('NOMBRE').get_group(nameUrl)
                dfname.reset_index(inplace=True, drop=True)
                dfname['LATENCIA(M)'] = dfname['LATENCIA(M)'].astype(float)

                pingFalse = dfname['PING'].value_counts().get('False', 0)
                pingNone = dfname['PING'].value_counts().get('None', 0)
                pingTrue = dfname['PING'].value_counts().get('True', 0)

                countError=pingFalse+pingNone
                if countError>pingTrue:
                    self.dicDatosLatencia.setdefault("ESTADO","Inestable")
                    self.updateLtcUrls() 
                else:           
                    listLatName=list(dfname['LATENCIA(M)'])
                    valorActual = listLatName[-1]
                    listLatName.pop()
                    ult23Valores = listLatName[-23:] #valores de las ult 2 horas
                    medLatencia = sum(ult23Valores) / len(ult23Valores)
                    promVariacion=medLatencia/10
                    variacMax=medLatencia+promVariacion
                    variacMin=medLatencia-promVariacion

                    if variacMin < valorActual < variacMax:
                        self.dicDatosLatencia.setdefault("ESTADO","Estable")
                        self.updateLtcUrls()
                    elif valorActual>variacMax:
                        self.dicDatosLatencia.setdefault("ESTADO","Incremento")
                        self.updateLtcUrls()
                    elif valorActual<variacMin:
                        self.dicDatosLatencia.setdefault("ESTADO","Decremento")
                        self.updateLtcUrls()     
