import pandas as pd
import class_conexion.cls_conexion as cnn
import mongodb_equiposred.cls_equipos_red as equiposRed

class consultas_loopback:
    def __init__(self,dicDatosLoopback):
        self.dicDatosLoopback= dicDatosLoopback

    def instancia_cnn_tnl_equiposTuneles(self):
        inst=cnn.conexion()
        session=inst.conexion_mongodb()
        db = session['tnl_equiposTuneles']
        return db  

    def contenido_tnlEquipos(self):
        db= self.instancia_cnn_tnl_equiposTuneles()
        pais= self.dicDatosLoopback.get("PAIS")
        coll=db[pais]

        num=coll.count_documents({})
        if num==0:
            dfLoopback= pd.DataFrame()
        else:
            all_device= list(coll.find())
            dfLoopback= pd.DataFrame(all_device)
            dfLoopback=dfLoopback.drop(['_id'], axis=1)
        return dfLoopback
    
    def agregarLoopback(self):#nombre,ip,marca,pais,tipo,loopback,tuneles
        db= self.instancia_cnn_tnl_equiposTuneles()
        pais= self.dicDatosLoopback.get("PAIS")
        coll=db[pais]
        coll.insert_one({
                        "NOMBRE": self.dicDatosLoopback.get("NOMBRE"),
                        "IP": self.dicDatosLoopback.get("IP"),
                        "MARCA": self.dicDatosLoopback.get("MARCA"),
                        "PAIS": self.dicDatosLoopback.get("PAIS"),
                        "TIPO": self.dicDatosLoopback.get("TIPO"),
                        "LOOPBACK": self.dicDatosLoopback.get("LOOPBACK")})
        inst_equiposRed=equiposRed.cls_consultas_equipos_red(self.dicDatosLoopback)
        inst_equiposRed.actualizar_equipo()
        mss='Loopback agregado exitosamente.'
        return mss
    
    def eliminarLoopback(self):#pais,nombre,tipo,loopback,marca,tuneles
        db= self.instancia_cnn_tnl_equiposTuneles()
        pais= self.dicDatosLoopback.get("PAIS")
        coll=db[pais]

        for documento in coll.find({}):
            if (documento.get("NOMBRE")==self.dicDatosLoopback.get("NOMBRE"))and(documento.get("TIPO")==self.dicDatosLoopback.get("TIPO"))and(documento.get("LOOPBACK")==self.dicDatosLoopback.get("LOOPBACK")):
                inst_equiposRed=equiposRed.cls_consultas_equipos_red(self.dicDatosLoopback)
                inst_equiposRed.actualizar_equipo()
                coll.delete_one({"_id":documento.get("_id")})
                mss='Loopback eliminado exitosamente.'    
                break     
            else:
                mss=''
                pass
        if mss=='':
            return 'No se pudo eliminar loopback.'
        else:
            return mss

        
