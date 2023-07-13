from decouple import config
from pymongo.errors import AutoReconnect
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import pyodbc
import pandas as pd

#Clase en donde se extraen y envian datos
class conexion:
    def __init__(self):
        self.conexion_mongodb()
        self.conexion_solar()

    def conexion_mongodb(self):
        for i in range(5):
            try:
                client = MongoClient('localhost')
                return client
            except ConnectionFailure:
                print("Error de conexión")

    
    def conexion_solar(self):
        cnxn_str = (f"Driver={config('solar_Driver')};"
            f"Server={config('solar_Server')};"
            f"Database={config('solar_Database')};"
            f"UID={config('solar_UID')};"
            f"PWD={config('solar_PWD')};"
            f"TrustServerCertificate={config('Certificate')};")

        try:
            cnxn = pyodbc.connect(cnxn_str)
            cursor = cnxn.cursor()

            cursor.execute(config('query'))
            registros=cursor.fetchall()

            columnas = ["Rol","SysName","IP_Address","Vendor","IOSVersion","MachineType","Pais","Region","Cliente","ObjectSubType"]
            l_registros=list()
            for i in registros:
                l_registros.append([i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9]])
            df= pd.DataFrame(l_registros, columns=columnas)

            list_filtro=['Cisco','HUAWEI Technology Co.,Ltd','MikroTik','Alcatel','Shenzhen Zhongxing Telecom Co.,ltd.',
            'Beijing Raisecom Scientific & Technology Development Co., Ltd.','Juniper Networks, Inc.']
            vendor=pd.DataFrame()
            for i in list_filtro:
                filtro = df.groupby('Vendor').get_group(str(i))   
                vendor= pd.concat([vendor,filtro])
            df_final = vendor.groupby('ObjectSubType').get_group('SNMP') 
            df_final=df_final.fillna("Ninguno")

            df_final["IP_Address"]= df_final["IP_Address"].astype(str)    
            ip_inv= df_final.index[df_final["IP_Address"].str.contains("10.216.")].tolist()
            for ip in ip_inv:
                df_final=df_final.drop([ip])
            df_final.reset_index(inplace=True, drop=True)

            df_final["IP_Address"]= df_final["IP_Address"].astype(str)    
            ip_inv= df_final.index[df_final["IP_Address"].str.contains("172.22.")].tolist()
            for ip in ip_inv:
                df_final=df_final.drop([ip])
            df_final.reset_index(inplace=True, drop=True)  

            df_final["Pais"]= df_final["Pais"].astype(str)  
            pais_inv= df_final.index[df_final["Pais"].str.contains("Brasil")].tolist() 
            for ip in pais_inv:
                df_final=df_final.drop([ip])
            df_final.reset_index(inplace=True, drop=True)

            return df_final

        except Exception as ex:
            print(ex)
