
import os
import datetime

carpeta = "/home/LATENCIAS"

fecha_actual = datetime.datetime.now()
fecha_actual = fecha_actual.replace(year=2023, month=8, day=31)
fecha_limite = fecha_actual - datetime.timedelta(days=30)

for archivo in os.listdir(carpeta):
    ruta_archivo = os.path.join(carpeta, archivo)

    if os.path.isfile(ruta_archivo):
        fecha_modificacion_timestamp = os.path.getmtime(ruta_archivo)
        fecha_modificacion = datetime.datetime.fromtimestamp(fecha_modificacion_timestamp)
        
        if fecha_modificacion < fecha_limite:
            os.remove(ruta_archivo)
