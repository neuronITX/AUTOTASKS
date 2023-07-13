from paramiko.ssh_exception import SSHException, AuthenticationException, NoValidConnectionsError
from netmiko import ConnectHandler, NetMikoAuthenticationException, NetMikoTimeoutException, ReadTimeout
import pandas as pd
from datetime import datetime

#Clase genera los backups
class cls_generar_backups:
    #IP,nombre,marca,user,passw,device,port
    def __init__(self,dicDatosBackup):
        self.dicDatosBackup= dicDatosBackup   

#EXCEPCIONES
    def exc_deviceNotReachable(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")

        mss="Device not reachable"
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")
        output=None 
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: Telnet (Fail)  - Backup: Fail - Error: {mss}"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (Fail)  - Backup: Fail - Error: {mss}"
        return date, output, mss,msjOutput

    def exc_readTimeout(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")

        mss="Timeout"
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")
        output=None 
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: Telnet (Fail)  - Backup: Fail - Error: {mss}"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (Fail)  - Backup: Fail - Error: {mss}"
        return date, output, mss ,msjOutput

    def exc_authenticationException(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")

        mss="Authentication Failure"
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")
        output=None 
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: Telnet (Fail)  - Backup: Fail - Error: {mss}"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (Fail)  - Backup: Fail - Error: {mss}"
        return date, output, mss ,msjOutput
                     
    def exc_sshException(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")

        mss="SSH Disabled"
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")
        output=None 
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: Telnet (Fail)  - Backup: Fail - Error: {mss}"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (Fail)  - Backup: Fail - Error: {mss}"
        return date, output, mss,msjOutput

    def exc_noValidConnectionsError(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")

        mss="Socket Error"
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")
        output=None 
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: Telnet (Fail)  - Backup: Fail - Error: {mss}"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (Fail)  - Backup: Fail - Error: {mss}"
        return date, output, mss,msjOutput

    def exc_error(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")

        mss='Error'
        now=datetime.now()
        date = now.strftime("%d-%m-%Y")
        output=None
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: Telnet (Fail)  - Backup: Fail - Error: {mss}"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (Fail)  - Backup: Fail - Error: {mss}"
        return date, output, mss ,msjOutput


#DISPOSITIVOS POR MARCA

    def backupequip(self):
        marca=self.dicDatosBackup.get("MARCA")
        if marca == "Cisco":
            try:                
                output,msjOutput= self.BackupCisco()
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss  ,msjOutput 
            except NetMikoTimeoutException:
                date, output, mss,msjOutput= self.exc_deviceNotReachable()
                return date, output, mss,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput= self.exc_readTimeout()
                return date, output, mss,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput= self.exc_authenticationException()
                return date, output, mss,msjOutput
            except SSHException:
                date, output, mss,msjOutput= self.exc_sshException()
                return date, output, mss,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput= self.exc_noValidConnectionsError()
                return date, output, mss,msjOutput
            except:
                date, output, mss,msjOutput= self.exc_error() 
                return date, output, mss ,msjOutput

        elif marca == "Juniper":  
            try:
                output,msjOutput= self.BackupJuniper()
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss,msjOutput
            except NetMikoTimeoutException:
                date, output, mss,msjOutput= self.exc_deviceNotReachable()
                return date, output, mss,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput= self.exc_readTimeout()
                return date, output, mss,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput= self.exc_authenticationException()
                return date, output, mss,msjOutput
            except SSHException:
                date, output, mss,msjOutput= self.exc_sshException()
                return date, output, mss,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput= self.exc_noValidConnectionsError()
                return date, output, mss,msjOutput
            except:
                date, output, mss,msjOutput= self.exc_error() 
                return date, output, mss,msjOutput

        elif marca == "HUAWEI":
            try: 
                output,msjOutput= self.BackupHuawei()
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss ,msjOutput                                  
            except NetMikoTimeoutException:
                date, output, mss,msjOutput=self.exc_deviceNotReachable()
                return date, output, mss ,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput=self.exc_readTimeout()
                return date, output, mss ,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput=self.exc_authenticationException()
                return date, output, mss ,msjOutput
            except SSHException:
                date, output, mss,msjOutput=self.exc_sshException()
                return date, output, mss ,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput=self.exc_noValidConnectionsError()
                return date, output, mss ,msjOutput
            except:
                date, output, mss,msjOutput=self.exc_error()  
                return date, output, mss ,msjOutput
        
        elif marca == "Raisecom":
            try: 
                output,msjOutput= self.BackupRaisecom()            
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss ,msjOutput          
            except NetMikoTimeoutException:
                date, output, mss,msjOutput=self.exc_deviceNotReachable()
                return date, output, mss ,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput=self.exc_readTimeout()
                return date, output, mss ,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput=self.exc_authenticationException()
                return date, output, mss ,msjOutput
            except SSHException:
                date, output, mss,msjOutput=self.exc_sshException()
                return date, output, mss ,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput=self.exc_noValidConnectionsError()
                return date, output, mss ,msjOutput
            except:
                date, output, mss,msjOutput=self.exc_error()  
                return date, output, mss ,msjOutput

        elif marca == "Alcatel":
            try: 
                output,msjOutput= self.BackupNokia() 
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss,msjOutput           
            except NetMikoTimeoutException:
                date, output, mss,msjOutput=self.exc_deviceNotReachable()
                return date, output, mss ,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput=self.exc_readTimeout()
                return date, output, mss ,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput=self.exc_authenticationException()
                return date, output, mss ,msjOutput
            except SSHException:
                date, output, mss,msjOutput=self.exc_sshException()
                return date, output, mss ,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput=self.exc_noValidConnectionsError()
                return date, output, mss ,msjOutput
            except:
                date, output, mss,msjOutput=self.exc_error()  
                return date, output, mss  ,msjOutput                         

        elif marca == "Telecom":
            try:            
                output,msjOutput= self.BackupTelecom()
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss ,msjOutput                  
            except NetMikoTimeoutException:
                date, output, mss,msjOutput=self.exc_deviceNotReachable()
                return date, output, mss ,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput=self.exc_readTimeout()
                return date, output, mss ,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput=self.exc_authenticationException()
                return date, output, mss ,msjOutput
            except SSHException:
                date, output, mss,msjOutput=self.exc_sshException()
                return date, output, mss ,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput=self.exc_noValidConnectionsError()
                return date, output, mss ,msjOutput
            except:
                date, output, mss,msjOutput=self.exc_error()  
                return date, output, mss ,msjOutput
        
        elif marca == "MikroTik":
            try:            
                output,msjOutput= self.BackupMikroTik()
                now=datetime.now()
                date = now.strftime("%d-%m-%Y")
                mss=None   
                return date, output, mss ,msjOutput                  
            except NetMikoTimeoutException:
                date, output, mss,msjOutput=self.exc_deviceNotReachable()
                return date, output, mss ,msjOutput
            except ReadTimeout:
                date, output, mss,msjOutput=self.exc_readTimeout()
                return date, output, mss ,msjOutput
            except AuthenticationException:
                date, output, mss,msjOutput=self.exc_authenticationException()
                return date, output, mss ,msjOutput
            except SSHException:
                date, output, mss,msjOutput=self.exc_sshException()
                return date, output, mss ,msjOutput
            except NoValidConnectionsError:
                date, output, mss,msjOutput=self.exc_noValidConnectionsError()
                return date, output, mss ,msjOutput
            except:
                date, output, mss,msjOutput=self.exc_error()  
                return date, output, mss ,msjOutput


    def BackupCisco(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandCisco = "show runn"
        equip = {
            "device_type": device,
            "host": ip,
            "fast_cli": False,
            "username": user,
            "password": passw,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        output = net_connect.send_command(commandCisco,read_timeout=200)

        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"
        
        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput

    def BackupRaisecom(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandNokia = "admin display-config"
        equip = {
            "device_type": device,
            "host": ip,
            "fast_cli": False,
            "username": user,
            "password": passw,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        net_connect.send_command("ena")
        net_connect.send_command("raisecom")
        net_connect.send_command("terminal page-break disable")
        output = net_connect.send_command("show running-config",read_timeout=200)

        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"
        
        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput
  
    def BackupNokia(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandNokia = "admin display-config"
        equip = {
            "device_type": device,
            "host": ip,
            "fast_cli": False,
            "username": user,
            "password": passw,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        output = net_connect.send_command(commandNokia,read_timeout=200)
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"
        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput

    def BackupJuniper(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandJuniper= "show configuration | display set"
        equip = {
            "device_type": device,
            "host": ip,
            "username": user,
            "password": passw,
            "fast_cli": False,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        output = net_connect.send_command(commandJuniper,read_timeout=200)

        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"
        
        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput
    
    def BackupHuawei(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandHuawei = "disp curr"
        equip = {
            "device_type": device,
            "host": ip,
            "fast_cli": False,
            "username": user,
            "password": passw,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        output = net_connect.send_command(commandHuawei,read_timeout=200)
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"
        
        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput

    def BackupTelecom(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandTelecom = "show startup-config"
        equip = {
            "device_type": device,
            "host": ip,
            "fast_cli": False,
            "username": user,
            "password": passw,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        output = net_connect.send_command(commandTelecom,read_timeout=200)
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"

        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput
    
    def BackupMikroTik(self):
        device=self.dicDatosBackup.get("DEVICE")
        nombre=self.dicDatosBackup.get("NOMBRE")
        ip=self.dicDatosBackup.get("IP")
        marca=self.dicDatosBackup.get("MARCA")
        user=self.dicDatosBackup.get("USER")
        passw=self.dicDatosBackup.get("PASSW")
        port=self.dicDatosBackup.get("PORT")

        commandTelecom = "show runn"
        equip = {
            "device_type": device,
            "host": ip,
            "fast_cli": False,
            "username": user,
            "password": passw,
            "conn_timeout": 180,
            "port": port,
            "banner_timeout": 200,
        }
        net_connect = ConnectHandler(**equip)
        output = net_connect.send_command(commandTelecom,read_timeout=200)
        if "telnet" in device:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión:  Telnet (OK)  - Backup: Ok"
        else:
            msjOutput=f"{nombre} - {ip} - {marca} - Conexión: SSH (OK)  - Backup: Ok"
        
        net_connect.disconnect()
        bytOutput=output.encode('utf-8')
        strOutput=bytOutput.decode('utf-8')
        return strOutput,msjOutput
