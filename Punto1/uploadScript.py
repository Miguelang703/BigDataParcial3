import requests
import boto3
import time

def uploadData(name, url,localtime,bucketname,s3):
    r = requests.get(url)
    filepath="/tmp/"+name+".html"
    f = open(filepath,"w")
    f.write(r.text)
    f.close()
    path = 'headlines/raw/periodico='+name+'/year='+str(localtime.tm_year)+'/month='+str(localtime.tm_mon)+'/day='+str(localtime.tm_mday)+'/'+name+'.html'
    s3.meta.client.upload_file(filepath,bucketname , path)

if __name__=='__main__':
    print("Descargando")
    localtime=time.localtime()
    bucket="bigdataparcial32021"
    s3 = boto3.resource('s3')
    uploadData("El_tiempo","https://www.eltiempo.com/",localtime,bucket,s3)
    uploadData("Publimetro","https://www.publimetro.co/",localtime,bucket,s3)
    print("Completado")