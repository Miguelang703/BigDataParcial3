import boto3
import datetime as dt
from bs4 import BeautifulSoup

s3 = boto3.resource('s3')
print('INICIANDO')

#Nombre de las paginas
name_eltiempo = 'El_tiempo'
name_publimetro = 'Publimetro'
#Fecha
today = dt.datetime.today()
day_actual = today.day
month_actual = today.month
year_actual = today.year

#Path
download_path_eltiempo = 'headlines/raw/periodico=' + name_eltiempo + '/year=' + str(year_actual) + '/month=' + str(month_actual) + '/day=' + str(day_actual) + '/' + name_eltiempo + '.html'
download_path_publimetro = 'headlines/raw/periodico=' + name_publimetro + '/year=' + str(year_actual) + '/month=' + str(month_actual) + '/day=' + str(day_actual) + '/' + name_publimetro + '.html'

#Direccion
key_eltiempo = download_path_eltiempo
key_publimetro = download_path_publimetro

#Nombre del bucket
bucketName = 'bigdataparcial32021'#'bucketparcialbd'

#Nombre del archivo a descargar
download_path_eltiempo = '/tmp/{}.'.format(key_eltiempo.split('/')[-1])
download_path_publimetro = '/tmp/{}.'.format(key_publimetro.split('/')[-1])
print(' - Descargando archivos')
#Descarga del archivo
s3.meta.client.download_file(bucketName, key_eltiempo, download_path_eltiempo)
s3.meta.client.download_file(bucketName, key_publimetro, download_path_publimetro)

#scraping el tiempo
print(' - scraping el tiempo')
with open(download_path_eltiempo) as file:
  content = file.read()
  soupET = BeautifulSoup(content,'html.parser')

articleET = soupET.find_all('div', attrs={'class': 'article-details'})

eltiempoCSV='{}; {}; {} \n'.format('categoria','titulo','link')

for row in articleET:
  try:
    a = str(str(str(str(row.find_all('a', attrs={'class':'category'})).split('<')).split('>')).split(',')[2]).replace('"','').replace("'","")
    b = str(str(str(row.find_all('a', attrs={'class':'title'})).split('<')).split('>')[1]).replace('"','').replace("'","").replace(', /a','')
    c = 'https://www.eltiempo.com'+str(row.find_all('a', attrs={'class':'title'})).split('"')[3]
    eltiempoCSV = eltiempoCSV+'{}; {}; {} \n'.format(a,b,c)
  except:
    pass

#Archivo resultante del scaping del tiempo
archivo=open('/tmp/eltiempo.txt','w', encoding='utf-8') 
archivo.write(''+eltiempoCSV)
archivo.close()

#Scraping el espectador
print(' - scraping publimetro ')
with open(download_path_publimetro) as file:
  content = file.read()
  soupES = BeautifulSoup(content,'html.parser')

articleES = soupES.find_all('article', attrs={'class': 'list-item-simple'})

publimetroCSV='{}; {}; {} \n'.format('categoria','titulo','link')

for row in articleES:
  try:
    a = row.find('span').get_text()
    b = row.find('h2').find('a').get_text()
    if b == None:
      b = row.find('h3').find('a').get_text()
    c = "https://www.publimetro.co"+row.find('a').get('href')
    #print(a+', '+b+', '+c)
    publimetroCSV = publimetroCSV+'{}; {}; {} \n'.format(a,b,c)
  except:
    pass

#Archivo resultante del espectador 
archivo=open('/tmp/publimetro.txt','w', encoding='utf-8') 
archivo.write(''+publimetroCSV)
archivo.close()

bucketName='bigdataparcial32021-scrapped'#'scrappingbd'
#Path de subida
upload_path_eltiempo = 'headlines/final/periodico='+name_eltiempo+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_eltiempo+'.csv'
upload_path_publimetro = 'headlines/final/periodico='+name_publimetro+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_publimetro+'.csv'
#Subida del archivo 
s3.meta.client.upload_file('/tmp/eltiempo.txt', bucketName, upload_path_eltiempo)
s3.meta.client.upload_file('/tmp/publimetro.txt', bucketName, upload_path_publimetro)
print(' - FIN')