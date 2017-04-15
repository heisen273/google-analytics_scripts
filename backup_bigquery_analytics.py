import os
import datetime
import subprocess
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
files = [f for f in os.listdir('./SaveExtract/') if  len(f.split('_'))>1 and f.split('_')[0] == datetime.datetime.now().strftime('%Y%m%d')]
print files
for f in files:
    table_name = f.split('_')[1].lower()
    if 'gebraucht-kaufen.at' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.AT_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.AT_VisitsPast13Months page:string,sessions:integer')
                #os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.AT_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.AT_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.AT_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
                    
        
        print 'AT_VisitsPast13Months'
    elif 'gebraucht-kaufen.de' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.DE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.DE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.DE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.DE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.DE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'DE_VisitsPast13Months'
    elif 'canada.for-sale.com' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.CA_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.CA_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --max_bad_records=10 --skip_leading_rows=1 --field_delimiter=";" LogFilesv2_Dataset.CA_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.CA_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.CA_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'CA_VisitsPast13Months'
    elif 'for-sale.in' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.IN_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.IN_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --max_bad_records=10 --skip_leading_rows=1 --field_delimiter=";" LogFilesv2_Dataset.IN_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.IN_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.IN_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'IN_VisitsPast13Months'
    elif 'for-sale.co.uk' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.UK_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.UK_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --max_bad_records=10 --skip_leading_rows=1 --field_delimiter=";" LogFilesv2_Dataset.UK_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break              
        print 'UK_VisitsPast13Months'
    elif 'immobilier-france.fr' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.FR_RE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.FR_RE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --max_bad_records=10 --skip_leading_rows=1 --field_delimiter=";" LogFilesv2_Dataset.FR_RE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.FR_RE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.FR_RE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'FR_RE_VisitsPast13Months'
    elif 'homes.for-sale.co.uk' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.UK_RE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.UK_RE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load  --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.UK_RE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_RE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_RE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'UK_RE_VisitsPast13Months'
    elif 'in-vendita.it' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.IT_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.IT_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.IT_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.IT_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.IT_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'IT_VisitsPast13Months'
    elif 'for-sale.ie' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.IE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.IE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load  --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.IE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.IE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.IE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'IE_VisitsPast13Months'
    elif 'compra-venta.es' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.ES_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.ES_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.ES_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.ES_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.ES_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break   
        print 'ES_VisitsPast13Months'
    elif 'birmingham' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.UK_Birmingham_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.UK_Birmingham_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.UK_Birmingham_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_Birmingham_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_Birmingham_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'UK_Birmingham_VisitsPast13Months'
    elif 'australia' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.AU_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.AU_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.AU_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.AU_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.AU_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'AU_VisitsPast13Months'
    elif 'za' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.SA_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.SA_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.SA_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.SA_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.SA_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'SA_VisitsPast13Months'
    elif 'leeds' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.UK_Leeds_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.UK_Leeds_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.UK_Leeds_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_Leeds_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_Leeds_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'UK_Leeds_VisitsPast13Months'
    elif 'nigeria' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.NG_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.NG_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.NG_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.NG_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.NG_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'NG_VisitsPast13Months'
    elif 'manchester' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.UK_Manchester_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.UK_Manchester_VisitsPast13Months page:string,sessions:integer')

#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.UK_Manchester_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_Manchester_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_Manchester_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break        print 'UK_Manchester_VisitsPast13Months'
    elif 'london' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.UK_London_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.UK_London_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.UK_London_VisitsPast13Months ./SaveExtract/')
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_London_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.UK_London_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'UK_London_VisitsPast13Months'
    elif 'site-annonce.be' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.BE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.BE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.BE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.BE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.BE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'BE_VisitsPast13Months'
    elif 'site-annonce.fr' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.FR_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.FR_VisitsPast13Months page:string,sessions:integer')

#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.FR_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.FR_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.FR_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'FR_VisitsPast13Months'
    elif 'brasil' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.BR_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.BR_VisitsPast13Months page:string,sessions:integer')

#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.BR_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.BR_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.BR_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'BR_VisitsPast13Months'
    elif 'te-koop' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.NL_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.NL_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.NL_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.NL_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.NL_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'NL_VisitsPast13Months'
    elif 'used.forsale.csv' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.US_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.US_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.US_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.US_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.US_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'US_VisitsPast13Months'
    elif 'veneta.com.ar' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.AR_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.AR_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.AR_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.AR_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.AR_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'AR_VisitsPast13Months'
    elif 'venta.com.mx' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.MX_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.MX_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.MX_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.MX_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.MX_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'MX_VisitsPast13Months'
    elif 'chile' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.CL_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.CL_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.CL_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.CL_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.CL_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'CL_VisitsPast13Months'
    elif 'colombia' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.CO_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.CO_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.CO_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.CO_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.CO_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break
        print 'CO_VisitsPast13Months'
    elif 'peru' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.PE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.PE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.PE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.PE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.PE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break        
        print 'PE_VisitsPast13Months'
    elif 'venezuela' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.VE_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.VE_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";"  LogFilesv2_Dataset.VE_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.VE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.VE_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break         
        print 'VE_VisitsPast13Months'
    elif 'uzywane' in table_name:
        os.system('bq rm -f LogFilesv2_Dataset.PL_VisitsPast13Months')
        os.system('bq mk LogFilesv2_Dataset.PL_VisitsPast13Months page:string,sessions:integer')
#                os.system('bq load --skip_leading_rows=1 --max_bad_records=10 --field_delimiter=";" LogFilesv2_Dataset.PL_VisitsPast13Months ./SaveExtract/'+f)
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1', '--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.PL_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        print(stdout)
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            while repeat:
                for i in range(10):
                    a = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=10','--field_delimiter=;','LogFilesv2_Dataset.PL_VisitsPast13Months', './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = a.stdout.readlines()
                    print(stdout)
                    if stdout == ['\n','\n']:
                        repeat = False
                        break
                    if stdout[2].split(' ')[0].lower() == 'warning':
                        repeat = False
                        break         
        print 'PL_VisitsPast13Months'

