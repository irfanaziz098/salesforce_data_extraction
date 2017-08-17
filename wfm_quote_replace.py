import sys
import datetime as dt
from csv import reader
import csv

sys.setdefaultencoding('utf-8')
filecsv=open('Gen81_Wfm_Etl_to_cvd2-20170805.csv','r')
filecsv2=open('Gen81_Wfm_Etl_to_cvd2-new.csv','wb')

def replacecoma(line):
    for i in range(0,len(line)):
        
        line[i]=line[i].replace(',','')
    return line

spamwriter=csv.writer(filecsv2, delimiter=',',
                            quotechar='\'', quoting=csv.QUOTE_MINIMAL)
for line in reader(filecsv):
    li=replacecoma(line)
    spamwriter.writerow(li)

filecsv.close()
filecsv2.close()
