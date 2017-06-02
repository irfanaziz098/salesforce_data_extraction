# -*- coding: utf-8 -*-
/*
@Author: Irfan Aziz
@email: irfanaziz098@gmail.com
*/
import requests  as r
import time
batchfile=open('batchinfo1.txt','w')
#For sandbox the url starts with test for production we use login
url1='https://test.salesforce.com/services/Soap/u/39.0'
header1={'Content-Type':'text/xml; charset=UTF-8','SOAPAction':'login'}
#Now we can read the login.txt by hiding the credential!!!!!
bod=open('login_preprod.txt','r')
#Each time a request is sent using the login info we need to read it everytime
resp1=r.post(url1,headers=header1,data=bod.read())
print 'Response from login:',resp1
#resp1=r.post(url1,headers=header1,data=bod.read())
# reponse is in xml 
#cont=json.loads(resp1.content)
#sid=cont["sessionId"]

#You can use some xml parsing library to extract the SID,
indx1=resp1.text.find('<sessionId>')
sid=resp1.text[indx1+11:indx1+123]
print 'Session ID: ',sid

url='https://yourinstance.salesforce.com/services/async/39.0/job'#
#Create a job
#PK chunking option can added in the header, however, it may not work for some objects like Order
#Sforce-Enable-PKChunking: chunkSize=250000;
#Add the condition for those that support PK-CHUNKING THAT DOENT
#Non_
print 'Creating a job with PK Chunk enaabled !!!'
header={'X-SFDC-Session':sid,'Content-Type':'application/xml; charset=UTF-8'}
header_pk={'X-SFDC-Session':sid,'Content-Type':'application/xml; charset=UTF-8','Sforce-Enable-PKChunking':'true'}

bo=open('create-job.xml','r')
resp2=r.post(url,headers=header,data=bo)
#print resp2.content
indx1=resp2.text.find('<id>')
jobid=resp2.text[indx1+4:indx1+22]

print 'Job created. jobid : ', jobid

print 'Adding batch to job'
#url='https://eu6.salesforce.com/services/async/39.0/job'
## Here we add a batch to the job that queries the opportunity table with pk chunking enabled
query1=open('query.txt','r')
query=query1.read()

print 'Query:',query
url2='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid+'/batch'
header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
resp3=r.post(url2,headers=header,data=query)

indx1=resp3.text.find('<id>')
batchid=resp3.text[indx1+4:indx1+22]
print 'batchid : ', batchid
print 'waiting!!!'
while time.sleep(60):
    print '.'

#check the status of the job if it has one or more completed batchqueued we will then query it.
url3='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid #+'/batch/'+batchid
header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
resp4=r.get(url3,headers=header)
#numberbatchqueuedQueued can tell the status of the batches that are in queue. and if check the numberofcompletedbatches
#is greater than zero

indx1=resp4.text.find('<numberBatchesQueued>')
batchqueued=resp4.text[indx1+21:indx1+22]
batchqueued=int(batchqueued)
print 'NumOfBatchQueued : ', batchqueued
indx2=resp4.text.find('<numberBatchesTotal>')
batchetotal=resp4.text[indx2+20:indx2+21]
batchetotal=int(batchetotal)
print 'NumOfBatchTotal : ', batchetotal
indx3=resp4.text.find('<numberBatchesCompleted>')
batchcomplete=resp4.text[indx2+20:indx2+21]
batchcomplete=int(batchcomplete)
print 'BatchCompleted : ', batchcomplete
        
while batchqueued<>0:
    
    time.sleep(20) 
    #wait untill all batches in the queue are processed
    url3='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid #+'/batch/'+batchid
    header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
    resp4=r.get(url3,headers=header)
    #numberBatchesQueued can tell the status of the batches that are in queue.
    indx1=resp4.text.find('<numberBatchesQueued>')
    batchqueued=resp4.text[indx1+21:indx1+22]
    batchqueued=int(batchqueued)
    print 'NumOfBatchQueued : ', batchqueued
#All batchesprocessed.
if batchqueued==0:
    print "job completed"
#Check all batches    
    url3='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid+'/batch'
    header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
    resp4=r.get(url3,headers=header)
#Gives the batch info. If it has more batch it will give multiple batches<id>...
    batchfile.write(resp4.content)
    batchfile.close()
#    print resp4.content
#The batch will have two or more batchinfo components. What we do is skip the first one and look at the second and rest
#first has <state>NotProcessed others have completed.The id for each varies...So put the ids and the state in one list. Then query those with compeleted state.    
#All batchid may not have a result id associated with it.
    batchfile=open('batchinfo1.txt','r')
    batchid_list=[]
    for li in batchfile:
        indx1=li.find('<id>')
        if indx1<>-1:
            id=li[6:24]
            #print 'the batch',li[6:24]
            if id not in batchid_list:batchid_list.append(id) 
    print batchid_list
    batchfile.close()
    #oNE BATCH ONLY
    result_id=[]
    for i in range(0,len(batchid_list)):
        print batchid_list[i]
        url3='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid+'/batch/'+batchid_list[i]
        header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
        resp4=r.get(url3,headers=header)
        r1=resp4.content
        print r1
        idx=resp4.content.find('<state>')
        state=resp4.content[idx+7:idx+16]
        state=str(state)
        print 'state',state
        url3='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid+'/batch/'+batchid_list[i]+'/result'
        header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
        resp4=r.get(url3,headers=header)
        print resp4.content
        idx=resp4.content.find('<result>')
        out=resp4.content[idx+8:idx+23]
        print 'out:',out
        if state=='Completed':
            result_id.append(out)
        print result_id
        for i in range(len(result_id)):
                url3='https://yourinstance.salesforce.com/services/async/39.0/job/'+jobid+'/batch/'+batchid+'/result/'+result_id[i]
                header={'X-SFDC-Session':sid,'Content-Type':'text/csv; charset=UTF-8'}
                resp4=r.get(url3,headers=header)
                #print resp4.content
                #However we have the ascii codec cant encode characters in position 383-384 not in range 128. I need to fix this. Then it means we are done...
                f=open('test.txt','a')
                f.write(resp4.text.encode('utf8'))
                f.close()
                f2=open('test.csv','a')
                f2.write(resp4.text.encode('utf8'))
                f2.close()
#Close job
url='https://yourinstance.salesforce.com/services/async/39.0/job'#
#Create a job
#PK chunking option can added in the header, however, it may not work for some objects like Order
#Sforce-Enable-PKChunking: chunkSize=250000;
header={'X-SFDC-Session':sid,'Content-Type':'application/xml; charset=UTF-8'}
bo=open('close-job.xml','r')
resp2=r.post(url,headers=header,data=bo)
        
