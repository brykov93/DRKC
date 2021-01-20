# -*- coding: utf-8 -*-
from flask import Flask, request
from flask import jsonify
from flask_cors import CORS
from flask import abort
import json
import pyodbc
import configparser
import hashlib
import uuid
from importlib import reload
import sys,os 
import codecs
import threading
import time
import logging
from logging.handlers import RotatingFileHandler
import traceback
from time import strftime
##if sys.stdout.encoding != 'utf8':
##    sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer, 'strict')
##if sys.stderr.encoding != 'utf8':
##    sys.stderr = codecs.getwriter('utf8')(sys.stderr.buffer, 'strict')

app=Flask(__name__)
CORS(app)
deleteInterval=3600
#app.config['JSON_AS_ASCII'] = False

def execSQL(sql,param,needFeatch):
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+UID+';PWD='+PWD)
    cursor = cnxn.cursor()
    cursor.execute(sql)
    if needFeatch:
        rows = cursor.fetchall()
        cnxn.close()
        return rows
    else:
        try:
            ids=cursor.fetchone()[0]
        except:
            ids=None
        cnxn.commit()
        cnxn.close()
        return ids


##@app.errorhandler(Exception)
##def exceptions(e):
##    """ Logging after every Exception. """
##    ts = strftime('[%Y-%b-%d %H:%M]')
##    tb = traceback.format_exc()
##    logger.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s',
##                  ts,
##                  request.remote_addr,
##                  request.method,
##                  request.scheme,
##                  request.full_path,
##                  tb)
##    return "Internal Server Error", 500

##@app.after_request
##def after_request(response):
##    """ Logging after every request. """
##    # This avoids the duplication of registry in the log,
##    # since that 500 is already logged via @app.errorhandler.
##    if response.status_code != 500:
##        ts = strftime('[%Y-%b-%d %H:%M]')
##        logger.error('%s %s %s %s %s %s',
##                      ts,
##                      request.remote_addr,
##                      request.method,
##                      request.scheme,
##                      request.full_path,
##                      response.status)
##    return response

@app.route('/getMkb',methods=['POST'])
def getMKB():
    param=request.get_json()
    if param==None:
        sql="select * from sprMKB" 
    else:
        sql="select * from sprMKB where codeMKB like '"+param['mkb']+"%'"
    rows = execSQL(sql,None,True)
    sprMKB=[]
    for row in rows:
        sprMKB.append({'ID':row[0],'CODE':row[1],'NAME':(row[1].strip()+':'+row[2].strip())})
    return jsonify(sprMKB)

@app.route('/getStatus',methods=['GET'])
def getStatus():
    sql="select * from sprStatus"
    rows = execSQL(sql,None,True)
    sprMKB=[]
    for row in rows:
        sprMKB.append({'ID':row[0],'NAME':(row[1]).strip()})
    return jsonify(sprMKB)

@app.route('/setPacientInfo',methods=['POST'])
def setPatientInfo():
    param=request.get_json()
    print(param)
    if checkSession(param['sessionId']):
        if 'sluchId' in param['sluch'] and param['sluch']['sluchId']!='':
            sluch=param['sluch']
            patient=sluch['pacient']
            retirement=sluch['retirement']
            mkb=sluch['mkb']
            diary=sluch['diary']
            ############################################
            sql='''UPDATE [reanim].[dbo].[reanimSluch]
                   SET [comingDate] = '''+checkToNull(sluch['commingDate'])+'''
                      [retirementDate] = '''+checkToNull(retirement['retirementDate'])+'''
                      [retirementCause] = '''+checkToNull(retirement['retirementCause'])+'''
                      [kartNum] = '''+sluch['history']+'''
                      ,[status] = '''+checkToNull(sluch['pacientStatus'])[:-1]+'''
                 WHERE [ID]='''+str(sluch['sluchId']) 
            result=execSQL(sql,param,False)
            ############################################
            bloodType=checkToNull(patient['bloodType'])
            if len(bloodType)>1:
                bloodType=bloodType[:-1]
            sql='''UPDATE [reanim].[dbo].[patients]
                   SET [FAM] = '''+checkToNull(patient['surname'])+'''
                      [IM] = '''+checkToNull(patient['name'])+'''
                      [OT] = '''+checkToNull(patient['patronymic'])+'''
                      [MALE] = '''+checkToNull(patient['gender'][:1])+'''
                      [birthDate] = '''+checkToNull(patient['birthDate'])+'''
                      [bloodType] = '''+bloodType+'''
                   WHERE [ID]='''+str(patient['Id'])
            result=execSQL(sql,param,False)
            ############################################
            sluchId=str(sluch['sluchId'])
            sql='''DELETE FROM [reanim].[dbo].[reanimSluchDiagnoses]
                   WHERE [sluchID]='''+sluchId
            result=execSQL(sql,param,False)
            sql='''DELETE FROM [reanim].[dbo].[reanimSluchDiary]
                   WHERE [sluchID]='''+sluchId
            result=execSQL(sql,param,False)
            print(mkb)
            print(diary)
            if len(mkb)>0:
                for s in mkb:
                    sql='''INSERT INTO [reanim].[dbo].[reanimSluchDiagnoses]
                                       ([sluchID]
                                       ,[establishmentDate]
                                       ,[MKB])
                                 VALUES
                                       ('''+"'"+(sluchId)+"'"+''',
                                        '''+"'"+s['establishmentDate']+"'"+''',
                                        '''+"'"+s['MKB']+"'"+''')
                            '''
                    result=execSQL(sql,param,False)
                    print(sql)
            if len(diary)>0:
                for s in diary:
                    sql='''INSERT INTO [reanim].[dbo].[reanimSluchDiary]
                                   ([sluchID]
                                   ,[state]
                                   ,[date])
                             VALUES
                                   ('''+"'"+(sluchId)+"'"+''',
                                   '''+"'"+s['state']+"'"+''',
                                   '''+"'"+s['date']+"'"+''')
                        '''
                    result=execSQL(sql,param,False)
            writeLog(param['sessionId'],sluchId,"'Изменение'")
            return 'OK'
        else:
            lpu=getLpuBySession(param['sessionId'])
            sluch=param['sluch']
            patient=sluch['pacient']
            retirement=sluch['retirement']
            mkb=sluch['mkb']
            diary=sluch['diary']
            ############################################
            bloodType=checkToNull(patient['bloodType'])
            if len(bloodType)>1:
                bloodType=bloodType[:-1]
            sql="INSERT INTO [reanim].[dbo].[patients]([FAM],[IM],[OT],[MALE],[birthDate],[bloodType]) OUTPUT INSERTED.ID VALUES ("
            sql=sql+"'"+patient['surname']+"',"
            sql=sql+"'"+patient['name']+"',"
            sql=sql+"'"+patient['patronymic']+"',"
            sql=sql+"'"+patient['gender'][:1]+"',"
            sql=sql+checkToNull(patient['birthDate'])
            sql=sql+str(bloodType)+")"
            pacId=execSQL(sql,param,False)
            ###########################################
            sql='''INSERT INTO [reanim].[dbo].[reanimSluch]
               ([comingDate]
               ,[retirementDate]
               ,[retirementCause]
               ,[kartNum]
               ,[pacID]
               ,[LPU]
               ,[status])
         OUTPUT INSERTED.ID
         VALUES
               (\''''+sluch['commingDate']+'''\',
               '''+checkToNull(retirement['retirementDate'])+'''
               '''+checkToNull(retirement['retirementCause'])+'''
               \''''+sluch['history']+'''\',
              '''+str(pacId)+''',
              '''+str(lpu)+''',
              \''''+sluch['pacientStatus']+'''\'
               )'''
            sluchId=execSQL(sql,param,False)
            ###########################################
            print(mkb)
            print(diary)
            if len(mkb)>0:
                for s in mkb:
                    sql='''INSERT INTO [reanim].[dbo].[reanimSluchDiagnoses]
                                       ([sluchID]
                                       ,[establishmentDate]
                                       ,[MKB])
                                 VALUES
                                       ('''+"'"+str(sluchId)+"'"+''',
                                        '''+"'"+s['establishmentDate']+"'"+''',
                                        '''+"'"+s['MKB']+"'"+''')
                            '''
                    result=execSQL(sql,param,False)
            if len(diary)>0:
                for s in diary:
                    sql='''INSERT INTO [reanim].[dbo].[reanimSluchDiary]
                                   ([sluchID]
                                   ,[state]
                                   ,[date])
                             VALUES
                                   ('''+"'"+str(sluchId)+"'"+''',
                                   '''+"'"+s['state']+"'"+''',
                                   '''+"'"+s['date']+"'"+''')
                        '''
                    result=execSQL(sql,param,False)
            writeLog(param['sessionId'],sluchId,"'Создание'")
            return 'OK'
    else:
        abort(401)

@app.route('/getPacientsInfo',methods=['POST'])
def getPacientsInfo():
    param=request.get_json()
    if checkSession(param['sessionId']):
        role=getRoleBySession(param['sessionId'])
        LPU=getLpuBySession(param['sessionId'])
        sql='''SELECT jrnSluch.* FROM jrnSluch'''
        if role==2:
            sql=sql+'where jrnSluch.LPU='+str(LPU)
        rows = execSQL(sql,None,True)
        sluchs=[]
        for row in rows:
            diagn=row[4]
            if diagn!=None:
                diagn=diagn[:-1]
            if row[6]:
                status=row[6].strip()
            else:
                status=''
            sluchs.append({'ID':row[0],
                           'kartNum':row[1],
                           'FIO':row[2],
                           'AGE':row[3],
                           'Diagnoses':diagn,
                           'bloodType':row[5],
                           'status':status,
                           'lpuName':row[8]})
        return jsonify(sluchs)
    else:
        abort(401)

@app.route('/getPacientInfo',methods=['POST'])
def getPacientInfo():
    param=request.get_json()
    if checkSession(param['sessionId']):
        sluchID=param['ID']
        sql='''SELECT [ID]
                      ,[comingDate]
                      ,[retirementDate]
                      ,[retirementCause]
                      ,[kartNum]
                      ,[pacID]
                      ,[LPU]
                      ,[status]
                  FROM [reanim].[dbo].[reanimSluch]
                  WHERE [reanim].[dbo].[reanimSluch].ID='''+str(sluchID)
        rows = execSQL(sql,None,True)
        if len(rows)==0:
            return 'OK'
        for row in rows:
            sluch={'ID':Trim(row[0]),
                   'comingDate':Trim(row[1]),
                   'retirementDate':Trim(row[2]),
                   'retirementCause':Trim(row[3]),
                   'kartNum':Trim(row[4]),
                   'pacID':Trim(row[5]),
                   'LPU':Trim(row[6]),
                   'status':Trim(row[7])}
            pacID=row[5]
        sql='''SELECT [ID]
                      ,[FAM]
                      ,[IM]
                      ,[OT]
                      ,[MALE]
                      ,[birthDate]
                      ,[bloodType]
                FROM [reanim].[dbo].[patients]
                WHERE [reanim].[dbo].[patients].ID='''+str(pacID)
        rows = execSQL(sql,None,True)
        for row in rows:
            pacient={'ID':Trim(row[0]),
                   'FAM':Trim(row[1]),
                   'IM':Trim(row[2]),
                   'OT':Trim(row[3]),
                   'MALE':Trim(row[4]),
                   'birthDate':Trim(row[5]),
                   'bloodType':Trim(row[6])}
        sql='''SELECT [ID]
                      ,[sluchID]
                      ,[state]
                      ,[date]
                  FROM [reanim].[dbo].[reanimSluchDiary]
                  WHERE [reanim].[dbo].[reanimSluchDiary].sluchID='''+str(sluchID)
        rows = execSQL(sql,None,True)
        diarys=[]
        if len(rows)>0:
            for row in rows:
                diary={'ID':Trim(row[0]),
                       'sluchID':Trim(row[1]),
                       'state':Trim(row[2]),
                       'date':Trim(row[3])}
                diarys.append(diary)
        sql='''SELECT [ID]
                      ,[sluchID]
                      ,[establishmentDate]
                      ,[MKB]
                FROM [reanim].[dbo].[reanimSluchDiagnoses]
                WHERE [reanim].[dbo].[reanimSluchDiagnoses].sluchID='''+str(sluchID)
        rows = execSQL(sql,None,True)
        mkbs=[]
        for row in rows:
            mkb={'ID':Trim(row[0]),
                   'sluchID':Trim(row[1]),
                   'establishmentDate':Trim(row[2]),
                   'MKB':Trim(row[3])}
            mkbs.append(mkb)
        result={'sluch':sluch,
                'pacient':pacient,
                'diary':diarys,
                'mkb':mkbs}
        return jsonify(result)
    else:
        abort(401)

@app.route('/logout',methods=['POST'])
def logout():
    param=request.get_json()
    if killSession(param['session']):
        return 'OK'

@app.route('/login',methods=['POST'])
def login():
    param=request.get_json()
    sql="select users.*,LPU.nameLPU from users left join LPU on LPU.ID=users.LPU where LOGIN='%s'" % param['userName']
    rows = execSQL(sql,None,True)
    if len(rows)>0:
        for row in rows:
            if row[6].strip()==param['userPass']:
                uid=makeSession(row[0],row[4])
                userInfo={'FAM':row[1],'IM':row[2],'OT':row[3],'SESSION':uid,'LPU':row[7]}
                return jsonify(userInfo)
        abort(401)
    else:
        abort(401)

def getRoleBySession(session):
    sql='''SELECT userRoles.roleId
            FROM sessions 
            LEFT JOIN userRoles ON sessions.userID = userRoles.userId
            where sessions.SESSION=N'''+"'"+session+"'"
    rows = execSQL(sql,None,True)
    return rows[0][0]

def checkSession(uid):
    killExpiredSessions(deleteInterval)
    sql="select * FROM [reanim].[dbo].[sessions] where [SESSION]=N'"+uid+"'"
    rows = execSQL(sql,None,True)
    return len(rows)>0
    
def makeSession(userId,LPU):
    uid=str(uuid.uuid4())
    sql='''INSERT INTO [reanim].[dbo].[sessions]
           ([SESSION]
           ,[userID]
           ,[LPU]
           ,[loginTime])
     VALUES
           (\''''+uid+'''\',
            '''+str(userId)+''',
            '''+str(LPU)+''',GETDATE())'''
    param=[uid,userId,LPU]
    execSQL(sql,param,False)
    return uid

def Trim(s):
    if isinstance( s, str):
        return s.strip()
    else:
        return s

def killSession(uid):
    sql="delete FROM [reanim].[dbo].[sessions] where [SESSION]=N'"+uid+"'"
    execSQL(sql,True,False)
    return True

def getLpuBySession(uid):
    sql="select LPU FROM [reanim].[dbo].[sessions] where [SESSION]=N'"+uid+"'"
    rows = execSQL(sql,None,True)
    return rows[0][0]
    
def checkToNull(param):
    if str(param)=='':
        s=' NULL,'
    elif param==None:
        s=' NULL,'
    else:
        s=" '"+param+"',"
    return s

def getUserBySession(uid):
    sql="select userID FROM [reanim].[dbo].[sessions] where [SESSION]=N'"+uid+"'"
    rows = execSQL(sql,None,True)
    return rows[0][0]

def writeLog(sessionID,sluchID,action):
    userID=getUserBySession(sessionID)
    sql='''INSERT INTO [reanim].[dbo].[LOGS]
                   ([userID]
                   ,[date]
                   ,[sluchID]
                   ,[action])
             VALUES
                   ('''+str(userID)+'''
                   ,GETDATE()
                   ,'''+str(sluchID)+'''
                   ,'''+action+''')'''
    execSQL(sql,True,False)

def killExpiredSessions(interval):
    sql='''DELETE FROM [reanim].[dbo].[sessions]
               where DATEDIFF(SECOND,[reanim].[dbo].[sessions].loginTime,GETDATE())>'''+str(interval)
    execSQL(sql,True,False)

def timer(interval):
    data = threading.local()
    while True:
        time.sleep(interval)
        killExpiredSessions(interval)
 

if __name__=='__main__':
    WorkDir=os.path.realpath(os.path.dirname(sys.argv[0]))
    config = configparser.ConfigParser()
    config.read(WorkDir+r'\settings.ini')
    SERVER=config['DEFAULT']['SERVER']
    DATABASE=config['DEFAULT']['DATABASE']
    UID=config['DEFAULT']['UID']
    PWD=config['DEFAULT']['PWD']
    t = threading.Thread(target=timer, name="killSessions", args=(deleteInterval, ), daemon=True)
    t.start()
##    handler = RotatingFileHandler('app.log', maxBytes=10000, backupCount=3)        
##    logger = logging.getLogger(__name__)
##    logger.setLevel(logging.ERROR)
##    logger.addHandler(handler)
    app.run(host='0.0.0.0',debug=True)
    






