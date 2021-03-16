import json
import boto3
import requests
import datetime
import time
import string
#from binance.client import Client
import constants
dynamodb = boto3.resource('dynamodb')
def lambda_handler(event, context):
    table = dynamodb.Table('BINANCE_TICKS')
    now = datetime.datetime.utcnow()
    dt = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')
    endTime = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
    dt1 = dt - datetime.timedelta(minutes = 1)
    startTime = int(dt1.replace(tzinfo=datetime.timezone.utc).timestamp())
    for sym in constants.symbols:
        ep = "https://api.binance.com/api/v3/klines?symbol="+sym+"&interval=1m&startTime="+str(startTime*1000)+"&endTime="+str(endTime*1000)  
        r1 = requests.get(ep)
        if r1.status_code == 200:
            data = json.loads(r1.text)
            if len(data) > 0:
                data = data[0]
                tick = {}
                tick["key"] = sym +"_"+ str(dt1)
                tick["symbol"] = sym
                tick["timestamp"] = str(dt1)
                tick["open"] = str(data[1])
                tick["high"] = str(data[2])
                tick["low"] = str(data[3])
                tick["close"] = str(data[4])
                tick["volume"] = str(data[5])
                table.put_item( Item = tick )
        else:
            print("[CRTITICAL]:unable to get response from api")
    return {
        'statusCode': 200,
        'body': json.dumps('Data captured for tick' + str(dt1))
    }
