import json
import boto3
import datetime
import time
import string
import constants
import math
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
def lambda_handler(event, context):
    # TODO implement
    print(event)
    params = event.get("queryStringParameters")
    print(params)
    # body = json.loads(event["body"])
    # print(body)
    interval = params.get("interval")
    symbol = params.get("symbol")
    limit = params.get("limit")
    if symbol is None:
        symbol = constant.symbols
    table = dynamodb.Table('BINANCE_TICKS')
    endTime = '2021-03-04 00:26:00'
    endTime = datetime.datetime.strptime('2021-03-04 00:26','%Y-%m-%d %H:%M')
    if limit == '12h':
        limit = endTime - datetime.timedelta(minutes  = 720)
    elif limit == '48h':
        limit = endTime - datetime.timedelta(minutes = 2160)
    else:
        limit = endTime -  datetime.timedelta(minutes = 1440)
    bool = True
    result =[]
    response = getResponse(bool,table,interval, symbol, endTime, limit)
    print(response)
    result.append(response)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
def getResponse(bool, table, interval, symbol, endTime, limit):
    res = []
    while(bool):
        if interval == '5m':
            startTime = endTime - datetime.timedelta(minutes = 5)
        elif interval == '15m':
            startTime = endTime - datetime.timedelta(minutes = 15)
        elif interval == '30m':
            startTime = endTime - datetime.timedelta(minutes = 30)
        elif interval == '1h':
            startTime = endTime - datetime.timedelta(minutes = 60)
        print(startTime)
        entries = table.query(IndexName='symbol-timestamp-index',
            KeyConditionExpression=Key('symbol').eq(symbol)&Key('timestamp').between(str(startTime), str(endTime)),
        )
        arr = entries['Items']
        if len(arr) == 0 or limit == endTime:
            bool = False
            break
        data = {}
        data["symbol"] = symbol
        data["startTime"] = arr[0]["timestamp"]
        data["endTime"]= arr[-1]["timestamp"]
        data["open"] = arr[0]["open"]
        data["close"] = arr[-1]["close"]
        high = -math.inf
        low = math.inf
        volume = 0
        for x in arr:
            high = max(high,float(x["high"]))
            low = min(low, float(x["low"]))
            volume = volume + float(x["volume"])
        data["high"] = high
        data["low"] = low
        data["volume"] = volume
        res.append(data)
        endTime = startTime
    return res