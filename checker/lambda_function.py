import json
import boto3
import constants
import datetime
from boto3.dynamodb.conditions import Key, Attr   
client = boto3.client('ses')
#SENDER = os.environ['SENDER_EMAIL']
AWS_REGION = "us-east-1"
CHARSET = "UTF-8"
dynamodb = boto3.resource('dynamodb')
def lambda_handler(event, context):
    table = dynamodb.Table('BINANCE_TICKS')
    num_markets = len(constants.symbols)
    num_exp_data_points = num_markets*1440
    endTime = datetime.datetime.strptime('2021-03-04 00:26','%Y-%m-%d %H:%M')
    startTime = endTime - datetime.timedelta(minutes = 1440)
    sum = 0
    for symbol in constants.symbols:
        entries = table.query(IndexName='symbol-timestamp-index',
                KeyConditionExpression=Key('symbol').eq(symbol)&Key('timestamp').between(str(startTime), str(endTime)),
            )
        sum = sum + len(entries['Items'])
    num_data_available = sum
    print(sum)
    print(num_exp_data_points)
    response = client.send_email(
            Destination={
                'ToAddresses': ['darshangolghate89@gmail.com'],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data':"""<title>Reporting Email</title></head><body style="font-family: Verdana,Tahoma, Malgun Gothic, Dotum, Geneva,Arial, sans-serif; font-size: 14px; color:#333;"><table width="1000" cellspacing="0" cellpadding="0" style="border: 1px solid #cbcbcc;"><tbody><tr bgcolor="#1d89e1" height="50px"><td width="1003" height="50"><table width="100%" border="0" cellspacing="0" cellpadding="0"><tbody><tr><td width="100%" height="20" align="left" style="padding-left:40px; font-size: 24px; color:#ffffff; vertical-align: baseline;">Mudrex</td></tr><tr><td width="100%" height="6" align="left" style="padding-left:40px; font-size: 12px; color:#ffffff; vertical-align: top;"></td></tr></tbody></table></td></tr><tr><td>&nbsp;</td></tr>
<tr><td height="50"><table width="1000" border="0" align="left" cellpadding="0" cellspacing="0"><tbody><tr><td width="40">&nbsp;</td><td>&nbsp;</td><td width="40">&nbsp;</td></tr><tr><td>&nbsp;</td><td><p style="line-height:28px;"> <b>Dear Admins,</b><br> <br> :</p></td><td>&nbsp;</td></tr></tbody></table></td></tr>

<tr><td height="50"><table width="1000" border="0" align="left" cellpadding="0" cellspacing="0"><tbody><tr><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr><tr><td width="40">&nbsp;</td><td width="921"><p style="line-height:28px;"> <b>Regards,</b><br> Mudrex System Admin</p></td><td width="39">&nbsp;</td></tr></tbody></table></td></tr><tr><td height="70" bgcolor="#ebeef1"><table width="100%" border="0" cellspacing="0" cellpadding="0"><tbody><tr><td height="25" align="left" style="padding-left:40px; color:#8d929a; font-size:12px; line-height:18px;">This Email has been sent automatically. Please do not respond to this Email<br></td></tr></tbody></table></td></tr></tbody></table></body></html>""",
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': "Mudrex Data Capture",
                },
            },
            Source='darshangolghate89@gmail.com',
        )
    print(response)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
