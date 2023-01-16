import boto3
import os
import subprocess
import sys
import json

import yfinance as yf

kinesis = boto3.client('kinesis', "us-east-2")

def lambda_handler(event, context):
    companies = ["AMZN", "BABA", "WMT", "EBAY", 
    "SHOP", "TGT", "BBY", "HD", "COST", "KR"]
    for company in companies:
        company_ticker = yf.Ticker(company)
        hist = company_ticker.history(start="2022-10-24", end="2022-11-05", interval = "5m")
        for index, row in hist.iterrows():
            dic = {"high":round(row["High"],2), "low":round(row["Low"],2), "volatility":round(row["High"] - row["Low"],2), "ts":index.strftime('%Y-%m-%d %X'), "name":company}
            
            as_jsonstr = json.dumps(dic)+"\n"
            output = kinesis.put_record(
                StreamName = "hagay-stream",
                Data=as_jsonstr,
                PartitionKey="partitionkey")
            print(as_jsonstr)
            
    return {
    'statusCode': 200,
    'body': json.dumps('Done!')
    }