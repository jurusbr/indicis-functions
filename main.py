from indicis import CDI,IPCA,DIFuturo
import service
from datetime import date, datetime, timedelta
import psycopg2
import locale
from dotenv import load_dotenv
from flask import escape
import logging
import os
import repository
import base64
import sys

load_dotenv(verbose=True)

RUNNING_ENV = os.getenv("RUNNING_ENV")

def func_main(event, context):
    payload = base64.b64decode(event['data']).decode('utf-8')
    logging.info("trigger with payload %s" % payload)     

    svc = service.Service()
    svc.crawler(payload)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) == 1:
        print("Necessario passar o indice como argumento. Exemplo: python main.py cdi")        
    else:
        svc = service.Service()
        svc.crawler(sys.argv[1])
