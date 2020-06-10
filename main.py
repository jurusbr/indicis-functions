from indicis import CDI,IPCA,DIFuturo
from datetime import date, datetime
import psycopg2
import locale
from dotenv import load_dotenv
from flask import escape
import logging
import os
import repository
import pytz

load_dotenv(verbose=True)

RUNNING_ENV = os.getenv("RUNNING_ENV")

def func_cdi(event, context):
    try:

        logging.info("========job cdi===========")
        logging.info(event)
        logging.info(context)

        data = get_current_time_sao_paulo()

        di, err_cdi = crawler_cdi(data)
        if err_cdi is not None:
            return err_cdi

        repo = repository.Repository()
        return repo.save_cdi(di,data)
    except Exception as e:
        return "Erro no job do cdi. Motivo:" + str(e)
  
def func_ipca(event, context):
    try:

        logging.info("========job ipca===========")
        logging.info(event)
        logging.info(context)

        cur_ipca, data , err_ipca= crawler_ipca()
        if err_ipca is not None:
            return err_ipca

        repo = repository.Repository()
        if repo.ipca_exists(data):
            msg = "IPCA {} ja cadastrado. Ignorar.".format(data)
            logging.info(msg)
            return msg
        else:
            return repo.save_ipca(cur_ipca,data)
        
    except Exception as e:
        return "Erro no job do cdi. Motivo:" + str(e)
  

def indicis_cloud_function(request):
    """
     Metodo invocado pelo request na api publica da cloud para realizar o crawler do cdi
    """
    logging.info("========indicis_cloud_function()===========")
    logging.info(request)

    if _is_cdi_request(request.args):

        try:
            data,err_arg = _to_date(request.args['data'])
            if err_arg is not None:
                return err_arg

            di, err_cdi = crawler_cdi(data)
            if err_cdi is not None:
                return err_cdi

            repo = repository.Repository()
            return repo.save_cdi(di,data)
        except Exception as e:
            return "Erro no request do cdi. Motivo:" + str(e)        

    else:
        return 'Server UP'

def _to_date(txtdata):
    try:
        if txtdata is None:
            return None, "Parametro data nao preenchido na requisicao"

        if len(txtdata)!=8:
            return None, "Formato da data errada. Formato correto =>  yyyymmdd"

        year = int(txtdata[:4])
        month = int(txtdata[4:6])
        day = int(txtdata[6:])
        logging.info("parameter data {} converted".format(txtdata))
        return (date(year,month,day), None)
    except Exception as e:
        logging.error(e)
        return None, "Erro no parsing da data. Formato correto => yyyymmd"  

def _is_cdi_request(request_args):
    if request_args and ('indice' in request_args) and (request_args['indice']=='cdi'):
        return True
    else:
        return False

def _is_ipca_request(request_args):
    if request_args and ('indice' in request_args) and (request_args['indice']=='ipca'):
        return True
    else:
        return False

def get_current_time_sao_paulo():
    tz = pytz.timezone('America/Sao_Paulo')
    return datetime.now(tz)
    
def crawler_cdi(data:date=None) -> (float, str):
    if data is None:
        data = date.today()
    try:
        logging.info("Crawling cdi da data {}".format(data.strftime('%m/%d/%Y')))
        cdi = CDI()
        di = cdi.crawler(data.year,data.month,data.day)
        return (di, None)
    except Exception as e:
        logging.error(e)
        return (-1, "Nao foi possivel fazer crawler do cdi na fonte")

def crawler_ipca() -> (float, date, str):
    try:
        logging.info("Crawling ipca")
        ipca = IPCA()
        data, mensal, _ = ipca.crawler()
        month_and_year = data.split("/")
        ipca_data = date(int(month_and_year[1]),int(month_and_year[0]),1)
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        valor = locale.atof(mensal)
        return (valor, ipca_data, None)
    except Exception as e:
        logging.error(e)
        return (None, None, "Nao foi possivel fazer crawler do cdi na fonte")



if __name__ == "__main__":
    #cur_cdi = crawler_cdi(date(2020,5,22))
    #print(cur_cdi)
    #_save_cdi(cur_cdi,date(2020,5,22))

    (cur_ipca, data, erro) = crawler_ipca()
    print(cur_ipca,data)
    repo = repository.Repository()
    if repo.ipca_exists(data):
        print("IPCA ja cadastrado")
    else:
        repo.save_ipca(cur_ipca,data)
    
