import logging
import sqlalchemy
import os
from datetime import date

RUNNING_ENV = os.getenv("RUNNING_ENV")
POSTGRES = os.getenv("POSTGRES")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

class Repository:
    def __init__(self):
        self.engine = self.create_engine()

    def get_map_indices(self) -> dict:
        try:
            logging.info("Carregando os tipos de indices e seus ids")
            conn = self.engine.connect()
            logging.info("Connection opened")
            stmt = sqlalchemy.text("SELECT id,name FROM tpindice order by id")
            result_set = self.engine.execute(stmt) 
            types = {}
            for row in result_set:     
                types[row[1]]=row[0]
            conn.close()
            return types
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel carregar os tipos de indices") 

    def save_indice(self, indice_id: int, valor, data: date):
        try:
            logging.info("Saving indice {} date {}".format(str(indice_id), data))
            conn = self.engine.connect()
            logging.info("Connection opened")
            metadata = sqlalchemy.MetaData(schema="public")
            tbindice = sqlalchemy.Table('indice', metadata, autoload=True, autoload_with=self.engine)
            ins = tbindice.insert().values(tpindice=indice_id, valor=valor, date=data)
            conn.execute(ins)
            logging.info("Inserido na base com sucesso.")
            conn.close()
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel inserir a base")   

    def save_curve(self, indice_id: int, curve, dtreference):
        try:
            logging.info("Saving futuro at {}".format(date))
            conn = self.engine.connect()
            logging.info("Connection opened")
            metadata = sqlalchemy.MetaData(schema="public")
            tbfuture = sqlalchemy.Table('future', metadata, autoload=True, autoload_with=self.engine)
            for index in curve:                
                ins = tbfuture.insert().values(tpindice=indice_id,valor=index[2],dtreference=dtreference,dtcode=index[1],code=index[0])
                logging.info("Insert nova linha em futuro")
                conn.execute(ins)
            logging.info("Inserido na base com sucesso.")
            conn.close()
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel inserir o futuro na base")   
    
    def save_cdi(self,cdi,date):
        try:
            logging.info("Saving cdi {} date {}".format(cdi,date))
            conn = self.engine.connect()
            logging.info("Connection opened")
            metadata = sqlalchemy.MetaData(schema="public")
            tbindice = sqlalchemy.Table('indice', metadata, autoload=True, autoload_with=self.engine)
            ins = tbindice.insert().values(tpindice=1,valor=cdi,date=date)
            logging.info("Insert cursor created")
            conn.execute(ins)
            logging.info("Inserido na base com sucesso.")
            conn.close()
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel inserir o cdi na base")   

    def save_ipca(self,ipca,date):
        try:
            logging.info("Saving ipca {} date {}".format(ipca,date))
            conn = self.engine.connect()
            logging.info("Connection opened")
            metadata = sqlalchemy.MetaData(schema="public")
            tbindice = sqlalchemy.Table('indice', metadata, autoload=True, autoload_with=self.engine)
            ins = tbindice.insert().values(tpindice=3,valor=ipca,date=date)
            logging.info("Insert cursor created")
            conn.execute(ins)
            logging.info("Inserido na base com sucesso.")
            conn.close()
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel inserir o cdi na base") 
    
    def save_di_futuro(self,curve,dtreference):
        try:
            logging.info("Saving di futuro at {}".format(date))
            conn = self.engine.connect()
            logging.info("Connection opened")
            metadata = sqlalchemy.MetaData(schema="public")
            tbfuture = sqlalchemy.Table('future', metadata, autoload=True, autoload_with=self.engine)
            for index in curve:                
                ins = tbfuture.insert().values(tpindice=2,valor=index[2],dtreference=dtreference,dtcode=index[1],code=index[0])
                logging.info("Insert nova linha em di futuro")
                conn.execute(ins)
            logging.info("Inserido na base com sucesso.")
            conn.close()
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel inserir o cdi na base")   

    
    
    def exists(self, idIndice:int, data:date) -> bool:
        try:
            logging.info("Check if indice {} exist at {}".format(idIndice,date))
            conn = self.engine.connect()
            logging.info("Connection opened")
            stmt = sqlalchemy.text("SELECT id FROM indice where tpindice=:tpindice and date=:date union SELECT id FROM future where tpindice=:tpindice and dtreference=:date")
            stmt = stmt.bindparams(date=data)
            stmt = stmt.bindparams(tpindice=idIndice)

            result_set = self.engine.execute(stmt) 

            logging.info("Resultado {}".format(result_set.rowcount))
            conn.close()
            return result_set.rowcount>0
        except Exception as e:
            conn.close()
            logging.error(str(e))
            raise Exception("Nao foi possivel inserir o indice na base") 



    def create_engine(self):
        if RUNNING_ENV=='cloud':
            logging.info("Creating engine cloud")
            return sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL(
                    drivername='postgres+pg8000',
                    username="dbuser",
                    password="frhnd2330",
                    database="postgres",
                    query={
                        'unix_sock': '/cloudsql/{}/.s.PGSQL.5432'.format("jurus-01:southamerica-east1:sql-dev")
                    }
                ),
                pool_size=5,
                max_overflow=2,
                pool_timeout=30,  
                pool_recycle=1800,
            )
        else:
            logging.info("Creating engine local")
            return sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL(
                    drivername='postgres+pg8000',
                    username="postgres",
                    password="Postgres2018!",
                    database="postgres",
                    host="localhost"
                ),
                pool_size=5,
                max_overflow=2,
                pool_timeout=30,  
                pool_recycle=1800,
            )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo = Repository()
    repo.get_map_indices()
