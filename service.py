import repository
import datetime
import logging
import locale

class Service:

    def __init__(self):
        self.repo = repository.Repository()
        self.map_indices = self.repo.get_map_indices()

    def crawler(self, indice_name: str):
        
        indice_id = self.map_indices[indice_name.lower()]
        data = datetime.date.today()

        if self.indice_cadastrado(indice_id, data):
            logging.info("{} {} ja cadastrado, abortar execucao.".format(indice_name, data))
            return
                
        crawler_class = self.dynamic_import("indicis.%s" % indice_name)
        crawler = crawler_class()
        
        if indice_name == 'IPCA':
            data_crawled, mensal, _ = crawler.crawler()
            month_and_year = data_crawled.split("/")
            ipca_data = datetime.date(int(month_and_year[1]),int(month_and_year[0]),1)
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
            valor = locale.atof(mensal)
            self.repo.save_indice(indice_id, valor, ipca_data)
        else:
            result = crawler.crawler()
            if isinstance(result, dict):
                print(result)
                curve = self.complete_curve(result)
                if curve:
                    self.repo.save_curve(indice_id, curve, data)
                else:
                    logging.info("Crawler %s return a empty dict, nothing to do" % indice_name)
            else:
                self.repo.save_indice(indice_id, result, data)

    def complete_curve(self, curve):
        wordmap = {"F":1, "G":2, "H":3, "J":4, "K":5, "M":6, "N":7, "Q":8, "U":9, "V":10,"X":11,"Z":12}
        
        curve_with_dates = []
        for code in curve:
            month = code[3:4]
            year = "20"+code[4:]
            dtKey = datetime.date(int(year),wordmap[month],1)
            curve_with_dates.append((code,dtKey,curve[code]))
        return curve_with_dates

    def indice_cadastrado(self, indice_id: int, data: datetime.date):
        return self.repo.exists(indice_id, data)
    
    def dynamic_import(self, name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod