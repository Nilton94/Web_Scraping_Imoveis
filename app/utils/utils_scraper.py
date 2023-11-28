from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
import time
import re
import requests
from random_user_agent.user_agent import UserAgent
import pandas as pd
import datetime
import pytz
from sqlalchemy import create_engine
from pandas import json_normalize
import os
from selenium.webdriver.chrome.service import Service
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager


class ScraperZap:
    
    # Constructor
    def __init__(self, transacao = 'aluguel', tipo = 'apartamentos', local = 'se+aracaju'):
        self.base_url = 'https://www.zapimoveis.com.br'
        self.transacao = transacao
        self.tipo = tipo
        self.local = local

    # Função para retornar número de páginas com base nos parâmetros passados
    def paginas(self):
        '''
            ### Objetivo
            * Função para retornar o total de páginas disponível com base na localidade e no tipo e subtipo escolhidos.
            ### Parâmetros
            #### Transação: 
                * Possui as opções ['aluguel', 'venda'].
            #### Tipo: 
                * RESIDENCIAL
                    * apartamentos
                    * studio
                    * quitinetes
                    * casas
                    * sobrados
                    * casas-de-condominio
                    * casas-de-vila
                    * cobertura
                    * flat
                    * loft
                    * terrenos-lotes-condominios
                    * fazendas-sitios-chacaras
                * COMERCIAL
                    * loja-salao
                    * conjunto-comercial-sala
                    * casa-comercial
                    * hoteis-moteis-pousadas
                    * andares-lajes-corporativas
                    * predio-inteiro
                    * terrenos-lotes-comerciais
                    * galpao-deposito-armazem
                    * box-garagem
            #### Local: 
                * {uf}+{cidade} -> o nome do estado é separado do nome da cidade pelo sinal de +. O nome da cidade, caso tenha espaços, deve ser separado com -
                * Ex: sp+sao-paulo
        '''
        
        # Definindo a URL
        url = f'{self.base_url}/{self.transacao}/{self.tipo}/{self.local}/?transacao={self.transacao}&pagina=1'

        # Definindo user agent aleatórios e headers da requisição
        ua = UserAgent()
        user_agents = ua.get_random_user_agent()
        headers = {'user-agent': user_agents.strip(), 'encoding':'utf-8'}

        # Requisição para obter total de imóveis disponíveis
        try:
            # Requisição
            r = requests.get(url, headers = headers)

            # Obtenção do total de imóveis disponíveis na url analisada
            soup = BeautifulSoup(r.text, 'html.parser')
            res = soup.find('div', {"class":"result-wrapper__title"}).text # listing-wrapper__title para result-wrapper__title em 22.11.23

            imoveis = int(re.sub('[^0-9]','',res))
            imoveis_pagina = imoveis//100 if imoveis//100 > 1 else 1

            resultado = {
                'Parametros': {'Transacao': self.transacao, 'Tipo': self.tipo, 'Local': self.local},
                'Requisicao': {'Status': r.status_code, 'Reason': r.reason, 'OK': r.ok},
                'Imoveis': imoveis, 
                'Paginas': imoveis_pagina
            }

            # Retornando possível quantidade de páginas (em geral, cada página tem aproximadamente 100 imóveis)
            return resultado
        
        except:
            # Requisição
            r = requests.get(url, headers = headers)
            resultado = {
                'Parametros': {'Transacao': self.transacao, 'Tipo': self.tipo, 'Local': self.local},
                'Requisicao': {'Status': r.status_code, 'Reason': r.reason, 'OK': r.ok},
                'Imoveis': None,
                'Paginas': None
            }

            return resultado
    
    # Função para retornar o html das páginas disponíveis com base nos parâmetros passados
    def scraping(self, workers: int = 3):
        '''
        Função para retornar os dados de imóveis com base na localidade e no tipo e subtipo escolhidos
            * Parâmetros
                * Valores possíveis para o tipo: 
                    * RESIDENCIAL
                        * apartamentos
                        * studio
                        * quitinetes
                        * casas
                        * sobrados
                        * casas-de-condominio
                        * casas-de-vila
                        * cobertura
                        * flat
                        * loft
                        * terrenos-lotes-condominios
                        * fazendas-sitios-chacaras
                    * COMERCIAL
                        * loja-salao
                        * conjunto-comercial-sala
                        * casa-comercial
                        * hoteis-moteis-pousadas
                        * andares-lajes-corporativas
                        * predio-inteiro
                        * terrenos-lotes-comerciais
                        * galpao-deposito-armazem
                        * box-garagem
                        
                * Local: {uf}+{cidade} -> o nome do estado é separado do nome da cidade pelo sinal de +. O nome da cidade, caso tenha espaços, deve ser separado com -
                    Ex: sp+sao-paulo
        '''
        
        # Resposta do método paginas
        _paginas = ScraperZap(self.transacao, self.tipo, self.local).paginas()

        # Páginas: a página do zapimóveis não mostra mais que 100 páginas por vez.
        pagina = int(_paginas['Paginas']) + 2 if int(_paginas['Paginas']) < 100 else 101

        # Lista para guardar o html de cada página
        html_pagina = []

        def get_html(paginas):

            # browser

            browser = webdriver.Chrome(ChromeDriverManager().install())

            browser.get(f'{self.base_url}/{self.transacao}/{self.tipo}/{self.local}/?transacao={self.transacao}&pagina={paginas}')
            
            time.sleep(2)

            # Rola até o fim da página para que todos os cards apareçam
            total_height = int(browser.execute_script("return document.body.scrollHeight"))
            n = 1

            while n < total_height:
                browser.execute_script(f"window.scrollTo(0, {n});")
                n += 90
                total_height = int(browser.execute_script("return document.body.scrollHeight"))

            time.sleep(2)

            resultado = browser.find_element(By.XPATH, '//*')
            source_code = resultado.get_attribute("innerHTML")

            browser.quit()

            return source_code

        if _paginas['Requisicao']['OK']:
            with concurrent.futures.ThreadPoolExecutor(max_workers = workers) as executor:
                # Criando a sequência de tasks que serão submetidas para a thread pool
                urls = {executor.submit(get_html, pagina): pagina for pagina in range(1,pagina)}
                
                # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
                for future in concurrent.futures.as_completed(urls):
                    url = urls[future]
                    try:
                        resultado = future.result()
                        html_pagina.append(resultado)
                    except Exception as exc:
                        print(f'{url} com erro: {exc}')
                        
            return html_pagina
        else:
            return _paginas
   
    # Função para tratar os dados das páginas obtidos conforme os parâmetros passados
    def tratamento_scraping(self, db_name: str = 'scraping', table_name: str = 'dados_imoveis_raw', if_exists: str = 'append'):
        '''
            ### Objetivo:
            * Função para tratar os dados obtidos com a função scraping, salvando os dados na tabela especificada.
            * Recebe como parâmetro a função scraping com os devidos parâmetros de localização, tipo e subtipo.
            ### Parâmetros:
            * if_exists: {fail, replace, append}
        '''
        # Resposta do método paginas
        _paginas = ScraperZap(self.transacao, self.tipo, self.local).paginas()

        if _paginas['Requisicao']['OK']:
            
            # Lista com os html das páginas disponíveis
            lista_html = ScraperZap(self.transacao, self.tipo, self.local).scraping()

            # Lista para guardas os dados
            dados = []

            # Foor loop que passará por cada página
            for html in range(len(lista_html)):

                try:
                    soup = BeautifulSoup(lista_html[html], 'html.parser')

                    res = soup.find('div', {"class":"listing-wrapper__content"})

                    for i in res:
                        
                        # Transação
                        transacaof = self.transacao

                        # Local
                        localf = self.local

                        # Tipo
                        tipof = self.tipo

                        # Subtipo
                        subtipof = 'Residencial' if tipof in ['apartamentos','studio','quitinetes','casas','sobrados','casas-de-condominio','casas-de-vila','cobertura','flat','loft','terrenos-lotes-condominios','fazendas-sitios-chacaras'] else 'Comercial'
                        
                        # id do imóvel
                        try:
                            id = i.find('div','result-card').find('a').get('data-id')
                        except:
                            id = 'Sem info'

                        # URL
                        try:
                            url_imo = i.find('div','result-card').find('a').get('href')
                        except:
                            url_imo = 'Sem info'

                        # Base de busca
                        try:
                            base = re.findall(r'www\.(.*?)\.com', url_imo)[0]
                        except:
                            base = 'Sem info'

                        # Se é destaque ou nao
                        try:
                            destaque = i.find('div',{'class':'l-tag-card__content'}).text
                            destaque.replace('\n','')
                        except:
                            destaque = 'Sem destaque'

                        # Titulo/Bairro
                        try:
                            bairro = i.find('div',{'data-cy':'card__address'}).text if i.find('div',{'data-cy':'card__address'}).text != '' else 'Sem info'
                        except:
                            bairro = 'Sem info'

                        # Endereço
                        try:
                            endereco = i.find('p', class_='card__street').text
                        except:
                            endereco = 'Sem info'

                        # Descricao
                        try:
                            descricao = i.find('p',{'class':'card__description'}).text
                        except:
                            descricao = 'Sem info'

                        # Área
                        try:
                            area = float(re.sub('[^0-9]', '', i.find('p', {'itemprop':'floorSize'}).text))
                        except:
                            area = 0.0

                        # Quartos
                        try:
                            quartos = float(re.sub('[^0-9]', '', i.find('p', {'itemprop':'numberOfRooms'}).text))
                        except:
                            quartos = 0.0

                        # Chuveiros
                        try:
                            chuveiros = float(re.sub('[^0-9]', '', i.find('p', {'itemprop':'numberOfBathroomsTotal'}).text))
                        except:
                            chuveiros = 0.0

                        # Garagens
                        try:
                            # garagens = 0 if len(i.find('p', {'itemprop':'numberOfBathroomsTotal'}).find_next('p').text) >= 5 else i.find('p', {'itemprop':'numberOfBathroomsTotal'}).find_next('p').text
                            if len(i.find('p', {'itemprop':'numberOfBathroomsTotal'}).find_next('p').text) > 1:
                                garagens = 0.0
                            else:
                                garagens = float(re.sub('[^0-9]', '', i.find('p', {'itemprop':'numberOfBathroomsTotal'}).find_next('p').text))
                        except:
                            garagens = 0.0

                        # Aluguel
                        listing_price = i.find('div', {'class':'listing-price'})
                        try:
                            aluguel = float(re.sub('[^0-9]', '', listing_price.find_all('p')[1].text.strip()))
                        except:
                            aluguel = 0.0

                        # Total
                        try:
                            total = float(re.sub('[^0-9]', '', listing_price.find_all('p')[0].text.strip()))
                        except:
                            total = 0.0

                        # Valor abaixo
                        try:
                            valor_abaixo = listing_price.find_all('p')[2].text.strip()
                        except:
                            valor_abaixo = 'Sem info'

                        # Data completa de Extração
                        data = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d")

                        # Mês de Extração
                        mes = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%m")

                        # Ano de Extração
                        ano = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%Y")

                        # Dia de Extração
                        dia = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%d")

                        # Salvando os dados dos imóveis de cada página na lista auxiliar
                        dados.append(
                            [
                                transacaof,
                                base,
                                localf,
                                tipof,
                                subtipof,
                                id,
                                url_imo,
                                destaque,
                                bairro,
                                endereco,
                                descricao,
                                area,
                                quartos,
                                chuveiros,
                                garagens,
                                aluguel,
                                total,
                                valor_abaixo,
                                data,
                                mes,
                                dia,
                                ano
                            ]
                        )
                except:
                    continue

            
            # Salvando os dados no dataframe final
            df = pd.DataFrame(
                dados,
                columns = [
                    'transacao',
                    'base',
                    'local',
                    'tipo',
                    'subtipo',
                    'id',
                    'url',
                    'destaque',
                    'bairro',
                    'endereco',
                    'descricao',
                    'area',
                    'quartos',
                    'chuveiros',
                    'garagens',
                    'aluguel',
                    'total',
                    'valor_abaixo',
                    'data',
                    'mes',
                    'dia',
                    'ano'
                ]
            )

            # Dropando linhas sem id do imóvel e duplicados com base na transação, id e mês de obtenção do dado
            df = (
                    df
                    .drop(index = df[(df['id'].isnull()) | (df['id'] == 'Sem info')].index)
                    .drop_duplicates(subset = ['transacao','id','ano','mes'], ignore_index = True)
            )

            # try:
            #     # Salvando no banco de dados
            #     engine = create_engine(f"postgresql://{os.environ['USERNAME_PSQL']}:{os.environ['PASSWORD_PSQL']}@localhost:5432/{db_name}")
                
            #     df.to_sql(
            #         f'{table_name}',
            #         con = engine,
            #         if_exists = f'{if_exists}',
            #         index = False
            #     )

            #     # Fechando conexão
            #     engine.dispose()
            # except:
            #     pass

            # Salvando como Parquet
            current_dir = os.getcwd()

            # Path do diretório para salvar dos dados
            # data_dir = os.path.join(current_dir, 'data', 'bronze') 
            # Resolver o problema de path no streamlit cloud e poder testar localmente
            data_dir = os.path.join(current_dir, 'data', 'bronze') if os.getcwd().__contains__('app') else os.path.join(current_dir, 'app', 'data', 'bronze')

            # Criando o diretório caso nao exista
            os.makedirs(data_dir, exist_ok  = True)

            # Path do arquivo
            parquet_file_path = os.path.join(data_dir, 'dados_imoveis_raw.parquet') if os.getcwd().__contains__('app') else os.path.join(data_dir, 'dados_imoveis_raw.parquet').replace('\\','/')

            # Pyarrow table
            pa_df = pa.Table.from_pandas(df)
            pa_schema = pa.schema(
                    [
                        pa.field('transacao', pa.string()),
                        pa.field('base', pa.string()),
                        pa.field('local', pa.string()),
                        pa.field('tipo', pa.string()),
                        pa.field('subtipo', pa.string()),
                        pa.field('id', pa.string()),
                        pa.field('url', pa.string()),
                        pa.field('destaque', pa.string()),
                        pa.field('bairro', pa.string()),
                        pa.field('endereco', pa.string()),
                        pa.field('descricao', pa.string()),
                        pa.field('area', pa.float64()),
                        pa.field('quartos', pa.float64()),
                        pa.field('chuveiros', pa.float64()),
                        pa.field('garagens', pa.float64()),
                        pa.field('aluguel', pa.float64()),
                        pa.field('total', pa.float64()),
                        pa.field('valor_abaixo', pa.string()),
                        pa.field('data', pa.string()),
                        pa.field('mes', pa.string()),
                        pa.field('dia', pa.string()),
                        pa.field('ano', pa.string())
                    ]
                )
            
            # Salvando como Parquet
            pq.write_to_dataset(
                pa_df, 
                root_path = parquet_file_path, 
                partition_cols = ['ano','mes','dia'],
                schema = pa_schema
            )
            
            timestamp = str(datetime.datetime.now(tz = None))

            return print(f'{timestamp} - Dados de {df.shape[0]} imóveis de {self.transacao} da cidade de {self.local}, do tipo {self.tipo}, foram salvos na tabela {table_name} do banco de dados {db_name} e no diretório {parquet_file_path}!')
        else:
            return _paginas
    
    # Função para executar os scraping usando múltiplos valores, usando concurrent.futures para executar tasks de forma concorrente
    def scraping_multiple(self, _transacao: list = ['aluguel'], _tipo: list = ['apartamentos'], _local: list = ['se+aracaju'], workers: int = 2):
        '''
            ### Objetivo
            Realizar o scraping de dados de imóveis usando múltiplos valores para os parâmetros de transação, local e tipo.
            A função usa o método threadpool da lib concurrent.futures para executar múltiplas tasks de forma concorrente, com base no número de workers.

            ### Parâmetros
            #### _transacao
            * Define qual a transação de imóveis.
            * Valores: aluguel e venda
            #### _tipo:
            * Recebe os tipos de imóveis desejados.
            * Valores:
                    * RESIDENCIAL
                        * apartamentos
                        * studio
                        * quitinetes
                        * casas
                        * sobrados
                        * casas-de-condominio
                        * casas-de-vila
                        * cobertura
                        * flat
                        * loft
                        * terrenos-lotes-condominios
                        * fazendas-sitios-chacaras
                    * COMERCIAL
                        * loja-salao
                        * conjunto-comercial-sala
                        * casa-comercial
                        * hoteis-moteis-pousadas
                        * andares-lajes-corporativas
                        * predio-inteiro
                        * terrenos-lotes-comerciais
                        * galpao-deposito-armazem
                        * box-garagem
            #### _local:
            * Recebe os locais dos imóveis buscados.
            * Valor no formato: {uf}+{nome}-{da}-{cidade}
            * Exemplo: se+aracaju, sp+sao-paulo
            #### workers:
            * Número de workers selecionados para a ThreadPool que irá executar as tasks de forma concorrente.
        '''

        # Função auxiliar
        def auxiliar(transacao, tipo, local):
            return ScraperZap(transacao = transacao, tipo = tipo, local = local).tratamento_scraping(if_exists = 'append')

        # Criando thread pool para executar tasks de forma concorrente
        with concurrent.futures.ThreadPoolExecutor(max_workers = workers) as executor:
            # Criando a sequência de tasks que serão submetidas para a thread pool
            urls = {executor.submit(auxiliar, transacao, tipo, local): transacao for transacao in _transacao for tipo in _tipo for local in _local}
            
            # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
            for future in concurrent.futures.as_completed(urls):
                url = urls[future]
                try:
                    resultado = future.result()
                    # print(f'{url} OK')
                except Exception as exc:
                    print(f'Erro na operação: {exc}')
    
    # Função para checar o total de imóveis disponíveis, tanto para venda quanto para aluguel, na cidade especificada. 
    # def check_cidades(self, db_name: str = 'scraping', table_name: str = 'disponibilidade_municipios', if_exists: str = 'append', modo:str = 'cidade', cidade: str = 'se+aracaju', estado: str = None):
    #     '''
    #         ### Objetivo
    #         * Checa, de forma geral, o total de imóveis de aluguel ou venda disponíveis no local especificado.
    #         * Salva os dados consultados na base de dados que guarda a disponibilidade de imóveis de venda e locação.
    #         * Retorna um dicionário com dados de venda e aluguel
    #         ### Parâmetros
    #         #### db_name:
    #             * Base de dados onde será salvo os dados da consulta.
    #         #### table_name:
    #             * Tabela onde a consulta será salva.
    #         #### if_exists:
    #             * Forma como os dados serão salvos na base
    #             * Valores: fail, replace, append
    #         #### modo:
    #             * Define como a consulta será feita, i.e, se será buscado dados específicos de uma cidade ou de um estado inteiro
    #             * Valores: cidade, estado
    #         #### cidade/estado:
    #             * Valores passados a depender do modo escolhido
    #             * Se a opção for cidade, os valores devem ser passados no seguinte formato: {uf}+{cidade}. Espaços em branco devem ser separados por - no nome da cidade.
    #     '''

    #     # Função auxiliar
    #     def disponibilidade(cidade):
    #         # Definindo user agent aleatórios e headers da requisição
    #         ua = UserAgent()
    #         user_agents = ua.get_random_user_agent()
    #         headers = {'user-agent': user_agents.strip(), 'encoding':'utf-8'}

    #         # Definindo a URL
    #         url = [f'{self.base_url}/{transacao}/imoveis/{cidade}/?&transacao={transacao}&pagina=1' for transacao in ['aluguel','venda']]

    #         # Data
    #         data = str(datetime.datetime.now(tz = None))

    #         # Requisição
    #         r = [requests.get(url, headers = headers) for url in url]

    #         # Obtenção do html
    #         soup = [BeautifulSoup(r.text, 'html.parser') for r in r]
            
    #         # Dados de aluguel
    #         try:
    #             res = soup[0].find('div', {"class":"listing-wrapper__title"}).text

    #             # Tratamento do html
    #             imoveis = float(re.sub('[^0-9]','',res))
    #             imoveis_pagina = imoveis//100 if imoveis//100 > 1 else 1

    #             # Dict com o resultado
    #             aluguel = {
    #                 'Aluguel': {
    #                     'Requisicao': {'Status': r[0].status_code, 'Reason': r[0].reason, 'OK': r[0].ok},
    #                     'Imoveis': imoveis,
    #                     'Paginas': imoveis_pagina
    #                 }
    #             }
    #         except:
    #             # Dict com o resultado
    #             aluguel = {
    #                 'Aluguel': {
    #                     'Requisicao': {'Status': r[0].status_code, 'Reason': r[0].reason, 'OK': r[0].ok},
    #                     'Imoveis': None,
    #                     'Paginas': None
    #                 }
    #             }

    #         # Dados de venda
    #         try:
    #             res = soup[1].find('div', {"class":"listing-wrapper__title"}).text

    #             # Tratamento do html
    #             imoveis = float(re.sub('[^0-9]','',res))
    #             imoveis_pagina = imoveis//100 if imoveis//100 > 1 else 1

    #             # Dict com o resultado
    #             venda = {
    #                 'Venda': {
    #                     'Requisicao': {'Status': r[1].status_code, 'Reason': r[1].reason, 'OK': r[1].ok},
    #                     'Imoveis': imoveis,
    #                     'Paginas': imoveis_pagina
    #                 }
    #             }
    #         except:
    #             # Dict com o resultado
    #             venda = {
    #                 'Venda': {
    #                     'Requisicao': {'Status': r[1].status_code, 'Reason': r[1].reason, 'OK': r[1].ok},
    #                     'Imoveis': None,
    #                     'Paginas': None
    #                 }
    #             }
            
    #         # Parametros
    #         parametros = {'Local': cidade, 'Data':data}
    #         resultado = parametros | aluguel | venda

    #         # Dataframe
    #         df = (
    #             json_normalize(resultado)
    #             .rename(columns = {'Local':'str_local','Data':'data','Aluguel.Imoveis':'imoveis_aluguel', 'Venda.Imoveis':'imoveis_venda'})
    #             .filter(['str_local','imoveis_aluguel','imoveis_venda','data'], axis = 'columns')
    #         )

    #         try:
    #             # Salvando na tabela de disponibilidade_municipios
    #             engine = create_engine(f"postgresql://{os.environ['USERNAME_PSQL']}:{os.environ['PASSWORD_PSQL']}@localhost:5432/{db_name}")
                
    #             df.to_sql(
    #                 f'{table_name}',
    #                 con = engine,
    #                 if_exists = f'{if_exists}',
    #                 index = False
    #             )

    #             print(f'Dados de venda e aluguel da cidade {cidade} salvos na tabela {table_name}!')

    #              # Fechando conexão
    #             engine.dispose()
    #         except:
    #             pass

    #         return resultado

    #     if modo.lower().strip() == 'cidade':
    #         # Chamando função auxiliar
    #         res = disponibilidade(cidade = cidade)

    #         return res

    #     elif modo.lower().strip() == 'estado':
    #         # Dados geográficos
    #         dados_municipios = UtilsPSQL().execute_query(
    #             f'''
    #                 SELECT 
    #                         DISTINCT str_local,
    #                         str_uf,
    #                         str_regiao
    #                 FROM imoveis_municipios
    #                 WHERE str_uf = '{estado.upper()}'
    #                 ORDER BY str_uf, str_local
    #             '''
    #         )

    #         res = []
    #         # Lista de locais
    #         locais = list(map(lambda x: x ,dados_municipios['str_local']))
            
    #         # Criando thread pool com 5 workers para executar tasks de forma concorrente
    #         with concurrent.futures.ThreadPoolExecutor(max_workers = 2) as executor:
    #             # Criando a sequência de tasks que serão submetidas para a thread pool
    #             urls = {executor.submit(disponibilidade, local): local for local in locais}
                
    #             # Loop para executar as tasks de forma concorrente. Também seria possível criar uma list comprehension que esperaria todos os resultados para retornar os valores.
    #             for future in concurrent.futures.as_completed(urls):
    #                 url = urls[future]
    #                 try:
    #                     resultado = future.result()
    #                     res.append(resultado)
    #                     # print(f'{url} OK')
    #                 except Exception as exc:
    #                     print(f'{url} com erro: {exc}')
            
    #         return res
    #     else:
    #         print(f'Modo {modo} não existe. Escolha uma opção válida de modo!')