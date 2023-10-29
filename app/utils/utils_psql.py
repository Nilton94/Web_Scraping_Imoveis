import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine

# Classe para realizar operações diversas nas bases do Postgre
class UtilsPSQL:
    def __init__(self, dbname = 'scraping'):
        try:
            self.dbname = dbname
            self.conn = psycopg2.connect(f"dbname = {self.dbname} user = {os.environ['USERNAME_PSQL']} password = {os.environ['PASSWORD_PSQL']}")
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f'O banco {self.dbname} não existe!')
            raise e


    def execute_query(self, query):
        '''
            Executa um comando sql na base selecionada
        '''
        
        try:
            self.conn.autocommit = True

            self.cur.execute(query)

            dados = self.cur.fetchall()

            # Cabeçalhos
            headers = [desc[0] for desc in self.cur.description]

            # Retorna dataframe
            df_query = pd.DataFrame(dados, columns = headers)

            return df_query
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Erro ao executar o comando: {e}")
            
        finally:
            # Fechando cursor e a conexão
            if self.conn:
                self.cur.close()
                self.conn.close()


    def create_drop_db (self, db_name: str, modo: str = 'create'):
        '''
            ## Método para criar ou deletar um banco de dados
            * Parâmetros:
                * modo: create ou drop --> str
                * db_name: caso o modo seja create, define o nome do novo db. Caso o modo seja drop, deleta o db passado
        '''
        
        try:
            # Autocommit para evitar transações concorrentes
            self.conn.autocommit = True

            # Bloco
            if modo.lower() == 'drop':
                query = f'''DROP DATABASE {db_name}'''
                
                self.cur.execute(query)

                print(f'Comando {modo.lower()} executado com sucesso para o db {db_name}!')

            elif modo.lower() == 'create':
                query = f'''CREATE DATABASE {db_name}'''
                
                self.cur.execute(query)
            
            else:
                print('Modo {modo} é inválido!')
        except (Exception, psycopg2.DatabaseError) as e:
            print(f'Erro eo executar o comando: {e}')
        
        finally:
            # Fechando cursor e a conexão
            if self.conn:
                self.cur.close()
                self.conn.close()


    def create_table(self, table_name: str, table_columns: dict):
        '''
            Cria uma tabela, no banco de dados selecionado, com base nos parâmetros passados.
            * Parâmetros:
                * table_name: nome da tabela --> str
                * table_columns: dicionário com nome e tipo de dado das colunas --> dict
        '''
        try:
            
            # Criando a consulta para criar a tabela
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            
            for coluna, tipo in table_columns.items():
                query += f"{coluna} {tipo}, "
            
            query = query.rstrip(", ") + ");"

            # Executando a query
            self.cur.execute(query)

            # Dando Commit
            self.conn.commit()

            print(f"Tabela '{table_name}' foi criada com sucesso!")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Erro ao criar a tabela: {error}")
            
        finally:
            # Fechando cursor e a conexão
            if self.conn:
                self.cur.close()
                self.conn.close()

    def drop_table(self, table_name: list, cascade = False):
        '''
            Função para apagar determinada tabela.
            Parâmetros:
                * table_name: lista com o nome das tabelas existente no db selecionado.
                * cascade: define se objetos vinculados também serão excluídos. default False.
        '''
        try:
            
            self.conn.autocommit = True
            
            # Criando a consulta para deletar as tabelas
            query = f"DROP TABLE IF EXISTS "
            
            for tabela in table_name:
                query += f"{tabela}, "
            
            query = query.rstrip(", ") + (" CASCADE;" if cascade == True else " RESTRICT;")

            # Executando a query
            self.cur.execute(query)

            # Dando Commit
            self.conn.commit()

            print(f"Tabela '{table_name}' foi deletada com sucesso!")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Erro ao deletar a tabela: {error}")
            
        finally:
            # Fechando cursor e a conexão
            if self.conn:
                self.cur.close()
                self.conn.close()


    def alter_table(self, table_name: str, operation: str, column_name: str = None, value: str = None):
        '''
            Função para alterar determinada tabela.
            Parâmetros:
                * table_name: nome da tabela existente no db selecionado a ser alterada.
                * operation: tipo de operação a ser feita com base nas opções possíveis
                    * ADD COLUMN {column_name} {datatype}: 
                        * Adiciona uma nova coluna com o tipo de dado especificado.
                    * DROP COLUMN {column_name}:
                        * Deleta a coluna especificada
                    * RENAME COLUMN {column_name} TO {new_column_name}:
                        * Renomeia uma coluna.
                    * RENAME TO {new_table_name}:
                        * Renomeia uma tabela.
                    * ALTER COLUMN {column_name} TYPE {new_data_type}:
                        * Altera o tipo de dado da coluna.
                    * Valores do parâmetro:
                        * add_column, drop_column, rename_column, rename_table, alter_column_dtype
                * value:
                    * Somente é usado nos casos de novo tipo de dado, novo name da coluna e novo nome de tabela
        '''        

        # Query base
        query = f"ALTER TABLE {table_name} "

        try:
            # if-else para finalizar a operação
            if operation == 'add_column':
                query += f'ADD COLUMN {column_name} {value};'
            elif operation == 'drop_column':
                query += f'DROP COLUMN {column_name};'
            elif operation == 'rename_column':
                query += f'RENAME COLUMN {column_name} TO {value};'
            elif operation == 'rename_table':
                query += f'RENAME TO {value}'
            elif operation == 'alter_column_dtype':
                query += f'ALTER COLUMN {column_name} TYPE {value};'
            else:
                raise ValueError(f"Operação inválida: {operation}")
            
            # Executando a query
            self.cur.execute(query)

            # Dando Commit
            self.conn.commit()

            print(f'A operação {operation} foi executada com sucesso!')

        except (Exception, psycopg2.DatabaseError) as e:
            return f'Houve um problema na execução do comando: {e}'
        
        finally:
            # Fechando cursor e a conexão
            if self.conn:
                self.cur.close()
                self.conn.close()

    def db_info(self, all_db = True):
        '''
            Retorna dados dos bancos de dados
            Parâmetros:
                * all_db: se True, retorna os nomes de todos os bancos de dados disponíveis. Caso contrário, retorna as tabelas e colunas do banco de dados escolhido.
        '''

        try:
            # Obtendo tabelas disponíveis e total de colunas no banco de dados selecionado
            if all_db:
                self.cur.execute(
                    f'''
                        SELECT 
                                DISTINCT db.datname db_name
                        FROM pg_database db
                        ORDER BY 1
                    '''
                )
            else:
                self.cur.execute(
                    f'''
                        SELECT 
                                db.datname db, 
                                c.table_name,
                                c.column_name
                        FROM pg_database db 
                        LEFT JOIN information_schema.columns c 
                                on (db.datname = c.table_catalog) 
                        WHERE 
                                db.datname = '{self.dbname}'
                                AND c.table_schema = 'public'
                        ORDER BY 1,2,3
                    '''
                )

            dados = self.cur.fetchall()

            # Cabeçalhos
            headers = [desc[0] for desc in self.cur.description]

            # Retorna dataframe
            db_info_df = pd.DataFrame(dados, columns = headers)

            return db_info_df
        
        except (Exception, psycopg2.DatabaseError) as e:
            return f'Houve um problema na execução do comando: {e}'
        
        finally:
            # Fechando cursor e a conexão
            if self.conn:
                self.cur.close()
                self.conn.close()

    def tables_info(self):
        '''
            Retorna as tabelas e o total de colunas de cada uma de acordo com o banco de dados 
        '''

        try:
            # Obtendo tabelas disponíveis e total de colunas no banco de dados selecionado
            self.cur.execute(
                '''
                    SELECT 
                            table_name tabela,
                            COUNT(DISTINCT column_name) colunas
                    FROM information_schema.columns
                    WHERE
                            table_schema = 'public'
                    GROUP BY 1
                    ORDER BY 1
                '''
            )

            dados = self.cur.fetchall()

            # Cabeçalhos
            headers = [desc[0] for desc in self.cur.description]

            # Retorna dataframe
            tables_info_df = pd.DataFrame(dados, columns = headers)

            return tables_info_df
        except (Exception, psycopg2.DatabaseError) as e:
            return f'Houve um problema na execução do comando: {e}'
        
    def columns_info(self, table_name, modo = 'full'):
        '''
            * Retorna informações da tabela selecionada.
            * Parâmetros:
                * modo:
                    * full --> retorna a tabela inteira como dataframe
                    * agg --> retorna dados da tabela, como colunas e data types.
            * Consultar o table_name no método UtilsPSQL().tables_info()
        '''

        if modo.lower() == 'full':
            try:
                # Obtendo tabelas disponíveis e total de colunas no banco de dados selecionado
                self.cur.execute(
                    f'''
                        SELECT 
                                * 
                        FROM {table_name}
                    '''
                )

                dados = self.cur.fetchall()

                # Cabeçalhos
                headers = [desc[0] for desc in self.cur.description]

                # Retorna dataframe
                columns_info_df = pd.DataFrame(dados, columns = headers)

                return columns_info_df
            except (Exception, psycopg2.DatabaseError) as e:
                return f'Houve um problema na execução do comando: {e}'
        elif modo.lower() == 'agg':
            try:
                # Obtendo tabelas disponíveis e total de colunas no banco de dados selecionado
                self.cur.execute(
                    f'''
                        SELECT 
                                table_name,
                                column_name,
                                data_type 
                        FROM information_schema.columns
                        WHERE
                                table_schema = 'public'
                                AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    '''
                )

                dados = self.cur.fetchall()

                # Cabeçalhos
                headers = [desc[0] for desc in self.cur.description]

                # Retorna dataframe
                columns_info_df = pd.DataFrame(dados, columns = headers)

                return columns_info_df
            except (Exception, psycopg2.DatabaseError) as e:
                return f'Houve um problema na execução do comando: {e}'
        else:
            print(f'Modo {modo} inválido. As opções são full e agg')
    

    def update_table(self, table_name: str, if_exists: str = 'append', df = None):
        '''
            - Adiciona dados do dataframe passado à tabela especificada
            - Parâmetros:
                * table_name: nome da tabela que receberá os dados.
                * if_exists: modo como os dados serão incluídos, i.e, se será uma adição (append), substituição (replace) ou a operação será abortada (fail)
        '''

        # Salvando no banco de dados
        engine = create_engine(f"postgresql://{os.environ['USERNAME_PSQL']}:{os.environ['PASSWORD_PSQL']}@localhost:5432/{self.dbname}")
        
        df.to_sql(
            f'{table_name}',
            con = engine,
            if_exists = f'{if_exists}',
            index = False
        )

        # Fechando conexão
        engine.dispose()

        return print(f'{df.shape[0]} registros foram salvos na tabela {table_name} do banco de dados {self.dbname}!')