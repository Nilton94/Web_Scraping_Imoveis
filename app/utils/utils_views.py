import pandas as pd
import streamlit as st
import datetime
import re
import plotly.express as px
import os
from utils.utils_scraper import ScraperZap

class StViews():
    def __init__(self, local: str = None, tipo: list = None, ranking: int = None):
        self.local = local
        self.tipo = tipo
        self.ranking = ranking

    def check_base(self):
        '''
            ### Objetivo:
            * Avalia a requisição no app e retorna um dataframe baseado nos resultados.
            ### Metodologia:
            * Caso o local selecionado já exista na base criada nos últimos 2 dias, o método checa se todos os tipos de imóveis selecionados pelo usuário constam na base.
            * Caso falte algum tipo de imóvel, o crawler é rodado especificamente para esse tipo, e então o dataframe final é retornado.
            * Em caso de o local não constar na base, o crawler é rodado normalmente para todos os parâmetros passados.
        '''
        
        try:
            # Carregando a base criada nos últimos 2 dias para checar se satisfaz o filtro passado
            df = pd.read_parquet(
                    (os.path.join(os.getcwd(),'data','bronze','dados_imoveis_raw.parquet') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','bronze','dados_imoveis_raw.parquet').replace('\\','/')),
                    # os.path.join(os.getcwd(),'app','data','bronze','dados_imoveis_raw.parquet').replace('\\','/'),
                    # filters = [
                    #     ("ano", "=", int((datetime.datetime.now(tz = None) - datetime.timedelta(0)).strftime("%Y"))), 
                    #     ("mes", "=", int((datetime.datetime.now(tz = None) - datetime.timedelta(0)).strftime("%m"))), 
                    #     ("dia", ">=", int((datetime.datetime.now(tz = None) - datetime.timedelta(0)).strftime("%d")))
                    # ]
                )
            
            # Checando se o local passado consta na base
            check_local = self.local in list(df['local'].unique())

            # Checando quais tipos de imóveis não constam na base
            check_tipos = [tipo for tipo in self.tipo if tipo not in list(df.loc[df['local'].isin([self.local]),'tipo'].unique())]

            if not check_local:
                # Scraping dos dados do local passado
                ScraperZap().scraping_multiple(
                    _transacao = ['aluguel'], 
                    _tipo = self.tipo, 
                    _local = [self.local]
                )

                # Base obtido na data atual conforme parâmetros passados
                df = pd.read_parquet(
                    (os.path.join(os.getcwd(),'data','bronze','dados_imoveis_raw.parquet') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','bronze','dados_imoveis_raw.parquet').replace('\\','/'))
                    # os.path.join(os.getcwd(),'app','data','bronze','dados_imoveis_raw.parquet').replace('\\','/'),
                    # filters = [
                    #     ("ano", "=", int(datetime.datetime.now(tz = None).strftime("%Y"))), 
                    #     ("mes", "=", int(datetime.datetime.now(tz = None).strftime("%m"))), 
                    #     ("dia", "=", int(datetime.datetime.now(tz = None).strftime("%d")))
                    # ]
                )
                
                # Limpando eventuais duplicados e filtrando apenas o local e tipos selecionados
                df = (
                        df
                        .loc[
                            (df.local.isin([self.local])) 
                            & (df.tipo.isin(self.tipo))
                            & (df.transacao == 'aluguel')
                        ]
                        .drop_duplicates(subset = ['local','id'], ignore_index = True)
                )

                # Transformando valores zerados em null para não afetar os cálculos média (schema do Arrow não aceita colunas com valores null)
                df.replace(0.0, None, inplace = True)
                df['bairro'] = df['bairro'].replace(to_replace= '', value = 'Sem info')

                # Retornando o dataframe
                return df

            elif check_local and len(check_tipos) > 0:
                # Scraping dos dados do local passado e dos tipos faltantes
                ScraperZap().scraping_multiple(
                    _transacao = ['aluguel'], 
                    _tipo = check_tipos,
                    _local = [self.local]
                )

                # Base dos últimos 2 dias, mais os tipos ausentes na consulta atual
                df = pd.read_parquet(
                    (os.path.join(os.getcwd(),'data','bronze','dados_imoveis_raw.parquet') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(),'app','data','bronze','dados_imoveis_raw.parquet').replace('\\','/'))
                    # os.path.join(os.getcwd(),'app','data','bronze','dados_imoveis_raw.parquet').replace('\\','/'),
                    # filters = [
                    #     ("ano", "=", int((datetime.datetime.now(tz = None) - datetime.timedelta(2)).strftime("%Y"))), 
                    #     ("mes", "=", int((datetime.datetime.now(tz = None) - datetime.timedelta(2)).strftime("%m"))), 
                    #     ("dia", ">=", int((datetime.datetime.now(tz = None) - datetime.timedelta(2)).strftime("%d")))
                    # ]
                )

                # Limpando eventuais duplicados e filtrando apenas o local e tipos selecionados
                df = (
                        df
                        .loc[
                            (df.local.isin([self.local])) 
                            & (df.tipo.isin(self.tipo))
                            & (df.transacao == 'aluguel')
                        ]
                        .drop_duplicates(subset = ['local','id'], ignore_index = True)
                )

                # Transformando valores zerados em null para não afetar os cálculos média (schema do Arrow não aceita colunas com valores null)
                df.replace(0.0, None, inplace = True)
                df['bairro'] = df['bairro'].replace(to_replace= '', value = 'Sem info')

                # Retornando o dataframe
                return df
            
            elif check_local and len(check_tipos) == 0:
                df = (
                        df
                        .loc[
                            (df.local.isin([self.local])) 
                            & (df.tipo.isin(self.tipo))
                            & (df.transacao == 'aluguel')
                        ]
                        .drop_duplicates(subset = ['local','id'], ignore_index = True)
                )
                df['bairro'] = df['bairro'].replace(to_replace= '', value = 'Sem info')

                return df
        except Exception as e:
            return f'Erro na operação: {e}'

    def base_agg(self):
        '''
            ### Objetivo:
            * Retorna a base de imóveis agrupada com dados de aluguel e amenidades por bairro
            * Trata os dados conforme cálculo dos limites superiores e inferiores dos valores de aluguel
        '''
        try:
            # Obtenção da base
            df = StViews(self.local, self.tipo).check_base()
            df['bairro'] = df['bairro'].replace(to_replace= '', value = 'Sem info')

            # Remoção de outliers de aluguel
            q1 = df.loc[
                    (
                        (df.local.isin([self.local])) 
                        & (df.transacao == 'aluguel')
                        & (df.tipo.isin(self.tipo))
                    ),
                    'aluguel'
                ].quantile(0.25)
            q3 = df.loc[
                    (
                        (df.local.isin([self.local])) 
                        & (df.transacao == 'aluguel')
                        & (df.tipo.isin(self.tipo))
                    ),
                    'aluguel'
                ].quantile(0.75)

            lim_sup = q3 + (q3-q1)*1.5
            lim_inf = q1 - (q3-q1)*1.5

            # Dataframe
            df_grouped = (
                    df
                    .loc[
                        (df.local.isin([self.local]))
                        & df.transacao.isin(['aluguel'])
                        & (df.tipo.isin(self.tipo))
                        & (df.aluguel >= lim_inf)
                        & (df.aluguel <= lim_sup)
                    ]
                    .groupby(by = ['local','bairro','subtipo','tipo'])
                    .agg(
                        {
                            'aluguel':'mean', 
                            'id':'count',
                            'area':'mean',
                            'quartos':'mean',
                            'chuveiros':'mean',
                            'garagens':'mean'
                        }
                    )
                    .rename(columns={'id':'imoveis'})
                    .round(1)
                    .sort_values(by = ['imoveis', 'aluguel'], ascending = [False, False])
                    .reset_index()
            )

            # Coluna de rank de imóveis por bairro de cada local
            df_rank = (
                df_grouped
                .groupby(by = ['local', 'bairro'])
                .agg({'imoveis':'sum'})
                .assign(rank = lambda x: x.groupby('local')['imoveis'].rank(ascending = False, method = 'min'))
                .sort_values(by = 'imoveis', ascending = False)
                .reset_index()
            )

            # Dataframe final agrupado
            df_grouped = (
                pd
                .merge(df_grouped, df_rank[['bairro', 'rank']], on = 'bairro', how = 'left')
                .sort_values(by = ['rank','imoveis'], ascending = [True, False])
            )
            df_grouped['bairro_f'] = df_grouped['bairro'].apply(lambda x: re.match(r'^([^,]+)', x).group(1))

            return df_grouped
        
        except Exception as e:
            return f'Erro na operação: {e}'
    
    def st_cards(self):
        '''
            ### Objetivo:
            * Retorna cards no streamlit de totalidade dos imóveis, média de aluguel e tipo de imóvel mais comum com base no local e tipo selecionado.
        '''
        
        # Base
        df = StViews(self.local, self.tipo, self.ranking).base_agg()

        # Dados
        total_imoveis = df['imoveis'].sum().round(0)
        media_aluguel = df['aluguel'].mean().round(0)
        media_quartos = df['quartos'].mean().round(1)
        media_banheiros = df['chuveiros'].mean().round(1)
        media_garagens = df['garagens'].mean().round(1)
        dici = dict(df.groupby(by = 'tipo').agg({'imoveis':'sum','aluguel':'mean'}).round(1).reset_index().sort_values(by = ['imoveis'],ascending=False).iloc[0])
        tipo_comum = dici['tipo']
        media_tipo = int(dici['aluguel'])

        with st.container():
            st.markdown(
                f'''
                <div style="height: 80px; width: 1125px; padding: 1rem; border: 1px solid #d1d1d1; border-radius: 5px; background-color: #000000;text-align: center; justify-content: center;">
                    <h3 style = "font-size: 30px; color: white; text-align: center; justify-content: center;">Dados Gerais de Imóveis em {(self.local).upper()}</h3>
                </div>
                ''',
                unsafe_allow_html = True
            )
            st.markdown('')

        # Cards
        col1, col2, col3, col4 = st.columns([0.82,0.82,1.3,1.2])

        # Card 1
        with col1:
            st.markdown(
                f"""
                <div style="height: 100px; width: 220px; padding: 1rem; border: 1px solid #d1d1d1; border-radius: 5px; background-color: #f9f9f9;">
                    <h3 style = "font-size: 20px; color: darkblue; text-align: center;">Total de Imóveis</h3>
                    <p style = "font-size: 15px;text-align: center">{total_imoveis}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
        # Card 2
        with col2:
            st.markdown(
                f"""
                <div style="height: 100px; width: 220px; padding: 1rem; border: 1px solid #d1d1d1; border-radius: 5px; background-color: #f9f9f9;">
                    <h3 style = "font-size: 20px; color: darkblue; text-align: center;">Média de Aluguel</h3>
                    <p style = "font-size: 15px;text-align: center">R$ {media_aluguel}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Card 3
        with col3:
            st.markdown(
                f"""
                <div style="height: 100px; width: 345px; padding: 0.5rem; border: 1px solid #d1d1d1; border-radius: 5px; background-color: #f9f9f9; position: relative;">
                    <h3 style = "font-size: 20px; color: darkblue; text-align: center; display: flex; flex-direction: row;">Tipo mais comum</h3>
                    <tr style = "font-size: 5px;text-align: center;"><b>Tipo: </b>{tipo_comum}</tr>
                    <tr style = "font-size: 5px;text-align: center;"> | </tr>
                    <tr style = "font-size: 5px;text-align: center;"><b>Média de Aluguel: </b>R$ {media_tipo}</tr>
                </div>
                """,
                unsafe_allow_html=True,
            )
        # Card 4
        with col4:
            st.markdown(
                f"""
                <div style="height: 100px; width: 315px; padding: 0.5rem; border: 1px solid #d1d1d1; border-radius: 5px; background-color: #f9f9f9;">
                    <h3 style = "font-size: 20px; color: darkblue; text-align: center; display: flex; flex-direction: row;">Amenidades (Média)</h3>
                    <tr style = "font-size: 5px;text-align: center;"><b>Quartos: </b>{media_quartos}</tr>
                    <tr style = "font-size: 5px;text-align: center;"> | </tr>
                    <tr style = "font-size: 5px;text-align: center;"><b>Banheiros: </b>{media_banheiros}</tr>
                    <tr style = "font-size: 5px;text-align: center;"> | </tr>
                    <tr style = "font-size: 5px;text-align: center;"><b>Garagens: </b>{media_garagens}</tr>
                </div>
                """,
                unsafe_allow_html=True,
            )

    def bar_plot(self):
        # Base
        df = StViews(self.local, self.tipo, self.ranking).check_base()
        df_grouped = StViews(self.local, self.tipo, self.ranking).base_agg()

        # Gráfico
        # st.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Plotando os gráficos...*]")

        df_plot = px.bar(
            data_frame = df_grouped[df_grouped['rank'] <= int(self.ranking)],
            x = 'bairro_f',
            y = 'imoveis',
            color = 'tipo',
            # facet_col='local',
            # facet_col_spacing = 0.2,
            # facet_col_wrap= 1,
            title = f'<b>Total de Imóveis por Tipo</b><br>Data: {df["data"].max()}',
            labels = {'imoveis':'Imóveis','bairro_f':'Bairro'},
            # text_auto = True
            orientation='v',
            width = 1100,
            height = 500
        )

        # Definindo eixos y e x independentes
        df_plot.update_yaxes(matches = None)
        df_plot.update_xaxes(matches = None)

        df_plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

        return df_plot #.show()