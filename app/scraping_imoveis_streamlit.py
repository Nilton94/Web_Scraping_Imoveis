import streamlit as st
import sqlite3
import pandas as pd
import plotly as pt
import os
import re
import plotly.express as px
from utils.utils_scraper import ScraperZap
from utils.utils_psql import UtilsPSQL
import datetime
import pytz
import plotly.graph_objects as go

# ------------------------------------------ Dados dos municípios ----------------------------------------------#

df_mun = pd.read_parquet(
    os.path.join(os.getcwd(),'data','bronze','municipios_raw.parquet')
)
df_mun.sort_values(by = ['str_uf', 'str_local'], ascending = [True, True], inplace = True)
locais = list(map(lambda x: x ,df_mun['str_local']))
# locais.insert(0,'-- Selecione o Local --')

st.markdown('#### DADOS DE ALUGUEL POR LOCAL E TIPO DE IMÓVEL - ZAPIMÓVEIS')

# ---------------------------------------------- Widgets --------------------------------------------------------#

local = st.sidebar.selectbox(
    "Local",
    options = locais,
    help = 'Selecione o local desejado para ver os imóveis disponíveis',
    key = 'local'
)

tipo = st.sidebar.multiselect(
    'Tipo de Imóvel',
    options = [
        'apartamentos',
        'studio',
        'quitinetes',
        'casas',
        'sobrados',
        'casas-de-condominio',
        'casas-de-vila',
        'cobertura',
        'flat',
        'loft',
        'terrenos-lotes-condominios',
        'fazendas-sitios-chacaras',
        'loja-salao',
        'conjunto-comercial-sala',
        'casa-comercial',
        'hoteis-moteis-pousadas',
        'andares-lajes-corporativas',
        'predio-inteiro',
        'terrenos-lotes-comerciais',
        'galpao-deposito-armazem',
        'box-garagem'
    ],
    key = 'tipo'

)

ranking = st.sidebar.number_input(
    'Top Bairros',
    min_value = 1,
    max_value = 50,
    key = 'ranking'
)

# ------------------------------------------- Obtenção da base -------------------------------------------------------#
# @st.cache
def base():
    
    try:
        with st.empty():
            st.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Fazendo scraping dos dados...*]")
            
            ScraperZap().scraping_multiple(
                _transacao = ['aluguel'], 
                _tipo = st.session_state.tipo, 
                _local = [st.session_state.local]
            )

            # Base conforme parâmetros passados
            df = pd.read_parquet(
                os.path.join(os.getcwd(),'data','bronze','dados_imoveis_raw.parquet'), 
                filters = [
                    ("ano", "=", int(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%Y"))), 
                    ("mes", "=", int(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%m"))), 
                    ("dia", "=", int(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%d")))
                ]
            )

            st.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Fazendo tratamento dos dados...*]")
            # Tratamentos
            # Remoção de outliers de aluguel
            q1 = df.loc[
                    (
                        (df.local.isin([st.session_state.local])) 
                        & (df.transacao == 'aluguel')
                        & (df.tipo.isin(st.session_state.tipo))
                    ),
                    'aluguel'
                ].quantile(0.25)
            q3 = df.loc[
                    (
                        (df.local.isin([st.session_state.local])) 
                        & (df.transacao == 'aluguel')
                        & (df.tipo.isin(st.session_state.tipo))
                    ),
                    'aluguel'
                ].quantile(0.75)

            lim_sup = q3 + (q3-q1)*1.5
            lim_inf = q1 - (q3-q1)*1.5

            # Dataframe
            df_grouped = (
                    df
                    .loc[
                        (df.local.isin([st.session_state.local]))
                        & df.transacao.isin(['aluguel'])
                        & (df.tipo.isin(st.session_state.tipo))
                        & (df.aluguel >= lim_inf)
                        & (df.aluguel <= lim_sup)
                    ]
                    .groupby(by = ['local','bairro','tipo'])
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

            # Gráfico
            st.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Plotando os gráficos...*]")

            df_plot = px.bar(
                data_frame = df_grouped[df_grouped['rank'] <= int(st.session_state.ranking)],
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
                width = 1600,
                height = 500
            )

            # Definindo eixos y e x independentes
            df_plot.update_yaxes(matches=None)
            df_plot.update_xaxes(matches=None)

            df_plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

            # Combo chart
            df = df_grouped[df_grouped['rank'] <= int(st.session_state.ranking)].groupby('bairro_f').agg({'imoveis':'sum', 'aluguel':'mean'}).round(1).sort_values('imoveis', ascending = False).reset_index()
            combo = go.Figure()
            combo.add_trace(
                go.Bar(
                    x = df['bairro_f'], 
                    y = df['imoveis'], 
                    name = 'Imóveis'
                )
            )

            # Add a line trace from the DataFrame
            combo.add_trace(
                go.Scatter(
                    x = df['bairro_f'], 
                    y = df['aluguel'], 
                    mode = 'lines', 
                    name = 'Aluguel', 
                    yaxis = 'y2'
                )
            )

            # Update layout
            combo.update_layout(
                title = 'Total de Imóveis e Média de Aluguel por Bairro',
                xaxis = dict(title = 'Bairro'),
                yaxis = dict(title = 'Imóveis'),
                yaxis2 = dict(title = 'Média do Aluguel', overlaying = 'y', side = 'right')
            )

            combo.update_layout(legend = dict(x = 1, y=1.3))

            st.write('')

        st.plotly_chart(df_plot)
        st.plotly_chart(combo)

    except:
        return st.write(f'Problemas ao carregar os dados para a cidade de {st.session_state.local}')
    
if st.sidebar.button("Gerar Gráficos!", key = 'gerador'):
    try:
        base()
    except Exception as e:
        st.error(f'Erro na execução: {e}')