import streamlit as st
import sqlite3
import pandas as pd
import plotly as pt
import os
import re
import plotly.express as px
from utils.utils_scraper import ScraperZap
import datetime
import pytz

# Dados dos municípios
df_mun = pd.read_parquet(
    os.path.join(os.getcwd(),'data','bronze','municipios_raw.parquet')
)
df_mun.sort_values(by = ['str_uf', 'str_local'], ascending = [True, True], inplace = True)
locais = list(map(lambda x: x ,df_mun['str_local']))

# Widgets
local = st.sidebar.multiselect(
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
        'quitinetes'
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

# if st.sidebar.button("Gerar Gráficos!"):
#     if st.session_state.tipo and st.session_state.local:  # Check if both selections are not empty
#         # Process data and generate plot
#         st.write("Gerando gráficos...")
#     else:
#         st.warning("Selecione ao menos uma opção de cada campo!")

# Obtenção da base
# ScraperZap().scraping_multiple(
#     _transacao = ['aluguel'], 
#     _tipo = st.session_state.tipo, 
#     _local = st.session_state.local
# )

# Data atual
dia = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%d")
mes = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%m")
ano = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).strftime("%Y")

# Base conforme parâmetros passados
df = pd.read_parquet(
    os.path.join(os.getcwd(),'data','bronze','dados_imoveis_raw.parquet'), 
    filters = [("ano", "=", int(ano)), ("mes", "=", int(mes)), ("dia", "=", int(dia)-2)]
)

# Tratamentos
# Remoção de outliers de aluguel
q1 = df.loc[
        (
            (df.local.isin(st.session_state.local)) & (df.transacao == 'aluguel')
        ),
        'aluguel'
    ].quantile(0.25)
q3 = df.loc[
        (
            (df.local.isin(st.session_state.local)) & (df.transacao == 'aluguel')
        ),
        'aluguel'
    ].quantile(0.75)

lim_sup = q3 + (q3-q1)*1.5
lim_inf = q1 - (q3-q1)*1.5

# Dataframe
df_grouped = (
        df
        .loc[
            (df.local.isin(st.session_state.local))
            & df.transacao.isin(['aluguel'])
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

df_grouped = (
    pd
    .merge(df_grouped, df_rank[['bairro', 'rank']], on = 'bairro', how = 'left')
    .sort_values(by = ['rank','imoveis'], ascending = [False, False])
)

df_grouped['bairro_f'] = df_grouped['bairro'].apply(lambda x: re.match(r'^([^,]+)', x).group(1))

# Gráfico
st.title(f'Análise do aluguel')

df_plot = px.bar(
    data_frame = df_grouped[df_grouped['rank'] <= 20],
    y = 'bairro_f',
    x = 'imoveis',
    color = 'tipo',
    facet_col='local',
    facet_col_spacing = 0.2,
    # facet_col_wrap= 1,
    title = f'<b>Total de Imóveis por Tipo</b><br>Data: {df["data"].max()}',
    labels = {'imoveis':'Imóveis','bairro_f':'Bairro'},
    # text_auto = True
    orientation='h',
    width = 1200,
    height = 400
)

# Definindo eixos y e x independentes
df_plot.update_yaxes(matches=None)
df_plot.update_xaxes(matches=None)

df_plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

st.write(df_plot)