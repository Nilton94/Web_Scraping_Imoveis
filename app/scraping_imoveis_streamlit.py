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
from utils.utils_views import StViews

# ------------------------------------------ Dados dos municípios ----------------------------------------------#

df_mun = pd.read_parquet(
    os.path.join(os.getcwd(),'data','bronze','municipios_raw.parquet')
)
df_mun.sort_values(by = ['str_uf', 'str_local'], ascending = [True, True], inplace = True)
locais = list(map(lambda x: x ,df_mun['str_local']))

# ---------------------------------------------- Page Config ----------------------------------------------------#

st.set_page_config(
    page_title = 'Dados de Aluguel - Zapimoveis',
    page_icon = 'house',
    layout = "wide"
)

st.header('_:blue[DADOS DE ALUGUEL POR LOCAL E TIPO DE IMÓVEL - ZAPIMÓVEIS]_')
st.markdown("""----""")

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

# ---------------------------------------------- Gerando os dados --------------------------------------------------------#
    
if st.sidebar.button("Gerar Gráficos!", key = 'gerador'):
    try:
        st.sidebar.write('Logs')
        
        st.sidebar.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Obtendo a base...*]")
        # Obtenção da base
        df = StViews(local = st.session_state.local, tipo = st.session_state.tipo, ranking = st.session_state.ranking).check_base()
        df_grouped = StViews(local = st.session_state.local, tipo = st.session_state.tipo, ranking = st.session_state.ranking).base_agg()

        st.sidebar.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Gerando cards...*]")
        
        # Cards
        StViews(local = st.session_state.local, tipo = st.session_state.tipo, ranking = st.session_state.ranking).st_cards()

        st.sidebar.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Gerando plots...*]")
        # Gráficos
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
            width = 1100,
            height = 500
        )

        # Definindo eixos y e x independentes
        df_plot.update_yaxes(matches = None)
        df_plot.update_xaxes(matches = None)

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
            yaxis2 = dict(title = 'Média do Aluguel', overlaying = 'y', side = 'right'),
            width = 1100,
            height = 500
        )

        combo.update_layout(legend = dict(x = 1, y=1.3))

        st.plotly_chart(df_plot)
        st.plotly_chart(combo)

    except Exception as e:
        st.error(f'Erro na execução: {e}')