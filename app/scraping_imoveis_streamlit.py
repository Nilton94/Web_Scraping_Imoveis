import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import plotly.express as px
from utils.utils_scraper import ScraperZap
# from utils.utils_psql import UtilsPSQL
import datetime
import plotly.graph_objects as go
from utils.utils_views import StViews
import io

# ------------------------------------------ Dados dos municípios ----------------------------------------------#

df_mun = pd.read_parquet(
    # os.path.join(os.getcwd(),'data','bronze','municipios_raw.parquet')
    # os.path.join(os.getcwd(),'data','bronze','municipios_raw.parquet').replace('\\','/')
    os.path.join(os.getcwd(),'app','data','bronze','municipios_raw.parquet').replace('\\','/')
)
df_mun.sort_values(by = ['str_uf', 'str_local'], ascending = [True, True], inplace = True)
locais = list(map(lambda x: x ,df_mun['str_local']))

# ---------------------------------------------- Page Config ----------------------------------------------------#

st.set_page_config(
    page_title = 'Dados de Aluguel - Zapimoveis',
    page_icon = 'house',
    layout = 'wide',
    initial_sidebar_state = 'expanded',
    menu_items = {
        # "Get help": "mailto:niltontestespython@gmail.com",
        "About": "App de busca e consolidação de dados de imóveis, com fins de estudo, usando os imóveis disponíveis no site da Zapimóveis. Criado por José Nilton Gonçalves de Andrade (https://www.linkedin.com/in/niltonandrade/)."
    }
)

st.header('_:blue[DADOS DE ALUGUEL POR LOCAL E TIPO DE IMÓVEL - ZAPIMÓVEIS]_')
st.markdown("""----""")

# ---------------------------------------------- Widgets --------------------------------------------------------#

local = st.sidebar.selectbox(
    "Local",
    options = locais,
    help = 'Selecione o local desejado para ver os imóveis disponíveis. Formato UF+cidade',
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
    help ='''
            ## Os imóveis na Zapimoveis podem ser divididos em dois grandes grupos:
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
    ''',
    key = 'tipo'

)

ranking = st.sidebar.number_input(
    'Top Bairros',
    min_value = 1,
    max_value = 50,
    help = 'Escolha quantos bairros serão mostrados com base na ordem decrescente do total de imóveis.',
    key = 'ranking'
)

# ---------------------------------------------- Gerando os dados --------------------------------------------------------#

if st.sidebar.button("Gerar Gráficos!", key = 'gerador'):
    # Checando se o local possui algum imóvel disponível nos tipos especificados
    imoveis_selecao = [z['Imoveis'] for z in [ScraperZap(tipo = tipo, local = local).paginas() for tipo in st.session_state.tipo for local in [st.session_state.local]] if z['Imoveis'] != None]
    total_imoveis_selecao = sum(imoveis_selecao)

    if total_imoveis_selecao != None and float(total_imoveis_selecao) > 0:
        try:
            # ----------------------------Sidebar ---------------------- #
            st.sidebar.markdown('---')
            st.sidebar.write('### Logs')
            st.sidebar.write(
                f'''
                {datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*A cidade de {st.session_state.local} possui, nos tipos selecionados, {int(total_imoveis_selecao)} imóveis de aluguel disponíveis na base do Zapimóveis. 
                Lembrando que o site do Zapimoveis só mostra 100 páginas por tipo, o que dá em torno de no máximo 10 mil imóveis por tipo!*]
            ''')
            st.sidebar.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Obtendo a base, removendo outliers e duplicados...*]")
            
            # Hora inicio
            inicio = datetime.datetime.now(tz = None).replace(microsecond=0)

            # ----------------------Obtenção da base ------------------- #
            df = StViews(local = st.session_state.local, tipo = st.session_state.tipo, ranking = st.session_state.ranking).check_base()
            df_grouped = StViews(local = st.session_state.local, tipo = st.session_state.tipo, ranking = st.session_state.ranking).base_agg()

            st.sidebar.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Gerando cards...*]")
            
            # --------------------------- Cards ------------------------ #
            StViews(local = st.session_state.local, tipo = st.session_state.tipo, ranking = st.session_state.ranking).st_cards()

            st.sidebar.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Gerando plots...*]")
            
            # ------------------------ Gráfico Sunburst ---------------- # 
            df_sun = df_grouped[df_grouped['rank'] <= int(st.session_state.ranking)]
            sun_plot = px.sunburst(
                data_frame = df_sun,
                path = ['bairro_f', 'subtipo', 'tipo'],
                values = 'imoveis',
                color = 'aluguel',
                color_continuous_scale = 'Portland',
                color_continuous_midpoint = np.average(df_sun['aluguel'], weights = df_sun['imoveis']),
                hover_data  = ['aluguel', 'area', 'quartos', 'chuveiros', 'garagens'],
                title = f'<b>Distribuição de Imóveis e Média de Amenidades</b><br>Data: {df["data"].max()}',
                width = 1100,
                height = 700
            )

            # ------------------------ Gráfico de Tabela Agrupada --------------- #
            df_tab = go.Figure(
                data = [
                    go.Table(
                        header = dict(
                            values = list(
                                df_sun[['bairro_f','subtipo','tipo','aluguel','imoveis','area','quartos','chuveiros','garagens']].rename(
                                    columns = {
                                        'bairro_f':'Bairro',
                                        'subtipo':'Categoria',
                                        'tipo':'Tipo',
                                        'aluguel':'Aluguel',
                                        'imoveis':'Imóveis',
                                        'area':'Área',
                                        'quartos':'Quartos',
                                        'chuveiros':'Chuveiros',
                                        'garagens':'Garagens'
                                    }
                                ).columns
                            ),
                            fill_color = 'paleturquoise',
                            align = 'center'
                        ),
                        cells = dict(
                            values = [df_sun.bairro_f, df_sun.subtipo, df_sun.tipo, df_sun.aluguel, df_sun.imoveis, df_sun.area, df_sun.quartos, df_sun.chuveiros, df_sun.garagens],
                            fill_color='lavender',
                            align='center'
                        )
                    )
                ]
            )
            df_tab.update_layout(width = 1100, height = 500, title_text = f'<b>Dados Tabelados Agrupados por Bairro</b><br>Data: {df["data"].max()}')

            # ------------------------ Gráfico de Tabela por Imóvel --------------- #
            df['bairro'] = df['bairro'].apply(lambda x: re.match(r'^([^,]+)', x).group(1))
            df['link'] = '<a href="'+ df["url"] + '">' + df["id"] + '</a>'
            df_tab_imo = (
                df[['bairro','endereco','subtipo','tipo','aluguel','area','quartos','chuveiros','garagens','link']]
                .rename(
                    columns = {
                        'bairro':'Bairro',
                        'endereco':'Endereço',
                        'subtipo':'Categoria',
                        'tipo':'Tipo',
                        'aluguel':'Aluguel',
                        'area':'Área',
                        'quartos':'Quartos',
                        'chuveiros':'Banheiros',
                        'garagens':'Garagens',
                        'link':'URL'
                    }
                )
                .sort_values(by = ['Bairro','Aluguel'], ascending = [True, False])
                .reset_index(drop=True)
            )
            
            df_tab_imo_plot = go.Figure(
                data = [
                    go.Table(
                        header = dict(
                            values = list(
                                df_tab_imo[['Bairro','Endereço','Categoria','Tipo','Aluguel','Área','Quartos','Banheiros','Garagens','URL']].columns
                            ),
                            fill_color = 'paleturquoise',
                            align = 'center'
                        ),
                        cells = dict(
                            values = [df_tab_imo.Bairro, df_tab_imo['Endereço'], df_tab_imo.Categoria, df_tab_imo.Tipo, df_tab_imo.Aluguel,df_tab_imo['Área'], df_tab_imo.Quartos, df_tab_imo.Banheiros, df_tab_imo.Garagens, df_tab_imo.URL],
                            fill_color='lavender',
                            align='center'
                        )
                    )
                ]
            )
            df_tab_imo_plot.update_layout(width = 1100, height = 500, title_text = f'<b>Dados Tabelados por Imóvel</b><br>Data: {df["data"].max()}')

            # ------------------------ Gráfico de Coluna --------------- # 
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

            # ------------------------ Gráfico de combo --------------- # 
            df_1 = df_grouped[df_grouped['rank'] <= int(st.session_state.ranking)].groupby('bairro_f').agg({'imoveis':'sum', 'aluguel':'mean'}).round(1).sort_values('imoveis', ascending = False).reset_index()
            combo = go.Figure()
            combo.add_trace(
                go.Bar(
                    x = df_1['bairro_f'], 
                    y = df_1['imoveis'], 
                    name = 'Imóveis'
                )
            )

            # Add a line trace from the DataFrame
            combo.add_trace(
                go.Scatter(
                    x = df_1['bairro_f'], 
                    y = df_1['aluguel'], 
                    mode = 'lines', 
                    name = 'Aluguel', 
                    yaxis = 'y2'
                )
            )

            # Update layout
            combo.update_layout(
                title = f'<b>Média do Aluguel por Bairro </b><br>Data: {df["data"].max()}',
                xaxis = dict(title = 'Bairro'),
                yaxis = dict(title = 'Imóveis'),
                yaxis2 = dict(title = 'Média do Aluguel', overlaying = 'y', side = 'right'), 
                width = 1100,
                height = 500
            )

            combo.update_layout(legend = dict(x = 1, y = 1.3))

            # ---------------------------- Plots ---------------------------- #
            st.plotly_chart(df_plot)
            st.plotly_chart(combo)
            st.plotly_chart(sun_plot)
            st.plotly_chart(df_tab)
            st.plotly_chart(df_tab_imo_plot)

            # Tempo de duração
            fim = datetime.datetime.now(tz = None).replace(microsecond=0)
            tempo = str(fim - inicio)
            
            # ------------------------ Download de Dados -------------------- # 
            
            st.markdown("""----""")

            col1, col2 = st.columns(2)
            col1.download_button(
                label='Download da Base Completa em CSV!',
                data = df.to_csv(index=False),
                use_container_width = True
            )

            excel_file = io.BytesIO()
            df.to_excel(excel_file, index = False, engine='xlsxwriter')
            excel_file.seek(0)
            col2.download_button(
                label='Download da Base Completa em XLSX!',
                data = excel_file,
                file_name = f'DadosZapimoveis-{inicio}.xlsx',
                use_container_width = True
            )


            st.sidebar.write(f'<b>Tempo de duração</b>: {tempo}', unsafe_allow_html=True)

        except Exception as e:
            # st.error(f'Erro na execução: {e}')
            st.write(e)
    else:
        st.write(f'#### :red[A cidade de {st.session_state.local}, nos tipos selecionados, não possui nenhum imóvel disponível para aluguel no site da Zapimoveis. Por favor, selecione outros tipos ou local!]')