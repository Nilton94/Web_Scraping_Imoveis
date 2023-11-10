import pandas as pd
import streamlit as st
import datetime
import re
import plotly.express as px

class StViews():
    def bar_plot(self, df, local: str, tipo: list, ranking: int):
        # Limites
        q1 = df.loc[
                (
                    (df.local.isin([local])) 
                    & (df.transacao == 'aluguel')
                    & (df.tipo.isin(tipo))
                ),
                'aluguel'
            ].quantile(0.25)
        q3 = df.loc[
                (
                    (df.local.isin([local])) 
                    & (df.transacao == 'aluguel')
                    & (df.tipo.isin(tipo))
                ),
                'aluguel'
            ].quantile(0.75)

        lim_sup = q3 + (q3-q1)*1.5
        lim_inf = q1 - (q3-q1)*1.5

        # Dataframe
        df_grouped = (
                df
                .loc[
                    (df.local.isin([local]))
                    & df.transacao.isin(['aluguel'])
                    & (df.tipo.isin(tipo))
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
        # st.write(f"{datetime.datetime.now(tz = None).replace(microsecond=0)} - :red[*Plotando os gráficos...*]")

        df_plot = px.bar(
            data_frame = df_grouped[df_grouped['rank'] <= int(ranking)],
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
            width = 30*int(ranking),
            height = 400
        )

        # Definindo eixos y e x independentes
        df_plot.update_yaxes(matches=None)
        df_plot.update_xaxes(matches=None)

        df_plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

        return df_plot.show()