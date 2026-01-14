import pandas as pd
from dash import Dash, dcc, html, Input, Output, dash
import plotly.express as px


df = pd.read_csv('../dados/clientes-v3-preparado.csv')
lista_nivel_educacao = df['nivel_educacao'].unique()
options = [{'label':nivel, 'value':nivel} for nivel in lista_nivel_educacao]

def criar_graficos(selecao_nivel_educacao):
    filto_df = df[df['nivel_educacao'].isin(selecao_nivel_educacao)]

    fig1 = px.bar(filto_df, x='estado_civil', y='salario', color='nivel_educacao', barmode='group', color_discrete_sequence=px.colors.sequential.Plasma, opacity=1)
    fig1.update_layout(
        title='Salário por estado cívil e nível de educação',
        xaxis_title='Estado civil',
        yaxis_title='Salário',
        legend_title='Nível de educação',
        plot_bgcolor='rgba(222,255,253,1)',
        paper_bgcolor='rgba(186,245,241,1)',
    )

    fig2 = px.scatter_3d(filto_df, x='idade', y='salario', z='nivel_educacao',color='nivel_educacao')
    fig2.update_layout(
        title='Salário vs Idade e anos de experiência',
        scene=dict(
            xaxis_title='Idade',
            yaxis_title='Salario',
            zaxis_title='Anos de Experiência',
        )
    )

    return fig1, fig2

def criar_app():
    app = dash.Dash(__name__)

    app.layout = html.Div([
        html.Div(["Dashboard Interativo"]),
        html.Div(''' Interatividade entre os dados '''),
        html.Br(),
        html.H2('Gráfico de salário por estado cívil'),
        dcc.Checklist(
            id='selecao_nivel_educacao',
            options=options,
            value=[lista_nivel_educacao[0]],
        ),
        dcc.Graph(id='id_grafico_barra'),
        dcc.Graph(id='id_grafico_3d'),
    ])

    return app

if __name__ == '__main__':
    app = criar_app()

    @app.callback(
        [
            Output('id_grafico_barra', 'figure'),
            Output('id_grafico_3d', 'figure'),
        ], [Input('selecao_nivel_educacao', 'value')],
    )
    def atualizar_grafico(selecao_nivel_educacao):
        fig1, fig2 = criar_graficos(selecao_nivel_educacao)
        return [fig1, fig2]
    app.run(debug=True,port=8050)