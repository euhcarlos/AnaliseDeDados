import plotly.express as px
import pandas as pd
from dash import Dash,html, dcc

def criar_grafico_histograma(df):
    # Histograma
    fig1 = px.histogram(df, x='salario', nbins=50, title='Distribuição salário')
    return fig1

def criar_grafico_pizza(df):
    # Pizza
    fig2 = px.pie(df, names='area_atuacao', color='area_atuacao', hole=0.2, color_discrete_sequence=px.colors.sequential.Plotly3)
    return fig2


def criar_grafico_bolha(df):
    # Bolha
    fig3 = px.scatter(df, x='idade', y='salario', size='anos_experiencia', color='area_atuacao')
    fig3.update_layout(title='Salário por idade e anos de experiência')
    return fig3

def criar_grafico_linha(df):
    # Linha
    fig4 = px.line(df, x='idade', y='salario', color='area_atuacao', facet_col='nivel_educacao')
    fig4.update_layout(
        title='Salário por idade e área de atuação para cada nivel de educação',
        xaxis_title='idade',
        yaxis_title='salario'
    )
    return fig4

def criar_grafico_3d(df):
    # 3D
    fig5 = px.scatter_3d(df, x='idade', y='salario', z='anos_experiencia', color='nivel_educacao')
    return fig5

def criar_grafico_barra(df):
    # Barra
    fig6 = px.bar(df, x='estado_civil', y='salario', color='nivel_educacao', barmode='group', color_discrete_sequence=px.colors.sequential.Plotly3, opacity=1)
    fig6.update_layout(
        title='Salário por estado civil e nível de educação',
        xaxis_title='idade',
        yaxis_title='salario',
        legend_title='Nível de educação',
        plot_bgcolor='rgba(222,255,253,1)', # Fundo interno
        paper_bgcolor='rgba(186,245,241,1)', # Fundo externo
    )
    return fig6

def criar_app():
    # Criar app

    df = pd.read_csv('../dados/clientes-v3-preparado.csv')

    app = Dash(__name__)

    app.layout = html.Div([
        dcc.Graph(figure=criar_grafico_histograma(df)),
        dcc.Graph(figure=criar_grafico_pizza(df)),
        dcc.Graph(figure=criar_grafico_bolha(df)),
        dcc.Graph(figure=criar_grafico_linha(df)),
        dcc.Graph(figure=criar_grafico_3d(df)),
        dcc.Graph(figure=criar_grafico_barra(df))
    ])

    return app

if __name__ == '__main__':
    app = criar_app()
    app.run(debug=True, port=8050)
