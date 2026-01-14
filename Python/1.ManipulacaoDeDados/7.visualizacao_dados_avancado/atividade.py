import pandas as pd
import plotly.express as px
from dash import Dash, html, dash, dcc

df = pd.read_csv('../dados/ecommerce_estatistica.csv')
print(df.head().to_string())

def grafico_histograma(df):
    fig = px.histogram(df, x='Nota', nbins=60, title='Distribuição de Notas dos Produtos')
    return fig

def grafico_dispersao(df):
    fig = px.scatter(df,x='Desconto', y='Qtd_Vendidos', color='Marca', hover_data=['Gênero'])
    fig.update_layout(
        title='Relação: Valor do Desconto vs. Quantidade Vendida',
        xaxis_title='Desconto',
        yaxis_title='Quantidade vendida',
    )
    return fig

def grafico_de_calor(df):
    colunas =  ['Nota', 'N_Avaliações', 'Desconto', 'Preço']

    df_corr = df[colunas].corr()

    fig = px.imshow(df_corr, text_auto=True, color_continuous_scale='RdBu_r')
    return fig

def grafico_barras(df):

    contagem = df['Marca'].value_counts()
    marcas_com_mais_de_1 = contagem[contagem > 1].index

    df_reduzida = df[df['Marca'].isin(marcas_com_mais_de_1)]

    fig = px.bar(df_reduzida, y='Marca')
    return fig

def grafico_pizza(df):
    fig = px.pie(df, names='Gênero')

    return fig

def grafico_densidade(df):
    fig = px.histogram(df, x="Nota", marginal="rug",hover_data=df.columns,nbins=30,histnorm='probability density')

    return fig

def grafico_regressao(df):
    df_clean = df.dropna(subset=['N_Avaliações', 'Qtd_Vendidos_Cod'])

    fig = px.scatter(
        df_clean,
        x='N_Avaliações',
        y='Qtd_Vendidos_Cod',
        trendline='ols',
        trendline_color_override='darkblue',
        template='plotly_white'
    )


    fig.update_layout(
        title='Relação: Número de Avaliações vs. Quantidade Vendida',
        xaxis_title='Número de Avaliações',
        yaxis_title='Quantidade Vendida (Cód)',
    )

    return fig

def criar_app():

    df = pd.read_csv('../dados/ecommerce_estatistica.csv')

    app = dash.Dash(__name__)

    app.layout = html.Div([
        dcc.Graph(figure=grafico_histograma(df)),
        dcc.Graph(figure=grafico_dispersao(df)),
        dcc.Graph(figure=grafico_de_calor(df)),
        dcc.Graph(figure=grafico_barras(df)),
        dcc.Graph(figure=grafico_pizza(df)),
        dcc.Graph(figure=grafico_densidade(df)),
        dcc.Graph(figure=grafico_regressao(df))
    ])

    return app

if __name__ == '__main__':
    app = criar_app()

    app.run(debug=True, port=8050)




