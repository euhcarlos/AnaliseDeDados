import plotly.express as px
import pandas as pd

df = pd.read_csv('../dados/clientes-v3-preparado.csv')
print(df.head().to_string())

# Gráfico de dispersão
fig = px.scatter(df, x='idade', y='salario', color='nivel_educacao', hover_data=['estado_civil'])
fig.update_layout(
    title='Idade vs Salário por nível de educação',
    xaxis_title='Idade',
    yaxis_title='Salário'
)
fig.show()