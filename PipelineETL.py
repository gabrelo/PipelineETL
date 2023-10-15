import pandas as pd
import sqlalchemy

# 1. Extração
engine = sqlalchemy.create_engine('mysql://usuario:senha@localhost/banco_de_dados')
consulta_sql = 'SELECT * FROM tabela_origem'
dados = pd.read_sql(consulta_sql, engine)

# 2. Transformação
dados['nova_coluna'] = dados['coluna_a'] + dados['coluna_b']
dados['outra_coluna'] = dados['coluna_c'] * 2

# 3. Carga
consulta_sql_destino = 'INSERT INTO tabela_destino (coluna1, coluna2) VALUES (?, ?)'
dados[['nova_coluna', 'outra_coluna']].to_sql('tabela_destino', engine, if_exists='replace', index=False)

# 4. Limpeza
# Opcional: feche a conexão com a fonte de dados
engine.dispose()

# 5. Relatórios
# Opcional: gere relatórios ou registre informações sobre o pipeline
print("Pipeline de ETL concluído com sucesso!")
