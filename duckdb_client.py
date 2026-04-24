import duckdb

# 1. Cria a conexão principal (pode ser em memória ou arquivo físico)
# Geralmente isso é inicializado no 'lifespan' do FastAPI
duck_conn = duckdb.connect(':memory:')
duck_conn.execute("SET TimeZone='America/Sao_Paulo';")

# ou duckdb.connect(":memory:")

def get_duckdb():
    """
    Gera um cursor isolado para a requisição atual.
    Isso evita conflito se múltiplas requisições tentarem usar a mesma conexão simultaneamente.
    """
    
    cursor = duck_conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()