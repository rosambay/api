# Usa uma imagem oficial, leve e atualizada do Python
FROM python:3.11-slim

# Impede o Python de gravar ficheiros .pyc e força o log a aparecer no terminal imediatamente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Define o diretório de trabalho
WORKDIR /app

# Instala as dependências do sistema necessárias para compilar bibliotecas (como bcrypt e asyncpg)
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia primeiro o ficheiro de requisitos (Aproveita o cache do Docker se as libs não mudarem)
COPY requirements.txt .

# Instala as bibliotecas do Python sem guardar cache para poupar espaço
RUN pip install --no-cache-dir -r requirements.txt

# Cria usuário não-root e transfere ownership do diretório de trabalho
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
RUN chown -R appuser:appgroup /app

# Copia todo o código do projeto para dentro do container
COPY . .

# Troca para o usuário não-root
USER appuser

# Expõe a porta que a API vai ouvir
EXPOSE 8000

# Comando padrão — pode ser sobrescrito pelo docker-compose
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
