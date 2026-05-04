FROM ubuntu:24.04

# Evita prompt bloccanti durante l'installazione dei pacchetti
ENV DEBIAN_FRONTEND=noninteractive

# Installa tutti i tool di compilazione necessari per il tuo Makefile
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    autoconf \
    automake \
    libtool \
    pkg-config \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Crea la working directory
WORKDIR /app

# Copia tutto il codice sorgente nel container
COPY . .

# Rendi lo script di avvio eseguibile
RUN chmod +x docker-entrypoint.sh

# Esegue i comandi del tuo Makefile
# 1. Scarica e compila lsmt, libuv e raft
RUN make deps
# 2. Compila il tuo server.c
RUN make release

# Crea la cartella in cui verranno salvate le SSTable
RUN mkdir -p /data

# Avvia lo script che genererà la configurazione
ENTRYPOINT ["./docker-entrypoint.sh"]
