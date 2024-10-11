FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    llvm \
    libffi-dev \
    liblzma-dev \
    python3-openssl \
    tk-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV HOME /root
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/bin:$PATH

RUN curl https://pyenv.run | bash && \
    bash -c "export PATH=\"$PYENV_ROOT/bin:$PATH\" && \
    export PYENV_ROOT=\"$PYENV_ROOT\" && \
    eval \"$(cat $PYENV_ROOT/completions/pyenv.bash)\" && \
    pyenv install 3.12.0 && \
    pyenv global 3.12.0"

RUN pip install --no-cache-dir poetry

WORKDIR /app
COPY ./producer /app/producer
COPY ./consumer /app/consumer

RUN (cd /app/producer && poetry install --no-dev) && \
    (cd /app/consumer && poetry install --no-dev)

WORKDIR /app/producer/app
CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]