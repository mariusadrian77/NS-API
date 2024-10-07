FROM python:3.11.3-slim-bullseye

RUN pip install --upgrade pip

RUN pip install poetry==1.8.3

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME=/usr/local \
    POETRY_CACHE_DIR=/tmp/poetry_cache \
    WORKING_DIR=/code/

RUN apt-get update \
    && apt-get install -y git curl vim \
    && apt-get clean

RUN curl -sSL https://install.python-poetry.org | python -

WORKDIR $WORKING_DIR

COPY ["poetry.lock", "pyproject.toml", "$WORKING_DIR"]

COPY [".", "$WORKING_DIR"]
