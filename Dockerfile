ARG PYTHON_VERS=3.10.7-slim

### BUILD REQUIREMENTS.txt ###
FROM python:$PYTHON_VERS as requirements

RUN python -m pip install --no-cache-dir --upgrade poetry
COPY pyproject.toml poetry.lock ./
RUN poetry export --without dev -f requirements.txt --without-hashes -o requirements.txt

### INSTALL REQUIREMENTS ###
FROM python:$PYTHON_VERS AS installation

COPY --from=requirements requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc

RUN python -m venv /opt/venv
# to use the virtual env
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

### BUILD APP ###
FROM python:$PYTHON_VERS AS appback

COPY --from=installation /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN mkdir data

COPY data data

COPY app.py app.py
# no need to run data processing
COPY /gtfs_builder gtfs_builder/

# Switching to non-root user appuser
RUN useradd ava
USER ava

CMD ["/bin/bash", "-c"]

ENTRYPOINT gunicorn -b 0.0.0.0:5002 app:app
