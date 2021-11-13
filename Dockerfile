FROM continuumio/miniconda3:4.7.12 AS build

WORKDIR /usr/src/

COPY environment.yml environment.yml
RUN conda install -c conda-forge mamba conda-pack

RUN mamba env create -f environment.yml
RUN conda clean --all --yes

# Use conda-pack to create a standalone enviornment
# in /venv:
RUN conda-pack -n gtfs_builder -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

# We've put venv in same path it'll be in final image,
# so now fix up paths:
RUN /venv/bin/conda-unpack



FROM debian:stable-slim AS runtime

COPY data/ter_moving_stops.parq ter_moving_stops.parq
COPY data/toulouse_moving_stops.parq toulouse_moving_stops.parq

COPY app.py app.py
# no need to run data processing
# COPY db_run.py db_run.py
COPY /gtfs_builder gtfs_builder/

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# no root user
RUN useradd --no-create-home ava
# RUN chown -R ava:ava /venv
USER ava


# When image is run, run the code with the environment
# activated:
SHELL ["/bin/bash", "-c"]

ENTRYPOINT source /venv/bin/activate && \
           gunicorn --workers=2 -b 0.0.0.0:5002 app:app