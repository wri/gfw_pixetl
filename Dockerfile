FROM ghcr.io/osgeo/gdal:ubuntu-full-3.8.5

ENV DIR=/usr/local/app
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV VENV_DIR="/.venv"

ARG ENV

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y python3-dev python3-venv \
        ca-certificates postgresql-client gcc g++ curl git libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN update-ca-certificates
RUN mkdir -p /etc/pki/tls/certs
RUN cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

# --system-site-packages is needed to copy the GDAL Python libs into the venv
RUN python -m venv ${VENV_DIR} --system-site-packages \
    && . ${VENV_DIR}/bin/activate \
    && python -m ensurepip --upgrade \
    && pip install pipenv

RUN mkdir -p ${DIR}
WORKDIR ${DIR}

COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
        echo "Install all dependencies" && \
        . ${VENV_DIR}/bin/activate && \
	    pipenv install --deploy --ignore-pipfile --dev;  \
	else \
	    echo "Install production dependencies only" && \
        . ${VENV_DIR}/bin/activate && \
	    pipenv install --deploy; \
	fi

COPY . .

RUN . ${VENV_DIR}/bin/activate \
    && pip install -e .

# Set current work directory to /tmp. This is important when running as an
# AWS Batch job. When using the ephemeral-storage launch template /tmp will
# be the mounting point for the external storage.
# In AWS batch we will then mount host's /tmp directory as Docker volume's /tmp
WORKDIR /tmp

ENV PYTHONPATH=/usr/local/app

ENTRYPOINT [". ${VENV_DIR}/bin/activate && pipenv run pixetl"]