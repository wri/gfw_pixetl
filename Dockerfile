FROM ghcr.io/osgeo/gdal:ubuntu-full-3.9.0

ENV DIR=/usr/local/app
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ARG ENV

RUN apt-get update -y \
     && apt-get install --no-install-recommends -y python3-pip python3-venv libpq-dev \
      ca-certificates postgresql-client gcc g++ python3-dev curl git pipenv \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

RUN update-ca-certificates
RUN mkdir -p /etc/pki/tls/certs
RUN cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

RUN mkdir -p ${DIR}
WORKDIR ${DIR}

COPY . .

RUN python3 -m venv .venv

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
	     echo "Install all dependencies" && \
         . .venv/bin/activate && \
	     pipenv install --deploy --ignore-pipfile --dev;  \
	else \
	     echo "Install production dependencies only" && \
         . .venv/bin/activate && \
	     pipenv install --deploy; \
	fi

RUN . .venv/bin/activate && pip install -e .

# Set current work directory to /tmp. This is important when running as an
# AWS Batch job. When using the ephemeral-storage launch template /tmp will
# be the mounting point for the external storage.
# In AWS batch we will then mount host's /tmp directory as Docker volume's /tmp
WORKDIR /tmp

ENV PYTHONPATH=/usr/local/app

ENTRYPOINT [". .venv/bin/activate && pipenv run pixetl"]