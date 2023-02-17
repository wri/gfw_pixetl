FROM osgeo/gdal:ubuntu-small-3.6.1
LABEL desc="Docker image with Pixetl and dependencies"
LABEL version="v1.7.4"


ENV DIR=/usr/local/app
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ARG ENV

RUN apt-get update -y \
     && apt-get install --no-install-recommends -y python3-pip libpq-dev \
      ca-certificates postgresql-client-14 gcc python3-dev curl git \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

RUN update-ca-certificates
RUN mkdir -p /etc/pki/tls/certs
RUN cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

RUN mkdir -p ${DIR}
WORKDIR ${DIR}

COPY . .

RUN pip3 install pipenv==v2022.11.30

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
	     echo "Install all dependencies" && \
	     pipenv install --system --deploy --ignore-pipfile --dev;  \
	else \
	     echo "Install production dependencies only" && \
	     pipenv install --system --deploy; \
	fi

RUN pip3 install -e .

# Set current work directory to /tmp. This is important when running as an
# AWS Batch job. When using the ephemeral-storage launch template /tmp will
# be the mounting point for the external storage.
# In AWS batch we will then mount host's /tmp directory as Docker volume's /tmp
WORKDIR /tmp

ENV PYTHONPATH=/usr/local/app

ENTRYPOINT ["pixetl"]