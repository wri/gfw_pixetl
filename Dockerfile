FROM osgeo/gdal:ubuntu-small-3.2.0

ENV DIR=/usr/local/app
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ARG ENV

RUN apt update -y && apt install -y python3-pip libpq-dev ca-certificates \
    postgresql-client-12
RUN update-ca-certificates
RUN mkdir -p /etc/pki/tls/certs
RUN cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

RUN mkdir -p ${DIR}
WORKDIR ${DIR}

COPY . .

RUN pip3 install pipenv==2021.11.15

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
	     echo "Install all dependencies" && \
	     pipenv install --system --deploy --ignore-pipfile --dev;  \
	else \
	     echo "Install production dependencies only" && \
	     pipenv install --system --deploy; \
	fi

RUN pip3 install -e .

# Un-comment this to ease debugging of memory problems
#RUN pip3 install memory_profiler

# Set current work directory to /tmp. This is important when running as AWS Batch job
# When using the ephemeral-storage launch template /tmp will be the mounting point for the external storage
# In AWS batch we will then mount host's /tmp directory as docker volume /tmp
WORKDIR /tmp

ENTRYPOINT ["pixetl"]