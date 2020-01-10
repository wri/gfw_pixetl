FROM fedora:30

ENV DIR=/usr/local/app

RUN sed -i '/^[fedora]/a\exclude=postgresql*' /etc/yum.repos.d/fedora.repo \
    && sed -i '/^[updates]/a\exclude=postgresql*' /etc/yum.repos.d/fedora-updates.repo

RUN dnf install -y https://download.postgresql.org/pub/repos/yum/12/fedora/fedora-30-x86_64/pgdg-fedora-repo-latest.noarch.rpm

RUN dnf install -y \
    make \
    automake \
    gcc \
    gcc-c++ \
    kernel-devel \
    libpq-devel \
    python3 \
    python3-devel \
    gdal \
    gdal-python-tools \
    && dnf clean all


RUN mkdir -p ${DIR}
WORKDIR ${DIR}

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .
RUN pip3 install -e .

WORKDIR /tmp

ENTRYPOINT ["pixetl"]