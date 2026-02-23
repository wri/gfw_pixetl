FROM ghcr.io/osgeo/gdal:ubuntu-full-3.9.3

ENV DIR=/usr/local/app \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    VENV_DIR="/.venv" \
    UV_PROJECT_ENVIRONMENT="/.venv" \
    UV_PYTHON_INSTALL_DIR="/opt/uv/python" \
    UV_PYTHON_PREFERENCE=only-managed \
    PATH="/.venv/bin:/usr/local/bin:/usr/bin:/bin"

ARG ENV

# ── System dependencies ────────────────────────────────────────────────────────
RUN apt-get update -y \
    && apt-get install --no-install-recommends -y \
        python3-dev \
        python3-venv \
        ca-certificates \
        postgresql-client \
        gcc \
        g++ \
        curl \
        git \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Install UV ─────────────────────────────────────────────────────────────────
# Pin to a specific UV release for reproducible builds.
# Update this version intentionally when you want to upgrade UV.
COPY --from=ghcr.io/astral-sh/uv:0.10.2 /uv /usr/local/bin/uv

# ── Install UV-managed Python 3.12 ────────────────────────────────────────────
RUN uv python install 3.12

# ── Create venv with access to the GDAL Python bindings ───────────────────────
# --system-site-packages propagates the GDAL Python libs installed by the
# base image into our venv, exactly as the previous Pipenv setup did.
# UV 0.10+ requires --clear to replace an existing venv; we pass it here so
# the build is safe to re-run even if the layer cache is partially warm.
RUN uv venv ${VENV_DIR} --python 3.12 --system-site-packages --clear

# ── Install Python dependencies ────────────────────────────────────────────────
RUN mkdir -p ${DIR}
WORKDIR ${DIR}

# Copy lockfile and project descriptor first so Docker can cache this layer
# independently of application source changes.
COPY pyproject.toml uv.lock ./

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
        echo "Installing all dependencies (including dev)..." && \
        uv sync --frozen --extra dev; \
    else \
        echo "Installing production dependencies only..." && \
        uv sync --frozen --no-dev; \
    fi

# ── Install the application package itself
COPY . .
RUN uv pip install . --no-deps

# ── Runtime configuration ──────────────────────────────────────────────────────
# AWS Batch mounts external ephemeral storage at /tmp, so we work there.
WORKDIR /tmp

ENV PYTHONPATH=/usr/local/app

ENTRYPOINT ["pixetl"]