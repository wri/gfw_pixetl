FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.4

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
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv

# ── Install UV-managed Python 3.12 ────────────────────────────────────────────
RUN uv python install 3.12

# ── Create a fully self-contained venv ─────────────────────────────────────────
# Deliberately NOT using --system-site-packages. That flag ties this venv to
# whatever Python version the base image's OWN system interpreter happens to
# be (see "GDAL Python bindings" below for why that used to matter and no
# longer does) -- if OSGeo ever changes the base OS/Python of this image, a
# venv built with --system-site-packages either can't see the system
# packages any more, or breaks outright. This venv only ever depends on
# libgdal.so being present, not on any particular Python install owning it,
# so bumping the FROM tag no longer risks breaking the build.
# --clear makes the build safe to re-run even if the layer cache is
# partially warm.
RUN uv venv ${VENV_DIR} --python 3.12 --clear

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

# ── GDAL Python bindings ────────────────────────────────────────────────────────
# Verify gdal-config actually resolves to this image's own GDAL build before
# relying on it -- fails the build immediately, with a clear message, rather
# than silently proceeding against whatever apt would have provided if a
# future edit accidentally reintroduces a `libgdal-dev` apt install.
RUN command -v gdal-config >/dev/null || (echo "gdal-config not found -- expected it to come from the base image's own GDAL build" && exit 1)
RUN echo "Building GDAL Python bindings against: $(gdal-config --version) ($(command -v gdal-config))"

# Built from source against whichever libgdal this base image ships,
# discovered dynamically via gdal-config rather than hardcoded -- so this
# line doesn't need to change when the FROM tag is bumped to a newer GDAL
# release. --no-build-isolation makes the build use packages already in the
# venv instead of fetching its own into an isolated build env -- which means
# setuptools/wheel must be installed here explicitly first (GDAL's own
# build_wheel step imports setuptools directly and isn't declared as a build
# dependency), and numpy must already be present (from uv sync above, since
# pyproject.toml depends on it) for numpy-based raster array support to build.
# This is the officially documented sequence for installing the GDAL Python
# bindings against a system libgdal:
# https://gdal.org/en/stable/api/python/python_bindings.html
RUN uv pip install "setuptools>=67" wheel
RUN uv pip install "gdal[numpy]==$(gdal-config --version).*" --no-build-isolation

# ── Install the application package itself
COPY . .
RUN uv pip install . --no-deps

# ── Verify the venv actually sees what it's supposed to ──────────────────────
# Fails the build (loudly, at build time) rather than shipping an image
# where osgeo isn't importable, or where a package pinned in
# pyproject.toml/uv.lock (numpy, pandas, ...) got shadowed by something
# unexpected.
RUN python3 -c "\
import subprocess; \
from osgeo import gdal; \
import numpy, pandas; \
expected = subprocess.check_output(['gdal-config', '--version']).decode().strip(); \
assert gdal.__version__.startswith(expected), f'osgeo.gdal reports {gdal.__version__} but gdal-config reports {expected} -- bindings were built against a different libgdal than this image ships'; \
print(f'OK: osgeo/gdal {gdal.__version__} matches gdal-config ({expected})'); \
mods = {'numpy': numpy, 'pandas': pandas, 'gdal_module': gdal}; \
bad = {n: m.__file__ for n, m in mods.items() if not m.__file__.startswith('${VENV_DIR}')}; \
assert not bad, f'Packages resolved outside {\"${VENV_DIR}\"}: {bad}'; \
print('OK: osgeo/numpy/pandas all resolve from the venv, not the system:'); \
[print(f'  {n}: {m.__file__}') for n, m in mods.items()]"

# ── Runtime configuration ──────────────────────────────────────────────────────
# AWS Batch mounts external ephemeral storage at /tmp, so we work there.
WORKDIR /tmp

ENV PYTHONPATH=/usr/local/app

CMD ["pixetl"]
