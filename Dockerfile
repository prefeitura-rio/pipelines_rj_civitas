# Build arguments
ARG PYTHON_VERSION=3.10-slim

# Start Python image
FROM python:${PYTHON_VERSION}

# Install git, firefox and build tools (versions pinned to Debian in python:3.10-slim; bump when the base image changes)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git=1:2.47.3-0+deb13u1 \
        firefox-esr=140.9.1esr-1~deb13u1 \
        build-essential=12.12 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setting environment with prefect version
ARG PREFECT_VERSION=1.4.1
ENV PREFECT_VERSION=${PREFECT_VERSION}

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv ${VIRTUAL_ENV}
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install requirements
WORKDIR /app
COPY . .
RUN python3 -m pip install --prefer-binary --no-cache-dir -U .
