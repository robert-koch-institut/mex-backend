# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

FROM python:3.15@sha256:46837ed3105db0029b6d656816185c6258e3b39690532fb782428e0a5fd76182 AS builder

WORKDIR /build

ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_NO_INPUT=on
ENV PIP_PREFER_BINARY=on
ENV PIP_PROGRESS_BAR=off

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN uv export --no-dev --no-editable | uv pip install --system --no-deps -r -

FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928

LABEL org.opencontainers.image.authors="mex@rki.de"
LABEL org.opencontainers.image.description="Backend server for the RKI metadata exchange."
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.url="https://github.com/robert-koch-institut/mex-backend"
LABEL org.opencontainers.image.vendor="robert-koch-institut"

ENV PYTHONUNBUFFERED=1
ENV PYTHONOPTIMIZE=1

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.15/site-packages /usr/local/lib/python3.15/site-packages
COPY --from=builder /usr/local/bin/backend /usr/local/bin/backend
COPY --from=builder /usr/local/bin/testing-backend /usr/local/bin/testing-backend

USER 10001

EXPOSE 8080

ENTRYPOINT [ "backend" ]
