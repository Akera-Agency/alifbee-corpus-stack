FROM ghcr.io/open-webui/open-webui:main

ARG OLLAMA_BASE_URL
ENV OLLAMA_BASE_URL=${OLLAMA_BASE_URL}