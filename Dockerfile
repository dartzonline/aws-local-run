FROM python:3.12-slim
WORKDIR /opt/localrun
COPY pyproject.toml .
COPY localrun/ localrun/
RUN pip install --no-cache-dir .
EXPOSE 4566
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:4566/health')" || exit 1
ENTRYPOINT ["localrun"]
CMD ["start", "--host", "0.0.0.0"]
