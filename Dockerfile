# Linux-side operations relay / backtesting image.
# The official MetaTrader5 Python wheels are Windows x86-64 builds; this image
# intentionally does not install MetaTrader5 or claim to host an MT5 terminal.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    OPS_BIND_HOST=0.0.0.0 \
    OPS_PORT=8080

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app app

COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt

COPY --chown=app:app . /app
RUN mkdir -p /app/backtest_results \
    && chown -R app:app /app

USER app
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2)" || exit 1

CMD ["python", "-m", "mt5_ops_webhooks"]
