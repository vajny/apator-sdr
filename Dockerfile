FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 rtl-433 rtl-sdr ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/apator
COPY apator.py devices.json ./
COPY web ./web
COPY run.sh /run.sh
RUN chmod +x /run.sh

EXPOSE 8099
CMD ["/run.sh"]
