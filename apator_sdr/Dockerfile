FROM debian:bookworm-slim

# rtl-sdr-blog = R828D / RTL-SDR V4. Debian librtlsdr hlásí FC0012 a zisk max 19.2.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 ca-certificates git cmake make g++ pkg-config libusb-1.0-0-dev libusb-1.0-0 \
 && git clone --depth 1 https://github.com/rtlsdrblog/rtl-sdr-blog.git /tmp/rtl-sdr \
 && cmake -S /tmp/rtl-sdr -B /tmp/rtl-sdr/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DINSTALL_UDEV_RULES=OFF \
      -DDETACH_KERNEL_DRIVER=ON \
 && cmake --build /tmp/rtl-sdr/build -j"$(nproc)" \
 && cmake --install /tmp/rtl-sdr/build \
 && git clone --depth 1 --branch 25.12 https://github.com/merbanan/rtl_433.git /tmp/rtl_433 \
 && cmake -S /tmp/rtl_433 -B /tmp/rtl_433/build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build /tmp/rtl_433/build -j"$(nproc)" \
 && cmake --install /tmp/rtl_433/build \
 && ldconfig \
 && apt-mark manual libusb-1.0-0 python3 ca-certificates \
 && apt-get purge -y git cmake make g++ pkg-config libusb-1.0-0-dev \
 && apt-get autoremove -y --purge \
 && rm -rf /tmp/rtl-sdr /tmp/rtl_433 /var/lib/apt/lists/*

ARG BUILD_VERSION=0.1.13
LABEL \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="addon" \
  io.hass.arch="aarch64|amd64|armv7"

WORKDIR /opt/apator
COPY apator.py ./
COPY web ./web
COPY run.sh /run.sh
RUN chmod +x /run.sh

EXPOSE 8099
CMD ["/run.sh"]
