# Changelog

Všechny podstatné změny v Apator SDR. Formát [Keep a Changelog](https://keepachangelog.com/cs/1.1.0/).

## [Unreleased]

## [0.1.7] - 2026-09-17

### Changed

- Log je podrobnější: RSSI, noise, FSK kmitočty, native/flex, potisk, CRC fail i u cizích ID, nedešifrovaný JSON, duplikáty native+flex.
- Na startu vypíše zisk a mapování sériovka → on-air ID.

## [0.1.6] - 2026-09-16

### Fixed

- Signál na kartě už není pomlčka: RSSI/SNR z flex kopie se sloučí, když nativní dekodér E-RM pole vynechá.
- Fallback z `rtl_433` logu (`rssi:` / `snr:`), kdy JSON metadata chybí.

## [0.1.5] - 2026-09-16

### Added

- Záložka **Discovery**: cizí CRC ok měřáky, jméno a tlačítko Přidat z webu.
- Hlavní pohled jen nakonfigurované měřáky.
- Přidání se uloží do `devices.json` / možností addonu.

### Changed

- Výchozí zisk **19.2** (Fitipower FC0012).

## [0.1.4] - 2026-09-16

### Changed

- `rtl_433` poslouchá jen Apator (`-R 0 -R 277` + flex), ne 192 cizích protokolů.
- I2C tuner FC0012 se bere vážně: zisk max 19.2, v logu je hláška.

### Fixed

- Duplicitní telegramy (nativní + flex) se nesypou dvakrát do logu/MQTT.
- Šum s rozbitým CRC od cizích ID se do logu neplete.

## [0.1.3] - 2026-09-16

### Added

- Image staví `rtl-sdr-blog` + `rtl_433` 25.12 (místo Debian 22.11).
- MQTT keepalive (ping každých 45 s), rediscovery po broken pipe.
- Na webu pruh Signál (SNR / RSSI).

### Changed

- Ingress `//api/state` se bere jako `/api/state`.

## [0.1.2] - 2026-09-16

### Added

- Měřáky se zadávají v konfiguraci addonu (sériové číslo, jméno, E-ITN30 / E-RM30).
- MQTT entity jen pro zadané sériovky.

### Fixed

- Addon se vypnul po `telegramů: 0`: `rtl_433` na Debianu padal na `-R 277`, Python skončil. Teď se rádio restartuje a web/`/health` běží dál.
- Tvoje sériová čísla nejsou default v image.

## [0.1.1] - 2026-09-16

### Added

- Repo jako Home Assistant addon store (`repository.yaml`, složka `apator_sdr/`).

## [0.1.0] - 2026-09-16

### Added

- Dekodér E-ITN 30.2 a E-RM 30 (868.95 MHz, FSK PCM).
- Web UI, MQTT Home Assistant discovery.
- HA addon skeleton (USB, ingress, Mosquitto).
