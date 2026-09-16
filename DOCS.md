# Apator SDR

Odečet **Apator Metra E-ITN 30.2** (indikátor tepla) a **E-RM 30** (rádiový modul vodoměru) z RTL-SDR na **868.95 MHz**.

Známá zařízení v `devices.json`:

- E-ITN `30731042` — kód `i.P.E.3.2.`, potisk `30731042/0922`
- E-RM `704488422` — vodoměr

## Lokálně

```bash
python3 apator.py check
python3 apator.py
```

Web: <http://127.0.0.1:8099/>

MQTT (volitelně):

```bash
MQTT_HOST=127.0.0.1 MQTT_PORT=1883 MQTT_USER=... MQTT_PASSWORD=... python3 apator.py
```

Discovery topic: `homeassistant/sensor/apator_<id>_*/config`  
Stav: `apator/<id>/state`

## Home Assistant addon

1. Mosquitto broker + integrace MQTT.
2. Složku projektu zkopíruj do `/addons/apator_sdr` na HA (Samba / Studio Code Server), **nebo** v Add-on store přidej tenhle adresář jako lokální repo.
3. Settings → Add-ons → ⋮ → Check for updates / reload.
4. Nainstaluj **Apator SDR**, povol Start on boot, USB dongle nech v hostu.
5. Addon si vezme MQTT ze supervisoru (`mqtt:want`). Entity se objeví pod zařízením *Indikátor tepla* a *Vodoměr*.
6. Panel v postranní liště (ingress) ukáže stejný web.

`devices.json` se při prvním startu zkopíruje do persistent `/data/devices.json` — další měřiče přidej tam.

RTL-SDR musí hostitel vidět (HAOS: USB). Tuner FC0012 má slabší zisk; CRC občas opravíme, když známe sériové číslo.
