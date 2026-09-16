# Apator SDR

Odečet **Apator Metra E-ITN 30.2** (indikátor tepla) a **E-RM 30** (rádiový modul vodoměru) z RTL-SDR na **868.95 MHz**.

Známá zařízení v `devices.json` (potisk / rádiové ID):

- E-RM `301835244/1022` → rádio `704488428` — studená voda
- E-RM `301835238/1022` → rádio `704488422` — teplá voda
- E-ITN `30731079/0922` — topení ložnice
- E-ITN `30731088/0922` — topení kuchyň
- E-ITN `30731042/0922` — topení obývák (kód `i.P.E.3.2.`)

U E-RM se potisk liší od ID ve vzduchu o `XOR 0x38000000`; do `devices.json` můžeš dát obě čísla.

## Home Assistant OS (Raspberry Pi)

Na Pi 4 s HAOS stačí USB RTL-SDR, Mosquitto a tenhle addon. Entity vzniknou samy přes MQTT discovery.

### 1. Mosquitto

1. Settings → Add-ons → Add-on store → **Mosquitto broker** → Install → Start.
2. Settings → Devices & services → **MQTT** → přidat (pokud tam ještě není). Nech výchozí broker.

Bez běžícího Mosquitto addon **Apator SDR nenastartuje** (`mqtt:need`).

### 2. RTL-SDR

Zastrč dongle do Pi. Na Pi 4 radši **USB 2** port (černý), USB 3 umí dělat šum na 868 MHz.

### 3. Addon z GitHubu

1. Settings → Add-ons → Add-on store → ⋮ vpravo nahoře → **Repositories**.
2. Vlož `https://github.com/vajny/apator-sdr` → Add.
3. Obnov store (⋮ → Check for updates / reload).
4. Nainstaluj **Apator SDR** (může to na Pi 4 chvíli trvat — image se staví lokálně).
5. Zapni **Start on boot** a **Watchdog**, případně **Show in sidebar**.
6. Start.

Po startu je v postranní liště panel **Apator** (stejný web jako lokálně). V Settings → Devices & services → MQTT se objeví zařízení *Studená voda*, *Teplá voda*, *Topení kuchyň*, *Topení obývák*, … jakmile přijde CRC ok telegram (~4 min).

`devices.json` je v `/addon_configs/<repo>_apator_sdr/devices.json` (Samba share **addon_configs**). První start ho tam zkopíruje.

### Když dongle nevidí

V logu addonu hledej `rtl_433` / `usb_claim`. Zkus:

- jiný USB port (USB 2)
- v addonu vypnout **Protection mode**
- Settings → System → Hardware, že `rtl2838` / `usb` tam je

Tuner **FC0012** má slabší zisk; CRC občas opravíme podle známého sériového čísla.

## Lokálně (bez HA)

```bash
python3 apator_sdr/apator.py check
python3 apator_sdr/apator.py
```

Web: <http://127.0.0.1:8099/>

MQTT ručně:

```bash
MQTT_HOST=127.0.0.1 MQTT_PORT=1883 MQTT_USER=... MQTT_PASSWORD=... python3 apator_sdr/apator.py
```

Discovery: `homeassistant/sensor/apator_<id>_*/config`  
Stav: `apator/<id>/state`

## Lokální addon bez GitHub store

Přes Samba/SSH zkopíruj složku `apator_sdr/` do `/addons/apator_sdr` na HA, pak Add-on store → reload → objeví se pod Local add-ons.
