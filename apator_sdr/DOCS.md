# Apator SDR

Odečet **Apator Metra E-ITN 30.2** (indikátor tepla) a **E-RM 30** (rádiový modul vodoměru) z RTL-SDR na **868.95 MHz**.

Měřáky se zadávají v konfiguraci addonu (sériové číslo z potisku). U E-RM se potisk liší od ID ve vzduchu o `XOR 0x38000000`; stačí napsat číslo z krabičky.

## Home Assistant OS (Raspberry Pi)

Na Pi 4 s HAOS stačí USB RTL-SDR, Mosquitto a tenhle addon. Entity vzniknou přes MQTT discovery **jen pro měřáky z konfigurace**.

### 1. Mosquitto

1. Settings → Add-ons → Add-on store → **Mosquitto broker** → Install → Start.
2. Settings → Devices & services → **MQTT** → přidat (pokud tam ještě není). Nech výchozí broker.

Bez běžícího Mosquitto addon **Apator SDR nenastartuje** (`mqtt:need`).

### 2. RTL-SDR

Zastrč dongle do Pi. Na Pi 4 radši **USB 2** port (černý), USB 3 umí dělat šum na 868 MHz.

### 3. Addon z GitHubu

1. Settings → Add-ons → Add-on store → ⋮ vpravo nahoře → **Repositories**.
2. Vlož `https://github.com/vajny/apator-sdr` → Add.
3. Obnov store, nainstaluj **Apator SDR**.
4. **Configuration** → pod *Měřáky* přidej řádky: sériové číslo, jméno, `E-ITN30` (topení) nebo `E-RM30` (voda). Save.
5. Zapni **Start on boot**, **Watchdog**, **Show in sidebar** → Start.

V liště je panel **Apator**. Neslyšené / cizí měřáky se ukážou na webu (ať víš, co opsat), MQTT entity jen pro zadané sériovky. První čistý telegram bývá do ~4 min.

### Když dongle nevidí

V logu hledej `usb_claim`, `No supported devices` nebo `rtl_433 skončil`. Zkus:

- jiný USB port (USB 2)
- v addonu vypnout **Protection mode**
- Settings → System → Hardware, že `rtl2838` / `usb` tam je

Tuner **FC0012** má slabší zisk; CRC občas opravíme podle zadaného sériového čísla.

## Lokálně (bez HA)

Do `apator_sdr/devices.json` (viz `devices.example.json`):

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
