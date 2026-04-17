# Raspberry Pi Inverter Auto-Setup

Dieses Projekt enthält zwei Auto-Setups für Wechselrichter auf Raspberry Pi:

- **PI30 / WR1** → `watchpower-like`
- **PI18 / WR2** → `solarpower-like`

Ziel ist eine **built-in-first** Architektur:

- kein Apache erforderlich
- kein lighttpd erforderlich
- keine Pflicht für `/var/www/html`
- die UI wird direkt über einen Python-Webserver bereitgestellt

## Einstieg

### PI30 / WR1

    python3 setup_pi30.py

### PI18 / WR2

    python3 setup_pi18.py

## Interaktive Abfragen

Die Installer fragen typischerweise:

- Geräte-/USB-Port
- Gerätename
- MQTT an/aus
- MQTT Host/Port
- MQTT Username/Passwort optional
- Poll-Intervall
- UI-Zielordner
- UI-Modus
  - 0 = built-in (empfohlen)
  - 1 = external (optional)
- UI-Port bei built-in

## Was der Installer erzeugt

Der Installer:

- kopiert Runtime-Dateien in einen lokalen Installationsordner
- kopiert tools/ui_server.py
- kopiert die UI-Dateien
- schreibt eine config.json
- erzeugt systemd-Service-Dateien im Unterordner build/
- erzeugt zusätzlich ein INSTALL_COMMANDS.sh

## Service-Namen

Die Service-Namen werden aus dem eingegebenen Gerätenamen als Slug abgeleitet.

Beispiele:

- Gerätename WR1 -> wr1-reader.service
- Gerätename Mein WR2 -> mein-wr2-reader.service

## Hinweis

Die Installer erzeugen Installationsdateien und Aktivierungsbefehle, führen diese aber nicht direkt selbst aus.
