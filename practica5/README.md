# Práctica 5 — Mensajería distribuida con MQTT y RabbitMQ

Sistema de monitoreo de flota: sensores simulados publican por MQTT en
`flota/{vehiculo_id}/{gps|temperatura|combustible}`. Un suscriptor guarda la
telemetría en SQLite y alerta por consola; un puente reenvía a RabbitMQ.

| Componente | Archivo |
|---|---|
| Brokers (Mosquitto + RabbitMQ) | `docker-compose.yml` |
| Simulador de sensores | `simulador_sensores.py` |
| Suscriptor + SQLite | `suscriptor.py` |
| Puente MQTT → RabbitMQ | `mqtt_rabbitmq_bridge.py` |

## Alertas

- Temperatura > 4 °C → `cola.alertas.temperatura` (`type: TEMP_ALERT`)
- Combustible < 20 % → `cola.combustible.nivel` (`type: FUEL_LOW`)
- Todo el GPS → `cola.gps.telemetria`

## Ejecución

```bash
cd practica5
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d

python suscriptor.py              # Terminal 1
python mqtt_rabbitmq_bridge.py    # Terminal 2
python simulador_sensores.py      # Terminal 3
```

## Verificación

- MQTT: `mosquitto_sub -h localhost -t 'flota/#' -v`
- SQLite: `sqlite3 telemetria.db "SELECT * FROM temp_data ORDER BY id DESC LIMIT 5;"`
- RabbitMQ: <http://localhost:15672> (usuario `guest`, clave `guest`), pestaña *Queues*.
