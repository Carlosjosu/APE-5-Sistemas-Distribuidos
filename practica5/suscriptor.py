import json
import sqlite3

import paho.mqtt.client as mqtt

BROKER = "localhost"
PUERTO = 1883
DB_PATH = "telemetria.db"
TOPICS = [
    ("flota/+/gps", 1),
    ("flota/+/temperatura", 1),
    ("flota/+/combustible", 1),
]
TEMP_MAXIMA = 4.0
COMBUSTIBLE_MINIMO = 20.0


def crear_tablas(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS gps_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            lat REAL,
            lng REAL,
            speed REAL
        );
        CREATE TABLE IF NOT EXISTS temp_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            temperature REAL,
            unit TEXT
        );
        CREATE TABLE IF NOT EXISTS fuel_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            fuel_level REAL,
            unit TEXT
        );
        """
    )
    conn.commit()


def guardar_gps(conn, datos):
    conn.execute(
        "INSERT INTO gps_data (vehicle_id, timestamp, lat, lng, speed) VALUES (?, ?, ?, ?, ?)",
        (datos["vehicle_id"], datos["timestamp"], datos["lat"], datos["lng"], datos["speed"]),
    )


def guardar_temperatura(conn, datos):
    conn.execute(
        "INSERT INTO temp_data (vehicle_id, timestamp, temperature, unit) VALUES (?, ?, ?, ?)",
        (datos["vehicle_id"], datos["timestamp"], datos["temperature"], datos.get("unit")),
    )
    if datos["temperature"] > TEMP_MAXIMA:
        print(
            f"ALERTA: {datos['vehicle_id']} temperatura {datos['temperature']} °C "
            f"(máximo {TEMP_MAXIMA} °C)",
            flush=True,
        )


def guardar_combustible(conn, datos):
    conn.execute(
        "INSERT INTO fuel_data (vehicle_id, timestamp, fuel_level, unit) VALUES (?, ?, ?, ?)",
        (datos["vehicle_id"], datos["timestamp"], datos["fuel_level"], datos.get("unit")),
    )
    if datos["fuel_level"] < COMBUSTIBLE_MINIMO:
        print(
            f"ALERTA: {datos['vehicle_id']} combustible bajo {datos['fuel_level']} % "
            f"(mínimo {COMBUSTIBLE_MINIMO} %)",
            flush=True,
        )


MANEJADORES = {
    "gps": guardar_gps,
    "temperatura": guardar_temperatura,
    "combustible": guardar_combustible,
}


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Error al conectar con el broker: {reason_code}", flush=True)
        return
    print(f"Conectado al broker MQTT en {BROKER}:{PUERTO}", flush=True)
    # Suscribirse aquí garantiza que se renueve la suscripción tras una reconexión
    client.subscribe(TOPICS)


def on_message(client, userdata, msg):
    conn = userdata["conn"]
    partes = msg.topic.split("/")
    if len(partes) != 3 or partes[2] not in MANEJADORES:
        print(f"Topic desconocido: {msg.topic}", flush=True)
        return

    try:
        datos = json.loads(msg.payload.decode("utf-8"))
        MANEJADORES[partes[2]](conn, datos)
        conn.commit()
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"JSON inválido en {msg.topic}: {msg.payload!r}", flush=True)
    except (KeyError, TypeError) as e:
        print(f"Mensaje incompleto en {msg.topic}: falta {e}", flush=True)
    except sqlite3.Error as e:
        print(f"Error de base de datos: {e}", flush=True)
        conn.rollback()


def main():
    conn = sqlite3.connect(DB_PATH)
    crear_tablas(conn)

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="suscriptor_telemetria",
        userdata={"conn": conn},
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PUERTO, keepalive=60)

    print("Suscriptor iniciado. Esperando telemetría...", flush=True)

    try:
        # loop_forever corre los callbacks en este mismo hilo, así la conexión
        # SQLite se usa siempre desde el hilo que la creó
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nDeteniendo suscriptor...")
    finally:
        client.disconnect()
        conn.close()


if __name__ == "__main__":
    main()
