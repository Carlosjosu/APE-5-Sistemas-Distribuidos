import json
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BROKER = "localhost"
PUERTO = 1883
VEHICULOS = ["VH-001", "VH-002", "VH-003"]


def simular_gps():
    return {
        "lat": round(-2.1709 + random.uniform(-0.01, 0.01), 6),
        "lng": round(-79.9224 + random.uniform(-0.01, 0.01), 6),
        "speed": round(random.uniform(20, 80), 1),
    }


def simular_temperatura():
    return {
        "temperature": round(random.uniform(-5, 8), 1),
        "unit": "celsius",
    }


def simular_combustible():
    return {
        "fuel_level": round(random.uniform(10, 100), 1),
        "unit": "percent",
    }


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Error al conectar con el broker: {reason_code}", flush=True)
    else:
        print(f"Conectado al broker MQTT en {BROKER}:{PUERTO}", flush=True)


def publicar(client, vehiculo, sensor, datos, timestamp):
    payload = {
        "vehicle_id": vehiculo,
        "timestamp": timestamp,
        **datos,
    }
    topic = f"flota/{vehiculo}/{sensor}"
    resultado = client.publish(topic, json.dumps(payload))
    resultado.wait_for_publish()
    print(f"Publicado en {topic}: {json.dumps(payload)}", flush=True)


def main():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="simulador_sensores",
    )
    client.on_connect = on_connect
    client.connect(BROKER, PUERTO, keepalive=60)
    client.loop_start()

    print("Simulador iniciado. Publicando telemetría...", flush=True)

    try:
        while True:
            for vehiculo in VEHICULOS:
                timestamp = datetime.now(timezone.utc).isoformat()
                publicar(client, vehiculo, "gps", simular_gps(), timestamp)
                publicar(client, vehiculo, "temperatura", simular_temperatura(), timestamp)
                publicar(client, vehiculo, "combustible", simular_combustible(), timestamp)
                time.sleep(5)
    except KeyboardInterrupt:
        print("\nDeteniendo simulador...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
