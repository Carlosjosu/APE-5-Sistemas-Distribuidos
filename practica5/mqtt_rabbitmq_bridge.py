import json
import queue
import time

import paho.mqtt.client as mqtt
import pika

MQTT_BROKER = "localhost"
MQTT_PUERTO = 1883
MQTT_TOPIC = "flota/#"

RABBIT_HOST = "localhost"
RABBIT_PUERTO = 5672
RABBIT_USUARIO = "guest"
RABBIT_CLAVE = "guest"

COLA_GPS = "cola.gps.telemetria"
COLA_TEMPERATURA = "cola.alertas.temperatura"
COLA_COMBUSTIBLE = "cola.combustible.nivel"
COLAS = [COLA_GPS, COLA_TEMPERATURA, COLA_COMBUSTIBLE]

TEMP_MAXIMA = 4.0
COMBUSTIBLE_MINIMO = 20.0

# Cola interna entre el hilo de paho (productor) y el hilo principal, que es
# el único que toca la conexión de pika (BlockingConnection no es thread-safe)
pendientes = queue.Queue()


def clasificar(sensor, datos):
    """Devuelve (cola_destino, mensaje) o None si no hay que reenviarlo."""
    if sensor == "gps":
        return COLA_GPS, datos
    if sensor == "temperatura" and datos["temperature"] > TEMP_MAXIMA:
        return COLA_TEMPERATURA, {
            **datos,
            "type": "TEMP_ALERT",
            "message": f"Temperatura {datos['temperature']} °C supera {TEMP_MAXIMA} °C",
        }
    if sensor == "combustible" and datos["fuel_level"] < COMBUSTIBLE_MINIMO:
        return COLA_COMBUSTIBLE, {
            **datos,
            "type": "FUEL_LOW",
            "message": f"Combustible {datos['fuel_level']} % por debajo de {COMBUSTIBLE_MINIMO} %",
        }
    return None


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Error al conectar con el broker MQTT: {reason_code}", flush=True)
        return
    print(f"Conectado al broker MQTT en {MQTT_BROKER}:{MQTT_PUERTO}", flush=True)
    client.subscribe(MQTT_TOPIC, qos=1)


def on_message(client, userdata, msg):
    partes = msg.topic.split("/")
    if len(partes) != 3:
        return

    try:
        datos = json.loads(msg.payload.decode("utf-8"))
        destino = clasificar(partes[2], datos)
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"JSON inválido en {msg.topic}: {msg.payload!r}", flush=True)
        return
    except (KeyError, TypeError) as e:
        print(f"Mensaje incompleto en {msg.topic}: falta {e}", flush=True)
        return

    if destino is not None:
        pendientes.put(destino)


def conectar_rabbitmq():
    parametros = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PUERTO,
        credentials=pika.PlainCredentials(RABBIT_USUARIO, RABBIT_CLAVE),
        heartbeat=60,
    )
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    for cola in COLAS:
        canal.queue_declare(queue=cola, durable=True)
    print(f"Conectado a RabbitMQ en {RABBIT_HOST}:{RABBIT_PUERTO}", flush=True)
    return conexion, canal


def publicar(canal, cola, mensaje):
    canal.basic_publish(
        exchange="",
        routing_key=cola,
        body=json.dumps(mensaje, ensure_ascii=False),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )
    print(f"Reenviado a {cola}: {json.dumps(mensaje, ensure_ascii=False)}", flush=True)


def reenviar(conexion, canal):
    """Bucle principal: publica lo pendiente y atiende los heartbeats de pika."""
    while True:
        try:
            cola, mensaje = pendientes.get(timeout=1)
        except queue.Empty:
            pass
        else:
            try:
                publicar(canal, cola, mensaje)
            except pika.exceptions.AMQPError:
                # Se devuelve a la cola interna para no perderlo al reconectar
                pendientes.put((cola, mensaje))
                raise
        # Aunque no lleguen alertas, esto responde los heartbeats del servidor
        # y evita que RabbitMQ cierre la conexión por inactividad
        conexion.process_data_events(time_limit=0)


def main():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="puente_mqtt_rabbitmq",
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PUERTO, keepalive=60)
    client.loop_start()

    print("Puente MQTT → RabbitMQ iniciado.", flush=True)

    conexion = None
    try:
        while True:
            try:
                conexion, canal = conectar_rabbitmq()
                reenviar(conexion, canal)
            except pika.exceptions.AMQPError as e:
                print(f"Conexión con RabbitMQ perdida ({e!r}). Reintentando en 5 s...", flush=True)
                time.sleep(5)
    except KeyboardInterrupt:
        print("\nDeteniendo puente...")
    finally:
        client.loop_stop()
        client.disconnect()
        if conexion is not None and conexion.is_open:
            conexion.close()


if __name__ == "__main__":
    main()
