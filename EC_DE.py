import socket
import requests
import json
import ssl
from requests.exceptions import SSLError
import sys
import threading
import time
import os
import signal
from urllib3.util.ssl_ import create_urllib3_context
from Crypto.Cipher import AES
import base64
import hashlib

# from kafka import KafkaProducer, KafkaConsumer # Se comenta esta línea, ya no se usa kafka-python
from confluent_kafka import Producer, Consumer  # Nuevo import de confluent_kafka

from EC_Central import HEADER  # Asegúrate de que HEADER esté configurado correctamente en ambos archivos

# Variable global para controlar el estado de pausa del taxi
taxi_paused = False
taxi_should_stop = False
taxi_should_resume = False
taxi_new_destination = None
taxi_return_to_base = False
map_size = 20
city_map = [['' for _ in range(map_size)] for _ in range(map_size)]
taxi_busy = False  # Indica si el taxi está ocupado
current_position = "0,0"  # Posición actual del taxi


# Ajustar las rutas de certificados según sea necesario
producer_conf = {
    'security.protocol': 'ssl',
    'ssl.ca.location': '/home/mario/Escritorio/Distributed-Systems/certificados/ca-cert.pem',
    'ssl.certificate.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-cert.pem',
    'ssl.key.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-key-no-pass.pem',
    'ssl.endpoint.identification.algorithm': 'none',
    'enable.ssl.certificate.verification': True
}

consumer_conf_base = {
    'security.protocol': 'ssl',
    'ssl.ca.location': '/home/mario/Escritorio/Distributed-Systems/certificados/ca-cert.pem',
    'ssl.certificate.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-cert.pem',
    'ssl.key.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-key-no-pass.pem',
    'ssl.endpoint.identification.algorithm': 'none',
    'auto.offset.reset': 'latest'
}

def listen_for_traffic_updates(broker_ip_port, taxi_id, producer):
    conf_traffic = consumer_conf_base.copy()
    conf_traffic['bootstrap.servers'] = broker_ip_port
    conf_traffic['group.id'] = f"Taxi_{taxi_id}_traffic"

    consumer = Consumer(conf_traffic)
    consumer.subscribe(['traffic_updates'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error en el consumidor traffic_updates: {msg.error()}")
            continue

        try:
            # 1) cargar el wrapper
            wrapper = json.loads(msg.value().decode('utf-8'))
            print(f"[traffic_updates] Wrapper recibido: {wrapper}")

            # 2) cargar mi token y secret_key para ESTE taxi
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)
            if not local_token or not local_secret:
                print(f"No tengo token/clave local para taxi {taxi_id}. Ignoro.")
                continue

            incoming_token = wrapper.get("token")
            if incoming_token != local_token:
                print("token != local_token, ignoro este traffic update.")
                continue

            encrypted_payload = wrapper.get("payload")
            # 3) descifrar con local_secret
            decrypted_str = decrypt_message(encrypted_payload, local_secret)
            update = json.loads(decrypted_str)  # → un dict con "status", "action", etc.

            # 4) Ahora uso 'update'
            if update.get("status") == "KO" and update.get("action") == "return_to_base":
                print(f"Taxi {taxi_id} recibió 'KO'. Regresando a la base.")
                global taxi_return_to_base
                taxi_return_to_base = True

        except Exception as e:
            print(f"Error procesando traffic_updates: {e}")

def register_taxi(taxi_id, registry_url):
    """Registrar el taxi en el sistema usando mTLS."""
    try:
        session = create_ssl_session()
        response = session.post(
            f"{registry_url}/register",
            json={"taxi_id": taxi_id},
            cert=("client.crt", "client.key"),  # Certificado del cliente
            verify="ca.crt"  # Deshabilita la verificación de certificados
        )
        if response.status_code == 201:
            print(f"Taxi {taxi_id} registrado correctamente.")
        elif response.status_code == 200:
            print(response.json()["message"])
        else:
            print(f"Error registrando taxi: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"Error durante el registro del taxi: {e}")

def check_taxi_registration(registry_url, taxi_id):
    try:
        session = create_ssl_session()
        response = session.get(
            f"{registry_url}/status/{taxi_id}",
            cert=("client.crt", "client.key"),
            verify="ca.crt"  # Deshabilita la verificación de certificados
        )
        if response.status_code == 200:
            print(f"Taxi {taxi_id} ya está registrado.")
            return True
        elif response.status_code == 404:
            print(f"Taxi {taxi_id} no está registrado.")
            return False
        else:
            print(f"Error al verificar el registro del taxi {taxi_id}: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error durante la verificación del registro del taxi: {e}")
        return False


def update_taxi_status_in_file(taxi_id, status):
    """
    Actualiza el estado "conectado" del taxi en taxis.json.
    """
    try:
        if not os.path.exists('taxis.json'):
            print("Archivo taxis.json no encontrado. Creando uno nuevo.")
            data = {'taxis': []}
        else:
            with open('taxis.json', 'r') as file:
                data = json.load(file)

        # Buscar el taxi y actualizar su estado
        taxi_found = False
        for taxi in data.get('taxis', []):
            if taxi['Id'] == taxi_id:
                taxi['conectado'] = status
                taxi_found = True
                break

        if not taxi_found:
            print(f"Taxi {taxi_id} no encontrado en taxis.json.")

        # Guardar los cambios
        with open('taxis.json', 'w') as file:
            json.dump(data, file, indent=4)

        print(f"Estado de conexión del taxi {taxi_id} actualizado a '{status}' en taxis.json.")

    except Exception as e:
        print(f"Error al actualizar el estado de conexión en taxis.json: {e}")

class SSLAdapter(requests.adapters.HTTPAdapter):
    """Adaptador SSL que ignora la verificación del CN en certificados."""
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.check_hostname = False  # Desactiva la verificación del CN/SAN
        context.verify_mode = ssl.CERT_REQUIRED  # Sigue validando el certificado

        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

    def cert_verify(self, conn, url, verify, cert):
        """Evita la verificación del hostname mientras sigue validando el certificado."""
        conn.assert_hostname = False  # Esto evita la validación del CN en requests
        return super().cert_verify(conn, url, verify, cert)

def create_ssl_session():
    """
    Crea una sesión de requests con un adaptador SSL que ignora la verificación del CN.
    """
    session = requests.Session()
    adapter = SSLAdapter()
    session.mount("https://", adapter)  # Aplica el adaptador a HTTPS

    return session

def encrypt_message(message, key):
    """Cifra un mensaje con AES-GCM usando el token como clave."""
    key = hashlib.sha256(key.encode()).digest()  # Convertir token en clave de 256 bits
    cipher = AES.new(key, AES.MODE_GCM)
    ciphertext, tag = cipher.encrypt_and_digest(message.encode('utf-8'))
    return base64.b64encode(cipher.nonce + tag + ciphertext).decode('utf-8')

def decrypt_message(encrypted_message, key):
    """Descifra un mensaje con AES-GCM usando el token como clave."""
    key = hashlib.sha256(key.encode()).digest()  # Convertir token en clave de 256 bits
    decoded = base64.b64decode(encrypted_message)
    nonce, tag, ciphertext = decoded[:16], decoded[16:32], decoded[32:]
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    return cipher.decrypt_and_verify(ciphertext, tag).decode('utf-8')

TOKENS_FILE = "taxi_tokens.json"

def load_all_local_tokens():
    """Carga el contenido completo de taxi_tokens.json como dict."""
    if not os.path.exists(TOKENS_FILE):
        return {}
    try:
        with open(TOKENS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error cargando {TOKENS_FILE}: {e}")
        return {}

def save_all_local_tokens(data):
    """Guarda `data` (un dict) en taxi_tokens.json."""
    try:
        with open(TOKENS_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error guardando {TOKENS_FILE}: {e}")

def store_token_for_taxi(taxi_id, token, secret_key):
    data = load_all_local_tokens()
    data[str(taxi_id)] = {
        "token": token,
        "secret_key": secret_key
    }
    save_all_local_tokens(data)

def get_token_for_taxi(taxi_id):
    data = load_all_local_tokens()
    entry = data.get(str(taxi_id))
    if entry is None:
        return None
    return entry["token"]  # o None si no existe "token"

def get_secret_key_for_taxi(taxi_id):
    data = load_all_local_tokens()
    entry = data.get(str(taxi_id))
    if entry is None:
        return None
    return entry["secret_key"]

def remove_token_for_taxi(taxi_id):
    data = load_all_local_tokens()
    if str(taxi_id) in data:
        del data[str(taxi_id)]
        save_all_local_tokens(data)


def manejar_sensores(ip_sensor, puerto_sensor, server_socket, taxi_id, producer):
    global taxi_paused
    direccion_sensor = (ip_sensor, int(puerto_sensor))
    sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sensor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sensor_socket.bind(direccion_sensor)
    sensor_socket.listen(1)

    print(f"Esperando conexión del sensor en {ip_sensor}:{puerto_sensor}...")

    while True:
        conn, addr = sensor_socket.accept()
        print(f"Conexión establecida con el sensor en {addr}")

        while True:
            try:
                mensaje = conn.recv(1024).decode()
                if not mensaje:
                    print("El sensor se ha desconectado. Esperando reconexión...")
                    break

                if mensaje == "KO":
                    print(f"¡Incidencia detectada desde el sensor: {mensaje}!")
                    taxi_paused = True
                    # Notificar a Kafka después de la pausa
                    producer.produce(topic='sensor_updates', value=json.dumps({"taxi_id": taxi_id, "status": "KO"}).encode('utf-8'))
                    producer.flush()
                    print(f"Actualización enviada a Kafka: {taxi_id} - KO")
                else:
                    print("Mensaje recibido del sensor:", mensaje)

            except (BrokenPipeError, ConnectionResetError):
                print("Conexión con el sensor perdida. Esperando reconexión...")
                break
        conn.close()

def consumir_mapa_actualizado(broker_ip_port, taxi_id):
    conf_map = consumer_conf_base.copy()
    conf_map['bootstrap.servers'] = broker_ip_port
    conf_map['group.id'] = f"map_updates_consumer_{int(time.time())}"

    consumer = Consumer(conf_map)
    consumer.subscribe(['map_update'])

    global city_map

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error en el consumidor map_update: {msg.error()}")
            continue

        try:
            # 1) cargar wrapper
            wrapper = json.loads(msg.value().decode('utf-8'))
            print(f"[map_update] Wrapper recibido: {wrapper}")

            # 2) cargar local token y secret
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)
            if not local_token or not local_secret:
                print("No tengo token/clave local. Ignoro map_update.")
                continue

            incoming_token = wrapper.get("token")
            if incoming_token != local_token:
                print("token != local_token, ignoro este map_update.")
                continue

            # 3) descifrar
            encrypted_payload = wrapper.get("payload")
            decrypted_str = decrypt_message(encrypted_payload, local_secret)
            data = json.loads(decrypted_str)  # → {"city_map": [...], ...} en claro

            # 4) extraer city_map
            new_map = data.get("city_map")
            if new_map:
                city_map = new_map
                os.system('clear')
                print("\nMapa actualizado recibido:")
                print_map()
                print("\n")
            else:
                print("No city_map en payload. Ignorando.")
        except Exception as e:
            print(f"Error procesando mensaje de map_update: {e}")

def print_map():
    RESET = '\033[0m'
    BLUE_BG = '\033[44m'
    YELLOW_BG = '\033[43m'
    RED_BG = '\033[41m'
    GREEN_BG = '\033[42m'

    # Imprimir encabezados de columna
    print('   ' + ' '.join(f'{j:2}' for j in range(map_size)))

    for i in range(map_size):
        # Imprimir encabezado de fila
        row_str = f'{i:2} '
        for j in range(map_size):
            cell = city_map[i][j]
            if cell == '':
                # Celda vacía
                row_str += '[ ]'
            elif isinstance(cell, str) and cell.isupper():
                # Destino (letra mayúscula)
                row_str += f"{BLUE_BG}[{cell}]{RESET}"
            elif isinstance(cell, str) and cell.islower():
                # Cliente (letra minúscula)
                row_str += f"{YELLOW_BG}[{cell}]{RESET}"
            elif str(cell).isdigit():
                # Taxi (número)
                # Aquí puedes determinar el estado del taxi si lo tienes
                row_str += f"{GREEN_BG}[{cell}]{RESET}"
            else:
                # Cualquier otro caso
                row_str += f"[{cell}]"
        print(row_str)


def load_locations(filename):
    """
    Carga las posiciones desde EC_locations.json y las devuelve como un diccionario.
    """
    with open(filename, 'r') as file:
        data = json.load(file)
        return {location['Id']: location['POS'] for location in data['locations']}

def move_towards(taxi_id, current_position, target_position, producer, action='', client_id='', request_id=''):
    # Se mantiene la lógica original sin cambios no necesarios.
    # Solo si envío mensajes, uso producer.produce en lugar de producer.send.
    # Pero aquí el código actual usa producer.send, lo cambiamos a producer.produce + flush y no alteramos comentarios ni lógica.
    # ... resto del código igual, solo cambiar send por produce.

    global taxi_paused, taxi_should_stop, taxi_should_resume, taxi_new_destination, taxi_return_to_base
    map_size = 20

    x, y = map(int, current_position.split(','))
    target_x, target_y = map(int, target_position.split(','))

    # **Establecer el estado inicial del taxi**
    if action in ['pick_up_client', 'drop_off_client']:
        taxi_estado = 'verde'
    else:
        taxi_estado = 'verde'

    while (x, y) != (target_x, target_y):
        # Revisamos si el taxi está en pausa
        if taxi_paused:
            print(f"Taxi {taxi_id} en pausa por incidencia detectada.")
            time.sleep(3)
            taxi_paused = False

        # **Revisamos si el taxi debe detenerse**
        if taxi_should_stop:
            print(f"Taxi {taxi_id} se detiene en la posición {x},{y}.")
            taxi_estado = 'rojo'

            # Obtener token y clave secreta para este taxi
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)

            # Preparar los datos a cifrar
            payload_data = {"POS": f"{x},{y}", "Estado": taxi_estado}
            if request_id:
                payload_data["RequestId"] = request_id

            # Cifrar la información con la clave secreta
            encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

            # Construir el wrapper con token y payload cifrado
            wrapper = {
                "token": local_token,
                "payload": encrypted_payload
            }

            # Publicar en el topic 'taxi_updates'
            producer.produce(topic='taxi_updates', value=json.dumps(wrapper).encode('utf-8'))
            producer.flush()

            # Esperar hasta que se reanude
            while not taxi_should_resume:
                time.sleep(1)
            taxi_should_stop = False
            taxi_should_resume = False
            taxi_estado = 'verde'
            print(f"Taxi {taxi_id} reanuda el servicio.")

        # **Revisamos si el taxi debe cambiar de destino**
        if taxi_new_destination:
            # Actualizar el destino
            target_position = taxi_new_destination
            target_x, target_y = map(int, target_position.split(','))
            print(f"Taxi {taxi_id} cambia de destino a {target_position}.")
            taxi_new_destination = None

        # **Revisamos si el taxi debe volver a la base**
        if taxi_return_to_base:
            target_position = "0,0"
            target_x, target_y = 0, 0
            print(f"Taxi {taxi_id} regresa a la base (0,0).")
            taxi_return_to_base = False

        # Movimiento en el eje X (esférico)
        if x < target_x:
            x += 1
        elif x > target_x:
            x -= 1

        # Ajustes para mapa esférico en X
        if x >= map_size:
            x = 0
        elif x < 0:
            x = map_size - 1

        # Movimiento en el eje Y (esférico)
        if y < target_y:
            y += 1
        elif y > target_y:
            y -= 1

        # Ajustes para mapa esférico en Y
        if y >= map_size:
            y = 0
        elif y < 0:
            y = map_size - 1

        # Actualizar la posición del taxi en Kafka
        new_position = f"{x},{y}"
        print(f"Taxi {taxi_id} moviéndose hacia {new_position}")

        # Preparar los datos a cifrar para la actualización durante el movimiento
        local_token = get_token_for_taxi(taxi_id)
        local_secret = get_secret_key_for_taxi(taxi_id)
        payload_data = {"POS": new_position, "Estado": taxi_estado}
        if request_id:
            payload_data["RequestId"] = request_id
        if action == 'drop_off_client' and client_id:
            payload_data["ClientId"] = client_id

        # Cifrar la información
        encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

        # Construir el wrapper y enviar
        wrapper = {
            "token": local_token,
            "payload": encrypted_payload
        }
        producer.produce(topic='taxi_updates', value=json.dumps(wrapper).encode('utf-8'))
        producer.flush()

        # Si el taxi llega a la posición del cliente y la acción es 'pick_up_client'
        if action == 'pick_up_client' and new_position == target_position:
            print(f"Taxi ha recogido al cliente {client_id}.")
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)
            payload_data = {
                "POS": new_position,
                "client_picked_up": True,
                "ClientId": client_id,
                "Estado": taxi_estado
            }
            if request_id:
                payload_data["RequestId"] = request_id

            encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

            wrapper = {
                "token": local_token,
                "payload": encrypted_payload
            }
            producer.produce(topic='taxi_updates', value=json.dumps(wrapper).encode('utf-8'))
            producer.flush()
            break

        # Si el taxi llega al destino y la acción es 'drop_off_client'
        if action == 'drop_off_client' and new_position == target_position:
            print(f"Taxi ha dejado al cliente {client_id}.")
            taxi_estado = 'rojo'
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)
            payload_data = {
                "POS": new_position,
                "client_dropped_off": True,
                "Estado": taxi_estado
            }
            if request_id:
                payload_data["RequestId"] = request_id

            encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

            wrapper = {
                "token": local_token,
                "payload": encrypted_payload
            }
            producer.produce(topic='taxi_updates', value=json.dumps(wrapper).encode('utf-8'))
            producer.flush()
            break

        time.sleep(1)

    # Retornar el ID actual del taxi (que ya no cambia)
    return taxi_id


def handle_taxi_assignments(broker_ip_port, taxi_id, destination_locations, producer):
    """
    Kafka consumer para recibir asignaciones de viaje.
    """
    conf_assign = consumer_conf_base.copy()
    conf_assign['bootstrap.servers'] = broker_ip_port
    conf_assign['group.id'] = f"Taxi_{taxi_id}_{int(time.time())}"

    consumer = Consumer(conf_assign)
    consumer.subscribe(['taxi_assignments'])

    current_position = "0,0"
    print(f"Taxi {taxi_id} listo para recibir asignaciones.")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error en el consumidor de taxi_assignments: {msg.error()}")
            continue

        try:
            # 1) Cargar el wrapper en vez de assignment en claro
            wrapper = json.loads(msg.value().decode('utf-8'))
            print(f"[taxi_assignments] Wrapper recibido: {wrapper}")

            # 2) Cargar mi token y secret_key local
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)
            if not local_token or not local_secret:
                print("No tengo token/clave local. Ignoro este taxi_assignments.")
                continue

            # 3) Verificar si el 'token' coincide con mi token local
            incoming_token = wrapper.get("token")
            if incoming_token != local_token:
                # No es para mí
                print("token != local_token, ignoro este wrapper de taxi_assignments.")
                continue

            # 4) Descifrar con mi secret_key
            encrypted_payload = wrapper.get("payload")
            decrypted_str = decrypt_message(encrypted_payload, local_secret)
            assignment = json.loads(decrypted_str)  # → JSON real con "TaxiId", "ClientId", etc.

            # 5) Verificar que "TaxiId" realmente es el mío
            if assignment.get("TaxiId") != taxi_id:
                print(f"taxi_assignments: me llegó un assignment pero TaxiId != {taxi_id}. Lo ignoro.")
                continue

            # -- A partir de aquí, tu misma lógica original:
            client_position = assignment.get("ClientPosition")
            destination_id = assignment.get("Destination")
            client_id = assignment.get("ClientId")
            request_id = assignment.get("RequestId")

            # Convertir el ID del destino en una coordenada
            if destination_id in destination_locations:
                destination_position = destination_locations[destination_id]
            else:
                print(f"Error: No se encontró la ubicación del destino {destination_id}")
                continue

            print(f"Taxi {taxi_id} asignado para recoger al cliente en {client_position} y llevarlo a {destination_id}")

            # Al iniciar el movimiento hacia el cliente, cambiar estado a 'verde'
            taxi_estado = "verde"
            # Preparar y cifrar actualización de estado
            payload_data = {
                "POS": current_position,
                "Estado": taxi_estado
            }
            if request_id:
                payload_data["RequestId"] = request_id

            # Obtener token y secret key para este taxi
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)

            # Cifrar la información
            encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

            # Construir el wrapper con token y payload cifrado
            wrapper = {
                "token": local_token,
                "payload": encrypted_payload
            }

            # Publicar en el topic 'taxi_updates'
            producer.produce(topic='taxi_updates', value=json.dumps(wrapper).encode('utf-8'))
            producer.flush()

            taxi_id = move_towards(taxi_id, current_position, client_position, producer,
                                   action='pick_up_client', client_id=client_id, request_id=request_id)
            current_position = client_position

            # Simular movimiento hacia el destino
            taxi_id = move_towards(taxi_id, current_position, destination_position, producer,
                                   action='drop_off_client', client_id=client_id, request_id=request_id)

            # Actualizar la posición actual
            current_position = destination_position

            # Taxi ha completado el viaje
            print(f"Taxi {taxi_id} ha completado el viaje a {destination_position}.")

            # Mover el taxi de regreso a la posición 0,0
            print(f"Taxi {taxi_id} regresando a la posición 0,0")
            taxi_id = move_towards(taxi_id, current_position, "0,0", producer)
            current_position = "0,0"

            # Marcar el taxi como libre en la posición 0,0
            print(f"Taxi {taxi_id} ahora está libre en la posición 0,0")
            # Leer token y secret local
            # Obtener token y secret key para este taxi
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)

            # Preparar y cifrar el mensaje de estado libre
            payload_data = {"Libre": "si", "POS": current_position}
            encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

            # Construir el wrapper y enviar
            libre_wrapper = {
                "token": local_token,
                "payload": encrypted_payload
            }
            producer.produce(topic='taxi_updates', value=json.dumps(libre_wrapper).encode('utf-8'))
            producer.flush()
            print(f"Estado de Taxi {taxi_id} actualizado a Libre en Kafka.")


        except Exception as e:
            print(f"Error descifrando taxi_assignments: {e}")

# Crear un bloqueo global para asegurar que solo un hilo imprime a la vez
print_lock = threading.Lock()

def print_thread_safe(message):
    with print_lock:
        print(message)

def handle_taxi_commands(broker_ip_port, taxi_id, destination_locations, producer):
    """
    Hilo que consume los comandos (parar, reanudar, ir, volver) dirigidos al taxi.
    """
    conf_cmd = consumer_conf_base.copy()
    conf_cmd['bootstrap.servers'] = broker_ip_port
    conf_cmd['group.id'] = f"Taxi_{taxi_id}_commands_{int(time.time())}"

    consumer = Consumer(conf_cmd)
    consumer.subscribe(['taxi_commands'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error en el consumidor de taxi_commands: {msg.error()}")
            continue

        try:
            # 1) Parsear wrapper
            wrapper = json.loads(msg.value().decode('utf-8'))
            print(f"[taxi_commands] Wrapper recibido: {wrapper}")

            # 2) Cargar mi token y secret_key local
            local_token = get_token_for_taxi(taxi_id)
            local_secret = get_secret_key_for_taxi(taxi_id)
            if not local_token or not local_secret:
                print("No tengo token/clave local. Ignoro taxi_commands.")
                continue

            # 3) Comparar token
            incoming_token = wrapper.get("token")
            if incoming_token != local_token:
                print("token != local_token, ignoro este wrapper de taxi_commands.")
                continue

            # 4) Descifrar el payload
            encrypted_payload = wrapper.get("payload")
            decrypted_str = decrypt_message(encrypted_payload, local_secret)
            command = json.loads(decrypted_str)  # → {"TaxiId": ..., "Action": ..., "DestinoPos": ...}

            # 5) Verificar que "TaxiId" realmente es el mío
            if command.get("TaxiId") != taxi_id:
                print(f"taxi_commands: me llegó un comando pero TaxiId != {taxi_id}, ignoro.")
                continue

            # -- Lógica original:
            action = command.get("Action")
            destino_id = command.get("DestinoId")
            destino_pos = command.get("DestinoPos")
            print(f"Comando recibido: {action} para Taxi {taxi_id}")

            if action == 'parar':
                taxi_stop()
            elif action == 'reanudar':
                taxi_resume()
            elif action == 'ir':
                if destino_id or destino_pos:
                    execute_go_to_position(destino_id, destino_pos, destination_locations, taxi_id, producer)
                else:
                    print("Destino no especificado en el comando 'ir'.")
            elif action == 'volver':
                execute_return_to_base(taxi_id, producer)
            else:
                print(f"Acción '{action}' no reconocida.")

        except Exception as e:
            print(f"Error descifrando taxi_commands: {e}")


def execute_go_to_position(destino_id, destino_pos, destination_locations, taxi_id_input, producer):
    global current_position, taxi_busy
    if taxi_busy:
        print(f"Taxi {taxi_id_input} está ocupado y no puede ejecutar el comando 'ir'.")
        return

    if destino_pos:
        # Validar que la posición es válida dentro del mapa
        try:
            x, y = map(int, destino_pos.split(','))
            if 0 <= x < map_size and 0 <= y < map_size:
                target_position = destino_pos
            else:
                print(f"Posición {destino_pos} fuera de los límites del mapa.")
                return
        except ValueError:
            print(f"Formato de posición inválido: {destino_pos}.")
            return
    elif destino_id and destino_id in destination_locations:
        target_position = destination_locations[destino_id]
    else:
        print(f"Destino {destino_id or destino_pos} no encontrado.")
        return

    print(f"Taxi {taxi_id_input} se moverá a la posición {target_position}.")
    # Marcar el taxi como ocupado para evitar conflictos
    taxi_busy = True
    # Mover el taxi hacia la posición
    move_to_position(taxi_id_input, current_position, target_position, producer)
    # Actualizar la posición actual
    current_position = target_position
    # Marcar el taxi como libre nuevamente
    taxi_busy = False


def execute_return_to_base(taxi_id_input, producer):
    global current_position, taxi_busy
    if taxi_busy:
        print(f"Taxi {taxi_id_input} está ocupado y no puede ejecutar el comando 'volver'.")
        return

    print(f"Taxi {taxi_id_input} regresando a la base (0,0).")
    # Marcar el taxi como ocupado para evitar conflictos
    taxi_busy = True
    # Mover el taxi hacia la posición 0,0
    move_to_position(taxi_id_input, current_position, "0,0", producer)
    # Actualizar la posición actual
    current_position = "0,0"
    # Marcar el taxi como libre nuevamente
    taxi_busy = False


def move_to_position(taxi_id, current_position, target_position, producer):
    global map_size
    x, y = map(int, current_position.split(','))
    target_x, target_y = map(int, target_position.split(','))

    while (x, y) != (target_x, target_y):
        # Movimiento en el eje X (esférico)
        if x < target_x:
            x += 1
        elif x > target_x:
            x -= 1

        # Ajustes para mapa esférico en X
        if x >= map_size:
            x = 0
        elif x < 0:
            x = map_size - 1

        # Movimiento en el eje Y (esférico)
        if y < target_y:
            y += 1
        elif y > target_y:
            y -= 1

        # Ajustes para mapa esférico en Y
        if y >= map_size:
            y = 0
        elif y < 0:
            y = map_size - 1

        # Actualizar la posición del taxi en Kafka
        new_position = f"{x},{y}"
        print(f"Taxi {taxi_id} moviéndose hacia {new_position}")

        # Enviar actualización de posición cifrada
        local_token = get_token_for_taxi(taxi_id)
        local_secret = get_secret_key_for_taxi(taxi_id)

        # Preparar y cifrar la información de posición
        payload_data = {"POS": new_position, "Estado": "rojo"}
        encrypted_payload = encrypt_message(json.dumps(payload_data), local_secret)

        # Construir el wrapper con token y payload cifrado
        wrapper = {
            "token": local_token,
            "payload": encrypted_payload
        }

        producer.produce(topic='taxi_updates', value=json.dumps(wrapper).encode('utf-8'))
        producer.flush()

        time.sleep(1)

    # Al llegar a la posición, actualizar `taxis.json` (si es necesario)
    # Actualizar taxis.json (si es necesario)
    update_taxi_position_in_file(taxi_id, target_position)
    print(f"Taxi {taxi_id} ha llegado a la posición {target_position}.")
    # Enviar actualización final de posición cifrada
    local_token = get_token_for_taxi(taxi_id)
    local_secret = get_secret_key_for_taxi(taxi_id)

    # Preparar y cifrar los datos finales
    final_payload = {"POS": target_position, "Estado": "rojo"}
    encrypted_final_payload = encrypt_message(json.dumps(final_payload), local_secret)

    # Construir el wrapper final
    final_wrapper = {
        "token": local_token,
        "payload": encrypted_final_payload
    }

    producer.produce(topic='taxi_updates', value=json.dumps(final_wrapper).encode('utf-8'))
    producer.flush()


def update_taxi_position_in_file(taxi_id, new_position):
    # Leer taxis.json
    try:
        with open('taxis.json', 'r') as file:
            data = json.load(file)
        # Encontrar el taxi y actualizar su posición
        for taxi in data.get('taxis', []):
            if taxi['Id'] == taxi_id:
                taxi['POS'] = new_position
                break
        # Guardar los cambios
        with open('taxis.json', 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Posición del taxi {taxi_id} actualizada en taxis.json a {new_position}.")
    except Exception as e:
        print(f"Error al actualizar taxis.json: {e}")


def taxi_stop():
    global taxi_should_stop
    taxi_should_stop = True
    print("Taxi se detendrá en la próxima oportunidad.")

def taxi_resume():
    global taxi_should_resume
    taxi_should_resume = True
    print("Taxi reanudará el servicio.")

def change_destination(destino_id, destination_locations):
    global taxi_new_destination
    if destino_id in destination_locations:
        taxi_new_destination = destination_locations[destino_id]
        print(f"Taxi cambiará su destino a {destino_id}.")
    else:
        print(f"Destino {destino_id} no encontrado.")

def return_to_base():
    global taxi_return_to_base
    taxi_return_to_base = True
    print("Taxi regresará a la base (0,0).")

def main():
    authenticated = False
    if len(sys.argv) != 6:
        print("Usage: python3 EC_DE.py <EC_Central IP> <EC_Central Port> <Sensor IP:PUERTO> <Broker IP:Port> <Taxi ID>")
        sys.exit(1)

    registry_url = "https://localhost:5001"
    #registry_url = "https://192.168.1.99:5001"
    central_ip = sys.argv[1]
    central_port = int(sys.argv[2])
    sensor_ip_puerto = sys.argv[3]
    broker_ip_port = sys.argv[4]
    taxi_id_input = sys.argv[5]

    # Verificar si el taxi ya está registrado
    #if not check_taxi_registration(registry_url, taxi_id_input):
        #register_taxi(taxi_id_input, registry_url)

    while True:
        print("\nMenú:")
        print("1. Registrar el taxi (Alta)")
        print("2. Eliminar el taxi (Baja)")
        print("3. Conectarse a la central")
        print("4. Salir")

        option = input("Selecciona una opción: ")

        if option == "1":
            try:
                session = create_ssl_session()
                response = session.post(
                    f"{registry_url}/register",
                    json={"taxi_id": taxi_id_input},
                    cert=("client.crt", "client.key"),
                    verify="ca.crt"  # Deshabilita la verificación de certificados
                )
                if response.status_code == 201:
                    print(f"Taxi {taxi_id_input} registrado correctamente.")
                elif response.status_code == 200:
                    print(response.json()["message"])
                else:
                    print(f"Error al registrar el taxi: {response.text}")
            except SSLError as e:
                print(f"Error SSL: {e}")

        elif option == "2":
            try:
                session = create_ssl_session()
                response = session.delete(
                    f"{registry_url}/unregister/{taxi_id_input}",
                    cert=("client.crt", "client.key"),
                    verify="ca.crt"  # Deshabilita la verificación de certificados
                )
                if response.status_code == 200:
                    print(f"Taxi {taxi_id_input} eliminado correctamente.")
                else:
                    print(f"Error al eliminar el taxi: {response.text}")
            except SSLError as e:
                print(f"Error SSL: {e}")

        elif option == "3":
            try:
                # Verificar si el taxi está registrado antes de conectarse a la central
                if not check_taxi_registration(registry_url, taxi_id_input):
                    print("Taxi no registrado. Finalizando el programa.")
                    sys.exit(1)

                # Configuración del productor Kafka
                p_conf = producer_conf.copy()
                p_conf['bootstrap.servers'] = broker_ip_port
                producer = Producer(p_conf)

                # Cargar localizaciones
                locations_file = 'EC_locations.json'
                destination_locations = load_locations(locations_file)

                # Conexión al servidor EC_Central
                print("Conectando al servidor EC_Central...")
                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.connect((central_ip, central_port))

                # Actualizar el estado del taxi en taxis.json a "conectado": "si"
                update_taxi_status_in_file(taxi_id_input, "si")

                # Leer el contenido de taxis.json
                taxis_data = "{}"  # Valor por defecto si no existe el archivo
                if os.path.exists('taxis.json'):
                    with open('taxis.json', 'r') as file:
                        taxis_data = file.read()

                # Enviar mensaje inicial con el ID del taxi y los datos
                initial_message = json.dumps({"type": "taxi", "Id": taxi_id_input, "taxis_json": taxis_data})
                initial_length = str(len(initial_message)).zfill(HEADER)
                server_socket.send(initial_length.encode('utf-8'))
                server_socket.send(initial_message.encode('utf-8'))
                print(f"ID del taxi {taxi_id_input} y JSON enviados al servidor central.")

                # Recibir longitud del mensaje esperado
                msg_length_str = server_socket.recv(HEADER).decode('utf-8').strip()
                if not msg_length_str.isdigit():
                    raise ValueError(f"Longitud de mensaje no válida: {msg_length_str}")
                msg_length = int(msg_length_str)
                print(f"Longitud esperada del mensaje: {msg_length}")

                # Recibir mensaje completo
                msg = b""
                while len(msg) < msg_length:
                    chunk = server_socket.recv(msg_length - len(msg))
                    if not chunk:
                        raise ConnectionError("Conexión cerrada antes de recibir el mensaje completo.")
                    msg += chunk

                if len(msg) != msg_length:
                    raise ValueError(f"Mensaje incompleto. Recibido {len(msg)} bytes, esperado {msg_length} bytes.")

                print(f"Mensaje completo recibido: {msg.decode('utf-8')}")

                # Parsear el mensaje recibido
                try:
                    message = json.loads(msg.decode('utf-8'))
                    print(f"Mensaje JSON parseado correctamente: {message}")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error al decodificar JSON: {e}")

                # Manejar el mensaje recibido
                if "error" in message:
                    print(f"Error desde el servidor: {message['content']}")
                    sys.exit(1)

                if message.get("type") == "confirmation":
                    local_token = message.get("token")
                    secret_key = message.get("secret_key")  # la clave que te pasa la central
                    print(f"Token recibido: {local_token}")
                    store_token_for_taxi(taxi_id_input, local_token, secret_key)
                    taxi_position = message.get("POS")
                    if not taxi_position:
                        raise ValueError("Error: No se recibió la posición inicial.")
                    print(f"Posición inicial recibida: {taxi_position}")
                    authenticated = True

            except Exception as e:
                print(f"Error conectando al servidor central: {e}")
                sys.exit(1)

            if authenticated:
                # Iniciar el hilo para manejar los sensores
                hilo_sensores = threading.Thread(target=manejar_sensores, args=(
                    sensor_ip_puerto.split(':')[0], sensor_ip_puerto.split(':')[1], server_socket, taxi_id_input,
                    producer))

                hilo_sensores.daemon = True
                hilo_sensores.start()

                # Iniciar el hilo para consumir actualizaciones del mapa desde Kafka
                hilo_mapa = threading.Thread(target=consumir_mapa_actualizado, args=(broker_ip_port, taxi_id_input))
                hilo_mapa.daemon = True
                hilo_mapa.start()

                # Iniciar el hilo para manejar asignaciones de taxi desde Kafka
                hilo_asignacion = threading.Thread(target=handle_taxi_assignments,
                                                   args=(broker_ip_port, taxi_id_input, destination_locations, producer))
                hilo_asignacion.daemon = True
                hilo_asignacion.start()

                # Iniciar el hilo para manejar comandos del taxi
                hilo_comandos = threading.Thread(target=handle_taxi_commands,
                                                 args=(broker_ip_port, taxi_id_input, destination_locations, producer))
                hilo_comandos.daemon = True
                hilo_comandos.start()

                traffic_thread = threading.Thread(target=listen_for_traffic_updates, args=(broker_ip_port, taxi_id_input, producer))
                traffic_thread.daemon = True
                traffic_thread.start()

                # Mantener el programa en ejecución
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("Taxi detenido.")

                    # Obtenemos el token y la clave secreta del taxi actual, usando la ID (p.ej. taxi_id_input)
                    local_token = get_token_for_taxi(taxi_id_input)
                    local_secret = get_secret_key_for_taxi(taxi_id_input)
                    if local_token and local_secret:
                        try:
                            # Preparar el payload de desconexión
                            payload = {"action": "disconnect"}
                            encrypted_payload = encrypt_message(json.dumps(payload), local_secret)

                            # Construir el wrapper con token y payload cifrado
                            wrapper = {"token": local_token, "payload": encrypted_payload}

                            # Enviamos un mensaje cifrado de desconexión a EC_Central
                            producer.produce(
                                topic='taxi_updates',
                                value=json.dumps(wrapper).encode('utf-8')
                            )
                            producer.flush()
                            print("Notificación de desconexión cifrada enviada a la central.")

                            # Borramos la entrada local del token para este taxi
                            remove_token_for_taxi(taxi_id_input)
                            print("Token eliminado localmente de taxi_tokens.json.")
                        except Exception as e:
                            print(f"Error al notificar desconexión: {e}")
                    sys.exit(0)

            else:
                print("No autenticado, saliendo del programa.")
                sys.exit(1)
        elif option == "4":
            print("Saliendo del programa.")
            break
        else:
            print("Opción no válida, inténtalo de nuevo.")


if __name__ == "__main__":
    main()
