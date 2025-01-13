import os
import socket
import threading
import json
# from kafka import KafkaConsumer, KafkaProducer  # Se comenta esta línea, ya no se usa kafka-python
import time
import sys
import string
import requests
import datetime
import base64
import hashlib
from confluent_kafka import Producer, Consumer  # Nuevo import de confluent_kafka
from Crypto.Cipher import AES

# Configuraciones
HEADER = 4
FORMAT = 'utf-8'
MAX_CONNECTIONS = 99
TAXI_JSON_FILE = 'taxis.json'
map_size = 20  # Tamaño del mapa (20x20)

clients = []  # Lista para mantener los clientes conectados
taxis = []  # Lista para mantener la información de los taxis
locations = []  # Lista para mantener las localizaciones de servicio
city_map = [['' for _ in range(map_size)] for _ in range(map_size)]  # Inicializar un mapa 20x20 con cadenas vacías
taxis_positions = {}  # Diccionario para rastrear posiciones de taxis
clients_positions = {}  # Diccionario para rastrear posiciones de clientes
client_destinations = {}  # Diccionario para contar los destinos pendientes de cada cliente
authenticated_taxis = []  # Lista de IDs de taxis autenticados
taxis_status = {}
destinations_positions = {}  # Diccionario para rastrear posiciones de destinos

client_letters = list(string.ascii_lowercase)
client_id_counter = 0

# Configuración SSL para confluent_kafka
ssl_config = {
    'ssl_ca_location': '/home/mario/Escritorio/Distributed-Systems/certificados/ca-cert.pem',
    'ssl_certificate_location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-cert.pem',
    'ssl_key_location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-key-no-pass.pem',
    'ssl_check_hostname': False,  # Deshabilita la verificación del hostname
    'enable.ssl.certificate.verification': True  # Deshabilitar la verificación temporalmente
}

# Almacenamiento temporal de tokens (asociados con los IDs de taxis)
#tokens = {}  # Estructura: {token: {"taxi_id": <id>, "pending_expiration": False}}
#ec_ctc_temperature_url = "http://192.168.1.99:5002/get_temperature"
ec_ctc_temperature_url = "http://127.0.0.1:5002/get_temperature"
def check_city_temperature(ec_ctc_temperature_url, producer):
    while True:
        try:
            # Solicitar la temperatura a la API
            temperature_response = requests.get(ec_ctc_temperature_url)
            if temperature_response.status_code == 200:
                temperature = temperature_response.json().get('temperature')

                if temperature is not None:
                    if temperature < 0:
                        print(f"Temperatura crítica ({temperature}°C): Enviando 'KO' a los taxis.")
                        real_payload = {"status": "KO", "action": "return_to_base", "position": "0,0"}
                    else:
                        print(f"Temperatura óptima ({temperature}°C): Enviando 'OK' a los taxis.")
                        real_payload = {"status": "OK"}

                    # Enviar *un* mensaje cifrado *por cada taxi* autenticado
                    for taxi_id in authenticated_taxis:
                        # 1) Encontrar token y secret_key de ese taxi
                        token_data = load_all_tokens()  # tokens.json a memoria
                        found_token = None
                        found_secret = None
                        for t, info in token_data.items():
                            if info["taxi_id"] == taxi_id:
                                found_token = t
                                found_secret = info["secret_key"]
                                break

                        if not found_token or not found_secret:
                            print(f"No se encontró token/clave para el taxi {taxi_id}. Omitiendo.")
                            continue

                        # 2) Cifrar el payload con found_secret
                        plaintext = json.dumps(real_payload)
                        encrypted_payload = encrypt_message(plaintext, found_secret)

                        # 3) Armar el wrapper
                        wrapper = {
                            "token": found_token,
                            "payload": encrypted_payload
                        }

                        # 4) Enviar el wrapper a 'traffic_updates'
                        producer.produce(
                            topic='traffic_updates',
                            value=json.dumps(wrapper).encode('utf-8')
                        )

                    # Importante: flush() solo una vez tras el bucle
                    producer.flush()
                else:
                    print("No se pudo obtener la temperatura. Reintento en 10 segundos.")
            else:
                print(f"Error al obtener la temperatura: {temperature_response.status_code}")

        except requests.RequestException as e:
            print(f"Error de red al consultar la temperatura: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}")

        time.sleep(10)  # Revisión cada 10 segundos



def update_map_json(city_map):
    """
    Actualiza el archivo city_map.json con los datos del mapa actualizados.
    """
    try:
        full_map = get_full_map()  # Get the updated map with concatenated IDs

        with open('city_map.json', 'w') as file:
            json.dump(city_map, file)  # Guarda el mapa actualizado en el archivo JSON
            print("Mapa guardado exitosamente en city_map.json")
    except Exception as e:
        print(f"Error al guardar el mapa: {e}")

def load_map():
    """
    Carga el mapa de la ciudad desde el archivo city_map.json.
    Si no existe, crea uno vacío.
    """
    try:
        with open('city_map.json', 'r') as file:
            return json.load(file)  # Cargamos el mapa desde el archivo
    except FileNotFoundError:
        # Si no existe, creamos un mapa vacío
        city_map = [[0 for _ in range(20)] for _ in range(20)]  # Mapa vacío de 20x20
        with open('city_map.json', 'w') as file:
            json.dump(city_map, file)
        return city_map

def log_event(event_type, source_ip, action, details):
    """
    Registra un evento en el archivo de auditoría.

    :param event_type: Tipo del evento (e.g., "Authentication", "Error").
    :param source_ip: IP de la máquina que genera el evento.
    :param action: Acción realizada (e.g., "Successful authentication").
    :param details: Detalles o parámetros adicionales del evento.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} | Source: {source_ip} | Event: {event_type} | Action: {action} | Details: {details}\n"

    try:
        with open("audit_log.txt", "a") as log_file:
            log_file.write(log_message)
    except Exception as e:
        print(f"Error al escribir en el registro de auditoría: {e}")


def generate_token_and_key(taxi_id):
    import uuid
    token_data = load_all_tokens()  # Leer tokens.json
    new_token = str(uuid.uuid4())

    # Generar la secret_key (por ejemplo, 32 bytes hex, o algo aleatorio)
    import secrets
    new_secret_key = secrets.token_hex(16)
    # => 16 bytes = 32 hex chars, AES-128, o si prefieres 32 bytes => secrets.token_hex(32)

    # Guardar en tokens.json
    token_data[new_token] = {
        "taxi_id": taxi_id,
        "secret_key": new_secret_key,
        "pending_expiration": False
    }
    save_all_tokens(token_data)  # Guardar

    return new_token, new_secret_key


def is_token_valid(token):
    token_data = load_all_tokens()
    # Verifica si el token está en el dict y no está pendiente de expiración
    return token in token_data and not token_data[token]["pending_expiration"]


TOKEN_FILE = "tokens.json"

def load_all_tokens():
    if not os.path.exists(TOKEN_FILE):
        return {}
    try:
        with open(TOKEN_FILE, 'r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error al cargar tokens: {e}")
        return {}

def save_all_tokens(token_data):
    try:
        with open(TOKEN_FILE, 'w') as file:
            json.dump(token_data, file, indent=4)
    except Exception as e:
        print(f"Error al guardar tokens: {e}")


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

#def is_taxi_registered(taxi_id):
#    """
#    Verifica si el taxi está registrado en el sistema.
#    """
#    registry_url = "https://registry.local:5001"
#    print(f"Verificando taxi_id recibido: {taxi_id}")
#    try:
#        response = requests.get(
#            f"{registry_url}/status/{taxi_id}",
#            cert=("client.crt", "client.key"),
#            verify="ca.crt"
#        )
#        print(f"Respuesta del servidor de registro: {response.status_code} - {response.text}")
#        if response.status_code == 200:
#            # Verifica explícitamente que el estado es "registered"
#            data = response.json()
#            return data.get("status") == "registered"
#        elif response.status_code == 404:
#            return False
#        else:
#            print(f"Error al verificar registro: {response.status_code}")
#            return False
#    except Exception as e:
#        print(f"Error al verificar el registro del taxi: {e}")
#        return False


def update_taxi_status(taxi_id, status):
    taxis_status[taxi_id] = status

def generate_client_id():
    global client_id_counter
    client_id = client_letters[client_id_counter % len(client_letters)]
    client_id_counter += 1
    return client_id
def is_taxi_registered(taxi_id):
    #registry_url = "https://192.168.1.99:5001"
    registry_url = "https://localhost:5001"
    try:
        response = requests.get(
            f"{registry_url}/status/{taxi_id}",
            cert=("client.crt", "client.key"),
            verify=False
        )
        if response.status_code == 200:
            data = response.json()
            registered = data.get("status") == "registered"

            # Si el taxi está registrado, asegúrate de que esté en taxis.json
            if registered:
                taxi = next((t for t in taxis if t['Id'] == taxi_id), None)
                if not taxi:
                    # Agregar taxi al archivo JSON
                    print(f"Taxi {taxi_id} registrado en el sistema pero no presente en taxis.json. Agregándolo.")
                    new_taxi = {
                        "Id": taxi_id,
                        "Estado": "rojo",
                        "POS": "0,0",
                        "Libre": "si",
                        "registrado": "si",
                        "conectado": "no"
                    }
                    taxis.append(new_taxi)
                    save_taxis()
            return registered
        return False
    except Exception as e:
        print(f"Error al verificar el registro del taxi: {e}")
        return False


def load_taxis():
    global taxis
    try:
        with open(TAXI_JSON_FILE, 'r') as file:
            data = json.load(file)
            taxis = data.get('taxis', [])
            print(f"Taxis cargados: {taxis}")
    except Exception as e:
        print(f"Error al cargar los taxis: {e}")

def remove_token_for_taxi(taxi_id):
    data = load_all_tokens()
    # Buscar el token asociado a taxi_id
    token_to_remove = None
    for token, info in data.items():
        if info["taxi_id"] == taxi_id:
            token_to_remove = token
            break
    if token_to_remove:
        del data[token_to_remove]
        save_all_tokens(data)
        print(f"Token y clave secreta para el taxi {taxi_id} eliminados de tokens.json.")
    else:
        print(f"No se encontró un token para el Taxi {taxi_id} en tokens.json.")


def handle_disconnection(taxi_id):
    print(f"Entrando en handle_disconnection para Taxi {taxi_id}")  # Depuración

    try:
        # Actualizar el estado del taxi en taxis.json
        taxi = next((t for t in taxis if t['Id'] == taxi_id), None)
        if taxi:
            taxi['conectado'] = 'no'
            save_taxis()
            print(f"Taxi {taxi_id} desconectado y actualizado en taxis.json.")
        else:
            print(f"Taxi {taxi_id} no encontrado en taxis.json.")

        # Eliminar el token asociado en tokens.json
        remove_token_for_taxi(taxi_id)

    except Exception as e:
        print(f"Error al manejar la desconexión de Taxi {taxi_id}: {e}")



def save_taxis():
    """
    Guarda los taxis en el archivo JSON.
    """
    try:
        with open(TAXI_JSON_FILE, 'w') as file:
            json.dump({'taxis': taxis}, file, indent=4)
            print("Taxis guardados.")
    except Exception as e:
        print(f"Error al guardar los taxis: {e}")

def load_locations(filename):
    global locations, destinations_positions
    with open(filename, 'r') as file:
        data = json.load(file)
        locations = data['locations']
        print("Localizaciones cargadas:", locations)

        for location in locations:
            pos = list(map(int, location['POS'].split(',')))
            destination_id = location['Id']
            x, y = pos
            if 0 <= x < map_size and 0 <= y < map_size:
                city_map[x][y] = destination_id
                destinations_positions[destination_id] = f"{x},{y}"

def is_position_valid(position):
    """
    Verifica si la posición está dentro del mapa 20x20.
    """
    pos = list(map(int, position.split(',')))
    return 0 <= pos[0] < map_size and 0 <= pos[1] < map_size

def update_taxi_position(taxi_id, new_position):
    # Remover el taxi de su posición anterior en el mapa
    old_position = taxis_positions.get(taxi_id)
    if old_position:
        x_old, y_old = map(int, old_position.split(','))
        # Restaurar el destino si estaba en esa posición
        destination_at_old = None
        for dest_id, dest_pos in destinations_positions.items():
            if dest_pos == old_position:
                destination_at_old = dest_id
                break
        if destination_at_old:
            city_map[x_old][y_old] = destination_at_old
        else:
            city_map[x_old][y_old] = ''

    # Actualizar la posición del taxi
    taxis_positions[taxi_id] = new_position
    x_new, y_new = map(int, new_position.split(','))
    city_map[x_new][y_new] = taxi_id

def update_client_position(client_id, new_position):
    # Remover el cliente de su posición anterior en el mapa
    old_position = clients_positions.get(client_id)
    if old_position:
        x_old, y_old = map(int, old_position.split(','))
        # Restaurar el destino si estaba en esa posición
        destination_at_old = None
        for dest_id, dest_pos in destinations_positions.items():
            if dest_pos == old_position:
                destination_at_old = dest_id
                break
        if destination_at_old:
            city_map[x_old][y_old] = destination_at_old
        else:
            city_map[x_old][y_old] = ''

    # Actualizar la posición del cliente
    clients_positions[client_id] = new_position
    x_new, y_new = map(int, new_position.split(','))
    city_map[x_new][y_new] = client_id

def remove_client(client_id):
    position = clients_positions.get(client_id)
    if position:
        x, y = map(int, position.split(','))
        # Restaurar el destino si estaba en esa posición
        destination_at_pos = None
        for dest_id, dest_pos in destinations_positions.items():
            if dest_pos == position:
                destination_at_pos = dest_id
                break
        if destination_at_pos:
            city_map[x][y] = destination_at_pos
        else:
            city_map[x][y] = ''

    if client_id in clients_positions:
        del clients_positions[client_id]

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
            elif cell in destinations_positions:
                # Destino
                row_str += f"{BLUE_BG}[{cell}]{RESET}"
            elif cell in clients_positions:
                # Cliente
                row_str += f"{YELLOW_BG}[{cell}]{RESET}"
            elif cell in taxis_positions:
                # Taxi
                taxi_info = taxis_status.get(cell, {})
                taxi_estado = taxi_info.get('Estado', 'rojo')
                c_id = taxi_info.get('ClientId')
                if c_id:
                    display_id = f"{cell}{c_id}"
                else:
                    display_id = cell
                if taxi_estado == 'verde':
                    row_str += f"{GREEN_BG}[{display_id}]{RESET}"
                else:
                    row_str += f"{RED_BG}[{display_id}]{RESET}"
            else:
                # Cualquier otro caso
                row_str += f"[{cell}]"
        print(row_str)

def get_full_map():
    full_map = [row[:] for row in city_map]

    # Agregar clientes al mapa
    for client_id, position in clients_positions.items():
        x, y = map(int, position.split(','))
        full_map[x][y] = client_id

    # Agregar taxis al mapa
    for taxi_id, position in taxis_positions.items():
        x, y = map(int, position.split(','))
        # Priorizar la localización si existe
        if full_map[x][y] == 0 or full_map[x][y] == '0':
            full_map[x][y] = taxi_id

        # Concatenate taxi ID and client ID for taxis with clients
    for taxi_id, taxi_info in taxis_status.items():
        if 'ClientId' in taxi_info:
            c_id = taxi_info['ClientId']
            position = taxis_positions[taxi_id]
            x, y = map(int, position.split(','))
            full_map[x][y] = f"{taxi_id}{c_id}"

    return full_map

def handle_client(conn, addr):
    global taxis, clients
    clients.append(conn)
    print(f"[NEW CONNECTION] {addr} connected.")
    # No se modifica la llamada a log_event
    log_event("Connection", addr[0], "New client connected", f"Cliente conectado desde {addr}")
    taxi_id = None
    try:
        # Recibir el tipo de cliente ('taxi' o 'command_client')
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            initial_message = json.loads(msg)
            client_type = initial_message.get('type')

            if client_type == 'taxi':
                log_event("Connection", addr[0], "Taxi connection attempt", f"Intento de conexión por taxi con mensaje: {initial_message}")
                token = initial_message.get('token')

                if token:
                    # Validar el token si está presente
                    if not is_token_valid(token):
                        print("Token inválido o expirado.")
                        error_message = {"type": "error", "content": "Token inválido o expirado"}
                        conn.send(json.dumps(error_message).encode(FORMAT))
                        conn.close()
                        return

                    # Obtener el ID del taxi asociado al token
                    token_data = load_all_tokens()
                    taxi_id = token_data[token]["taxi_id"]
                    print(f"Mensaje recibido de Taxi {taxi_id} con token válido.")
                    handle_taxi_connection(conn, initial_message)
                else:
                    # Si no hay token, debe ser un mensaje inicial de autenticación
                    print("No se envió un token. Procediendo a autenticar el taxi.")
                    handle_taxi_connection(conn, initial_message)

            elif client_type == 'command_client':
                log_event("Connection", addr[0], "Command client connection attempt", "Cliente de comandos conectado")
                handle_command_client_connection(conn)
            else:
                print(f"Tipo de cliente desconocido: {client_type}. Conexión rechazada.")
                log_event("Connection", addr[0], "Connection rejected", f"Tipo de cliente desconocido: {client_type}")
                conn.close()
                return

    except Exception as e:
        print(f"[DISCONNECTED] {addr} disconnected. Error: {e}")
        if taxi_id:
            handle_disconnection(taxi_id)
        log_event("Error", addr[0], "Unhandled exception", str(e))
    finally:
        if conn in clients:
            clients.remove(conn)
        conn.close()


def handle_taxi_connection(conn, taxi_info):
    try:
        taxi_id = taxi_info.get('Id')
        print(f"Verificando taxi_id recibido: {taxi_id}")

        # Recargar taxis desde el archivo JSON
        load_taxis()

        if not is_taxi_registered(taxi_id):
            print(f"Conexión rechazada: Taxi {taxi_id} no está registrado en el sistema.")
            log_event("Authentication", conn.getpeername()[0], "Failed authentication", f"Taxi ID {taxi_id} no está registrado")
            return

        log_event("Authentication", conn.getpeername()[0], "Successful authentication", f"Taxi ID {taxi_id} autenticado correctamente")

        print(f"Taxi {taxi_id} verificado como registrado.")
        existing_taxi = next((t for t in taxis if t['Id'] == taxi_id), None)
        if existing_taxi:
            print(f"Taxi autenticado: {taxi_id}")
            existing_taxi['conectado'] = 'si'
            save_taxis()

            token, secret_key = generate_token_and_key(taxi_id)

            if taxi_id not in authenticated_taxis:
                authenticated_taxis.append(taxi_id)

            update_taxi_position(taxi_id, existing_taxi['POS'])

            confirmation_message = {
                "type": "confirmation",
                "content": "Taxi autenticado y anyadido al mapa",
                "token": token,
                "secret_key": secret_key,
                "city_map": city_map,
                "POS": existing_taxi["POS"]
            }
            confirmation_message_json = json.dumps(confirmation_message)
            confirmation_length = f"{len(confirmation_message_json)}".zfill(HEADER)
            conn.send(confirmation_length.encode(FORMAT))
            conn.send(confirmation_message_json.encode(FORMAT))

            # Aquí antes se usaba producer.send(), ahora usamos producer.produce() y producer.flush()
            full_map = get_full_map()
            for taxi_id in authenticated_taxis:
                # 1) buscar token y secret_key para ese taxi
                found_token = None
                found_secret = None
                token_data = load_all_tokens()
                for t, info in token_data.items():
                    if info["taxi_id"] == taxi_id:
                        found_token = t
                        found_secret = info["secret_key"]
                        break

                if not found_token or not found_secret:
                    print(f"No se encontró token/clave para taxi {taxi_id}. Omitiendo.")
                    continue

                # 2) payload real
                #    Ejemplo para map_update
                real_payload = {"city_map": full_map}

                # 3) cifrar con secret_key
                plaintext = json.dumps(real_payload)
                encrypted_payload = encrypt_message(plaintext, found_secret)  # tu AES-GCM con la secret_key

                # 4) wrapper
                wrapper = {
                    "token": found_token,
                    "payload": encrypted_payload
                }

                # 5) publish
                producer.produce(topic="map_update", value=json.dumps(wrapper).encode("utf-8"))

            producer.flush()
        else:
            print(f"Taxi no registrado: {taxi_id}. Conexión rechazada.")
            error_message = {"type": "error", "content": "Taxi no registrado"}
            error_length = str(len(json.dumps(error_message))).zfill(HEADER)
            conn.send(error_length.encode(FORMAT))
            conn.send(json.dumps(error_message).encode(FORMAT))
            conn.close()
            return
    except Exception as e:
        print(f"Cliente de taxis desconectado. Error: {e}")
        log_event("Error", conn.getpeername()[0], "Taxi connection error", str(e))

def handle_command_client_connection(conn):
    # Aquí manejamos los comandos recibidos del cliente de comandos
    print("Cliente de comandos conectado.")
    while True:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                command = json.loads(msg)

                # Procesar el comando recibido
                taxi_id = command.get("TaxiId")
                action = command.get("Action")
                destino_pos = command.get("DestinoPos")
                print(f"Comando recibido: {action} para Taxi {taxi_id}")

                # Enviar el comando al taxi correspondiente
                send_command_to_taxi(taxi_id, action, destino=destino_pos)

        except Exception as e:
            print(f"Cliente de comandos desconectado. Error: {e}")
            break

def send_command_to_taxi(taxi_id, action, destino=None):
    # 1) Localizar token y clave secreta del taxi en tokens.json
    token_data = load_all_tokens()
    found_token = None
    found_secret = None

    for t, info in token_data.items():
        if info["taxi_id"] == taxi_id:
            found_token = t
            found_secret = info["secret_key"]
            break

    if not found_token or not found_secret:
        print(f"No se encontró token/clave para el taxi {taxi_id}. No se puede cifrar el comando.")
        return

    # 2) Crear la parte en claro que deseas cifrar
    command_content = {
        "TaxiId": taxi_id,
        "Action": action
    }
    if destino:
        command_content["DestinoPos"] = destino

    # 3) Cifrar con found_secret
    plaintext = json.dumps(command_content)
    encrypted_payload = encrypt_message(plaintext, found_secret)

    # 4) Armar wrapper con token + payload cifrado
    wrapper = {
        "token": found_token,
        "payload": encrypted_payload
    }

    # 5) Enviar
    producer.produce(topic='taxi_commands', value=json.dumps(wrapper).encode('utf-8'))
    producer.flush()
    print(f"Comando cifrado '{action}' enviado al Taxi {taxi_id}.")


def remove_client_if_no_taxi_assigned(client_id):
    # Verificar si el cliente sigue en el mapa
    if client_id in clients_positions:
        print(f"No se asignó taxi al cliente {client_id} después de 3 segundos. Eliminando del mapa.")
        remove_client(client_id)
        # Enviar el mapa actualizado
        full_map = get_full_map()
        for taxi_id in authenticated_taxis:
            # 1) buscar token y secret_key para ese taxi
            found_token = None
            found_secret = None
            token_data = load_all_tokens()
            for t, info in token_data.items():
                if info["taxi_id"] == taxi_id:
                    found_token = t
                    found_secret = info["secret_key"]
                    break

            if not found_token or not found_secret:
                print(f"No se encontró token/clave para taxi {taxi_id}. Omitiendo.")
                continue

            # 2) payload real
            #    Ejemplo para map_update
            real_payload = {"city_map": full_map}

            # 3) cifrar con secret_key
            plaintext = json.dumps(real_payload)
            encrypted_payload = encrypt_message(plaintext, found_secret)  # tu AES-GCM con la secret_key

            # 4) wrapper
            wrapper = {
                "token": found_token,
                "payload": encrypted_payload
            }

            # 5) publish
            producer.produce(topic="map_update", value=json.dumps(wrapper).encode("utf-8"))

        producer.flush()

def handle_service_requests(bootstrap_server):
    # Configuración del consumer con SSL
    consumer_conf = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl_ca_location'],
        'ssl.certificate.location': ssl_config['ssl_certificate_location'],
        'ssl.key.location': ssl_config['ssl_key_location'],
        'ssl.endpoint.identification.algorithm': 'none',
        'auto.offset.reset': 'latest',
        'group.id': 'service_requests_group'
    }

    consumer_sr = Consumer(consumer_conf)
    consumer_sr.subscribe(['service_requests'])

    # Configuración del producer con SSL
    producer_conf_sr = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl_ca_location'],
        'ssl.certificate.location': ssl_config['ssl_certificate_location'],
        'ssl.key.location': ssl_config['ssl_key_location'],
        'ssl.endpoint.identification.algorithm': 'none'
    }
    producer_sr = Producer(producer_conf_sr)

    request_to_client = {}

    while True:
        msg = consumer_sr.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        service_request = json.loads(msg.value().decode('utf-8'))
        request_id = service_request.get('RequestId')
        client_id = service_request.get('ClientId')
        position = service_request.get('Position')
        destination = service_request.get('Destination')

        print(f"Solicitud recibida con RequestId {request_id}, Cliente {client_id}, Posición {position}, Destino {destination}")
        log_event("Service Request", "Kafka", "New service request", f"RequestId: {request_id}, ClientId: {client_id}, Position: {position}, Destination: {destination}")

        # Log de solicitud recibida
        log_event(
            event_type="Service Request",
            source_ip="Kafka",
            action="New service request",
            details=f"RequestId: {request_id}, ClientId: {client_id}, Position: {position}, Destination: {destination}"
        )

        # Si ClientId no está proporcionado, asignar uno
        if not client_id:
            client_id = generate_client_id()
            print(f"Asignado ClientId {client_id} para RequestId {request_id}")
            request_to_client[request_id] = client_id
        else:
            print(f"ClientId {client_id} ya existe para RequestId {request_id}")

        # Agregar el cliente al diccionario de posiciones
        update_client_position(client_id, position)

        # Enviar el mapa actualizado
        full_map = get_full_map()
        for taxi_id in authenticated_taxis:
            # 1) buscar token y secret_key para ese taxi
            found_token = None
            found_secret = None
            token_data = load_all_tokens()
            for t, info in token_data.items():
                if info["taxi_id"] == taxi_id:
                    found_token = t
                    found_secret = info["secret_key"]
                    break

            if not found_token or not found_secret:
                print(f"No se encontró token/clave para taxi {taxi_id}. Omitiendo.")
                continue

            # 2) payload real
            #    Ejemplo para map_update
            real_payload = {"city_map": full_map}

            # 3) cifrar con secret_key
            plaintext = json.dumps(real_payload)
            encrypted_payload = encrypt_message(plaintext, found_secret)  # tu AES-GCM con la secret_key

            # 4) wrapper
            wrapper = {
                "token": found_token,
                "payload": encrypted_payload
            }

            # 5) publish
            producer.produce(topic="map_update", value=json.dumps(wrapper).encode("utf-8"))

        producer.flush()

        # Buscar un taxi libre
        assigned_taxi = None
        for taxi in taxis:
            if taxi.get('Libre', 'si') == 'si' and taxi.get('Id') in authenticated_taxis:
                assigned_taxi = taxi
                break

        if assigned_taxi:
            assigned_taxi['Libre'] = 'no'
            print(f"Taxi {assigned_taxi['Id']} asignado al cliente {client_id}")

            # Actualizar taxis.json con el nuevo estado del taxi
            save_taxis()

            log_event("Taxi Assignment", "Kafka", "Taxi assigned", f"Taxi ID {assigned_taxi['Id']} asignado al cliente {client_id} para destino {destination}")

            # Enviar la respuesta al cliente inmediatamente
            response_message = {
                "RequestId": request_id,
                "ClientId": client_id,
                "TaxiId": assigned_taxi['Id'],
                "Status": "assigned",
                "Position": assigned_taxi['POS'],
                "Destination": destination
            }
            producer_sr.produce(topic='service_responses', value=json.dumps(response_message).encode('utf-8'))
            producer_sr.flush()
            print(f"Respuesta enviada para el Cliente {client_id}: Taxi {assigned_taxi['Id']} asignado y en camino")

            # 1) Localizar token y clave secreta del taxi (assigned_taxi['Id'])
            token_data = load_all_tokens()
            found_token = None
            found_secret = None

            for t, info in token_data.items():
                if info["taxi_id"] == assigned_taxi['Id']:
                    found_token = t
                    found_secret = info["secret_key"]
                    break

            if not found_token or not found_secret:
                print(
                    f"No se encontró token/clave para el taxi {assigned_taxi['Id']}. No se puede cifrar la asignación.")
                return

            # 2) Cifrar la carga real (lo que antes enviabas en claro)
            plaintext = json.dumps({
                "TaxiId": assigned_taxi['Id'],
                "ClientId": client_id,
                "ClientPosition": position,
                "Destination": destination,
                "Action": "pick_up_client",
                "RequestId": request_id
            })
            encrypted_payload = encrypt_message(plaintext, found_secret)
            # Ojo: encrypt_message() es la misma que ya usas (AES-GCM con la secret_key).

            # 3) Crear un wrapper
            wrapper = {
                "token": found_token,
                "payload": encrypted_payload
            }

            # 4) Enviar ese wrapper cifrado a `taxi_assignments`
            producer_sr.produce(
                topic='taxi_assignments',
                value=json.dumps(wrapper).encode('utf-8')
            )
            producer_sr.flush()
            print(f"Asignación cifrada enviada al Taxi {assigned_taxi['Id']}")

        else:
            log_event("Service Request", "Kafka", "No taxis available", f"RequestId: {request_id}, ClientId: {client_id}, Position: {position}")
            response_message = {
                "RequestId": request_id,
                "ClientId": client_id,
                "Status": "No hay taxis disponibles en este momento"
            }
            producer_sr.produce(topic='service_responses', value=json.dumps(response_message).encode('utf-8'))
            producer_sr.flush()
            print(f"No hay taxis disponibles para Cliente {client_id}")
            remove_client_if_no_taxi_assigned(client_id)

def consume_kafka(bootstrap_server):
    global city_map
    consumer_conf = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl_ca_location'],
        'ssl.certificate.location': ssl_config['ssl_certificate_location'],
        'ssl.key.location': ssl_config['ssl_key_location'],
        'ssl.endpoint.identification.algorithm': 'none',
        'auto.offset.reset': 'latest',
        'group.id': 'taxi_updates_group'
    }

    consumer_tu = Consumer(consumer_conf)
    consumer_tu.subscribe(['taxi_updates'])

    producer_conf_tu = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl_ca_location'],
        'ssl.certificate.location': ssl_config['ssl_certificate_location'],
        'ssl.key.location': ssl_config['ssl_key_location'],
        'ssl.endpoint.identification.algorithm': 'none'
    }
    producer_tu = Producer(producer_conf_tu)

    while True:
        msg = consumer_tu.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Recibir y parsear el mensaje cifrado
        received = json.loads(msg.value().decode('utf-8'))
        print(f"Recibido mensaje cifrado de Kafka: {received}")

        # Verificar el token
        token = received.get('token')
        if not token or not is_token_valid(token):
            print(f"Error: Token inválido o ausente en el mensaje: {received}")
            log_event("Error", "Kafka", "Invalid token", f"Mensaje recibido: {received}")
            continue

        # Obtener ID y clave secreta del taxi asociado al token
        token_data = load_all_tokens()
        if token not in token_data:
            print(f"Error: Token {token} no encontrado.")
            continue
        taxi_id = token_data[token]["taxi_id"]
        secret_key = token_data[token]["secret_key"]

        # Descifrar el payload
        encrypted_payload = received.get("payload")
        try:
            decrypted_str = decrypt_message(encrypted_payload, secret_key)
            taxi_info = json.loads(decrypted_str)
            print(f"Información descifrada: {taxi_info}")
        except Exception as e:
            print(f"Error al descifrar el mensaje de taxi_updates: {e}")
            log_event("Error", "Decrypt", "Decryption failed", f"Mensaje recibido: {received}")
            continue

        #AHORA verificamos si el taxi se desconectó (dentro del mensaje descifrado)
        if taxi_info.get('action') == "disconnect":
            print(f"Taxi {taxi_id} ha solicitado desconexión. Eliminando token y actualizando estado.")

            # Llamar a `handle_disconnection` correctamente
            handle_disconnection(taxi_id)

            # Verificar si el token realmente fue eliminado
            token_data_after = load_all_tokens()
            if token not in token_data_after:
                print(f"Token {token} eliminado correctamente de tokens.json.")
            else:
                print(f"ERROR: Token {token} sigue presente en tokens.json después de handle_disconnection.")

            continue  # Saltamos el resto del procesamiento para este mensaje

        # Verificar si 'POS' está presente en la información descifrada
        if 'POS' not in taxi_info:
            print(f"El mensaje recibido para el taxi {taxi_id} no contiene posición ('POS'). Se omite la actualización.")
            continue

        # Procesar normalmente si se recibió 'POS'
        position = taxi_info['POS']
        client_id = taxi_info.get('ClientId')
        request_id = taxi_info.get('RequestId')
        estado = taxi_info.get('Estado', 'rojo')

        # Actualizar la posición del taxi
        update_taxi_position(taxi_id, position)
        log_event("Taxi Update", "Kafka", "Position update", f"Taxi {taxi_id} -> {position}")

        # Log de actualizaciones de posición
        log_event(
            event_type="Taxi Update",
            source_ip="Kafka",
            action="Position update",
            details=f"Taxi ID {taxi_id} posición actualizada a {position}"
        )

        # Actualizar el estado del taxi en taxis_status
        taxis_status[taxi_id] = {'Estado': estado}

        # Incluir ClientId en taxis_status solo si el taxi ya recogió al cliente
        if estado == 'verde' and client_id:
            taxis_status[taxi_id]['ClientId'] = client_id
        else:
            taxis_status[taxi_id].pop('ClientId', None)

        taxi_to_update = next((t for t in taxis if t['Id'] == taxi_id), None)
        if taxi_to_update:
            taxi_to_update['Estado'] = estado
            taxi_to_update['POS'] = position
            save_taxis()
            print(f"Taxi {taxi_id} actualizado en taxis.json")
        else:
            print(f"Error: Taxi {taxi_id} no encontrado en taxis.json.")

        # Si el taxi ha dejado al cliente, envía estado "completed" al cliente correspondiente
        if taxi_info.get('client_dropped_off'):
            response_message = {
                "RequestId": request_id,
                "ClientId": client_id,
                "TaxiId": taxi_id,
                "Status": "completed",
                "NewPosition": position
            }
            producer_tu.produce(topic='service_responses', value=json.dumps(response_message).encode('utf-8'))
            producer_tu.flush()
            print(f"'completed' enviado a Cliente {client_id} - Posición final {position}")

            # **Actualizar taxis_status al estado 'verde'**
            taxis_status[taxi_id] = {'Estado': 'verde'}

            # **Remover 'ClientId' de taxis_status para que en el mapa solo se muestre el ID del taxi**
            taxis_status[taxi_id].pop('ClientId', None)

        # Si el taxi ha recogido al cliente (se indica con client_picked_up flag)
        if taxi_info.get('client_picked_up'):
            # Remover el cliente del mapa
            client_position = clients_positions.get(client_id)
            if client_position:
                x, y = map(int, client_position.split(','))
                city_map[x][y] = 0
            remove_client(client_id)
            print(f"Cliente {client_id} recogido y removido del mapa.")

        # Si el taxi se marca como libre, actualizar taxis.json
        if taxi_info.get('Libre') == 'si':
            taxi_to_update = next((taxi for taxi in taxis if taxi['Id'] == taxi_id), None)
            if taxi_to_update:
                taxi_to_update['Libre'] = 'si'
                taxi_to_update['POS'] = position
                taxi_to_update['Estado'] = 'rojo'
                save_taxis()
                print(f"Taxi {taxi_id} marcado como libre y guardado en taxis.json.")

            taxis_status[taxi_id] = {'Estado': 'rojo'}


        # Enviar el mapa actualizado
        full_map = get_full_map()
        update_map_json(full_map)
        print("Mapa actualizado y guardado en city_map.json.")

        for t_id in authenticated_taxis:
            # 1) buscar token y secret_key para ese taxi
            found_token = None
            found_secret = None
            token_data = load_all_tokens()
            for tkn, info in token_data.items():
                if info["taxi_id"] == t_id:
                    found_token = tkn
                    found_secret = info["secret_key"]
                    break

            if not found_token or not found_secret:
                print(f"No se encontró token/clave para taxi {t_id}. Omitiendo.")
                continue

            # 2) payload real
            real_payload = {"city_map": full_map}

            # 3) cifrar con secret_key
            plaintext = json.dumps(real_payload)
            encrypted_payload = encrypt_message(plaintext, found_secret)

            # 4) wrapper
            wrapper = {
                "token": found_token,
                "payload": encrypted_payload
            }

            # 5) publish
            producer.produce(topic="map_update", value=json.dumps(wrapper).encode("utf-8"))

        producer.flush()

def display_city_map():
    while True:
        os.system('clear')
        print("\nCurrent City Map:")
        print_map()
        print("\n")
        time.sleep(1)

def start_server(port, kafka_server):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((SERVER, port))
    server.listen()
    print(f"[LISTENING] EC_Central listening on {SERVER}:{port}")

    log_event("Server", SERVER, "Server started", f"Server listening on port {port}")
    load_taxis()
    load_all_tokens()

    # Aquí no tocamos la lógica del hilo, solo usamos las funciones ya modificadas

    service_request_thread = threading.Thread(target=handle_service_requests, args=(kafka_server,))
    service_request_thread.start()

    # Iniciar el consumidor Kafka en un hilo separado para manejar las actualizaciones de los taxis
    kafka_thread = threading.Thread(target=consume_kafka, args=(kafka_server,))
    kafka_thread.start()

    # Iniciar la visualización del mapa en un hilo separado
    #display_thread = threading.Thread(target=display_city_map)
    #display_thread.start()

    # El hilo de temperatura también usa producer global así que no cambiamos estructura
    temperature_thread = threading.Thread(target=check_city_temperature, args=(ec_ctc_temperature_url, producer))
    temperature_thread.start()

    while True:
        conn, addr = server.accept()
        log_event("Connection", addr[0], "New connection", f"Cliente conectado desde {addr}")
        if threading.active_count() - 1 < MAX_CONNECTIONS:
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python EC_Central.py <port> <kafka_broker_ip:port>")
        sys.exit(1)

    PORT = int(sys.argv[1])
    bootstrap_server = sys.argv[2]
    #SERVER = "192.168.1.84"
    SERVER = "localhost"

    # Configuración del productor con confluent_kafka
    producer_conf = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl_ca_location'],
        'ssl.certificate.location': ssl_config['ssl_certificate_location'],
        'ssl.key.location': ssl_config['ssl_key_location'],
        'ssl.endpoint.identification.algorithm': 'none',
        'enable.ssl.certificate.verification': True
    }

    # Inicializar el productor global (no cambiamos lógica, solo el tipo de producer)
    producer = Producer(producer_conf)

    # Cargar localizaciones antes de iniciar el servidor
    load_locations('EC_locations.json')

    start_server(PORT, bootstrap_server)
