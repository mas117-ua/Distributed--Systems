import sys
import time
import json
import threading
import uuid
# from kafka import KafkaProducer, KafkaConsumer  # Se comenta esta línea, ya no se usa kafka-python
from confluent_kafka import Producer, Consumer  # Nuevo import de confluent_kafka

# Definir tamaño del mapa
MAP_SIZE = 20

def validate_position(position):
    """
    Valida si la posición está dentro del mapa 20x20.
    """
    try:
        x, y = map(int, position[0].split(','))  # Usar position[0] para acceder a la cadena
        if 0 <= x < MAP_SIZE and 0 <= y < MAP_SIZE:
            return True
    except ValueError:
        return False
    return False

def load_request_data(filename):
    """
    Carga todas las solicitudes de destinos desde EC_Request.json.
    """
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            # Retorna la lista de todos los destinos en "Requests"
            if data["Requests"]:
                return [request["Id"] for request in data["Requests"]]
            else:
                print("No hay solicitudes en el archivo.")
                sys.exit(1)
    except FileNotFoundError:
        print(f"Archivo {filename} no encontrado.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Error al parsear el archivo JSON.")
        sys.exit(1)

def remove_completed_destination(filename, destination_id):
    """
    Elimina un destino completado de EC_Request.json.
    """
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            # Eliminar el destino completado
            data["Requests"] = [request for request in data["Requests"] if request["Id"] != destination_id]

        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Destino {destination_id} eliminado de {filename}.")
    except FileNotFoundError:
        print(f"Archivo {filename} no encontrado.")
    except json.JSONDecodeError:
        print("Error al parsear el archivo JSON.")
    except Exception as e:
        print(f"Error al eliminar el destino: {e}")

def send_service_request(producer, client_id, request_id, position, destination, broker_ip_port):
    """
    Envía una solicitud de servicio a Kafka.
    """
    # Crear mensaje de solicitud
    service_request = {
        "RequestId": request_id,
        "Position": position,  # Posición del cliente
        "Destination": destination  # Destino del cliente
    }
    if client_id:
        service_request["ClientId"] = client_id

    # Enviar la solicitud a Kafka (con confluent_kafka Producer)
    # Antes: producer.send('service_requests', service_request)
    # Ahora: producer.produce(topic='service_requests', value=json.dumps(service_request).encode('utf-8')), luego flush
    try:
        producer.produce(topic='service_requests', value=json.dumps(service_request).encode('utf-8'))
        producer.flush()
        print(f"Solicitud de servicio enviada: RequestId {request_id}, Cliente {client_id}, Posición {position}, Destino {destination}")
    except Exception as e:
        print(f"Error enviando la solicitud de servicio: {e}")

def listen_for_responses(client_id, request_id, broker_ip_port, status_event, position):
    # Reemplazar KafkaConsumer por Consumer de confluent_kafka con SSL y configuración necesaria
    # Configuración SSL (usar la misma que en EC_DE ejemplo anterior)
    ssl_config = {
        'security.protocol': 'ssl',
        'ssl.ca.location': '/home/mario/Escritorio/Distributed-Systems/certificados/ca-cert.pem',
        'ssl.certificate.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-cert.pem',
        'ssl.key.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-key-no-pass.pem',
        'ssl.endpoint.identification.algorithm': 'none'
    }

    consumer_conf = {
        'bootstrap.servers': broker_ip_port,
        'group.id': f"ec_customer_{uuid.uuid4()}",
        'auto.offset.reset': 'latest',
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl.ca.location'],
        'ssl.certificate.location': ssl_config['ssl.certificate.location'],
        'ssl.key.location': ssl_config['ssl.key.location'],
        'ssl.endpoint.identification.algorithm': 'none'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(['service_responses'])

    print(f"Esperando respuestas en Kafka para RequestId {request_id}...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error en el consumidor: {msg.error()}")
            continue

        try:
            response = json.loads(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"Error deserializando mensaje: {e}")
            continue

        print(f"Mensaje recibido desde Kafka: {response}")
        if response.get("RequestId") == request_id:
            assigned_client_id = response.get("ClientId")
            taxi_id = response.get("TaxiId")
            status = response.get('Status')
            if assigned_client_id and not client_id[0]:
                # Guardar el ClientId la primera vez que lo recibamos, sin importar el estado
                client_id[0] = assigned_client_id
            if status == "assigned":
                print(f"Taxi {taxi_id} asignado correctamente para Cliente {assigned_client_id}. El taxi está en camino.")
                status_event['status'] = 'assigned'
            elif status == "completed":
                new_position = response.get("NewPosition")  # Obtener la nueva posición del cliente
                print(f"Cliente {assigned_client_id}: El viaje ha sido completado con éxito. Nueva posición: {new_position}")
                status_event['status'] = 'completed'
                position[0] = new_position  # Actualizar la posición del cliente
                break
            else:
                print(f"Respuesta recibida para Cliente {assigned_client_id}: {status}")
                status_event['status'] = status
                break  # Salir del bucle si se recibe otro estado inesperado

    consumer.close()

def main():
    if len(sys.argv) != 5:
        print("Uso: python3 EC_Customer.py <Broker IP:Port> <Archivo EC_Request.json> <Posición (x,y)>")
        sys.exit(1)

    # Obtener parámetros de línea de comandos
    broker_ip_port = sys.argv[1]
    request_file = sys.argv[2]
    position = [sys.argv[4]]

    # Validar la posición
    if not validate_position(position):
        print(f"Posición {position[0]} fuera de los límites del mapa o formato incorrecto. Solicitud denegada.")
        sys.exit(1)

    # Cargar todos los destinos de la solicitud desde el archivo EC_Request.json
    destinations = load_request_data(request_file)
    client_id = [None]

    # Configurar producer de confluent_kafka con SSL
    ssl_config = {
        'security.protocol': 'ssl',
        'ssl.ca.location': '/home/mario/Escritorio/Distributed-Systems/certificados/ca-cert.pem',
        'ssl.certificate.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-cert.pem',
        'ssl.key.location': '/home/mario/Escritorio/Distributed-Systems/certificados/client-key-no-pass.pem',
        'ssl.endpoint.identification.algorithm': 'none'
    }

    producer_conf = {
        'bootstrap.servers': broker_ip_port,
        'security.protocol': 'ssl',
        'ssl.ca.location': ssl_config['ssl.ca.location'],
        'ssl.certificate.location': ssl_config['ssl.certificate.location'],
        'ssl.key.location': ssl_config['ssl.key.location'],
        'ssl.endpoint.identification.algorithm': 'none'
    }

    producer = Producer(producer_conf)

    for destination in destinations:
        # Generar el request_id una sola vez por destino
        request_id = str(uuid.uuid4())

        while True:
            status_event = {'status': None}

            # Escuchar las respuestas con referencia a la posición actualizada
            consumer_thread = threading.Thread(
                target=listen_for_responses,
                args=(client_id, request_id, broker_ip_port, status_event, position)
            )

            consumer_thread.start()

            # Pausa breve para asegurar que el consumidor esté listo
            time.sleep(1)

            # Enviar la solicitud de servicio con la posición actual
            send_service_request(producer, client_id[0], request_id, position[0], destination, broker_ip_port)

            # Esperar a que el consumidor reciba la respuesta
            consumer_thread.join()

            # Verificar el estado recibido
            status = status_event['status']
            if status == "completed":
                print(f"Servicio completado con éxito para la posición {position[0]} y destino {destination}")
                # Eliminar el destino completado de EC_Request.json
                remove_completed_destination(request_file, destination)
                break
            elif status == "assigned":
                print(f"Servicio asignado, esperando a que se complete el viaje al destino {destination}")
                break
            elif status == "No hay taxis disponibles en este momento":
                print(f"No hay taxis disponibles para el destino {destination}. Intentando de nuevo...")
                time.sleep(5)
            else:
                print(f"Estado inesperado para el destino {destination}: {status}")
                break

        # Esperar un breve intervalo antes de pasar al siguiente destino
        time.sleep(4)

if __name__ == "__main__":
    main()

