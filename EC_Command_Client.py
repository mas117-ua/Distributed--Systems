import socket
import sys
import json

HEADER = 64
FORMAT = 'utf-8'

def is_position(value):
    """
    Verifica si el valor tiene el formato de posición 'x,y'.
    """
    parts = value.split(',')
    if len(parts) == 2 and all(part.isdigit() for part in parts):
        return True
    return False

def main():
    if len(sys.argv) != 3:
        print("Uso: python EC_Command_Client.py <EC_Central IP> <EC_Central Port>")
        sys.exit(1)

    central_ip = sys.argv[1]
    central_port = int(sys.argv[2])

    # Crear el socket y conectarse al servidor central
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((central_ip, central_port))

    # Enviar mensaje inicial para indicar el tipo de cliente
    initial_message = json.dumps({"type": "command_client"})
    initial_length = str(len(initial_message)).zfill(HEADER)
    client_socket.send(initial_length.encode(FORMAT))
    client_socket.send(initial_message.encode(FORMAT))
    print("Conectado al servidor central como cliente de comandos.")

    print("Comandos disponibles:")
    print("  parar <TaxiId>")
    print("  reanudar <TaxiId>")
    print("  ir <TaxiId> <DestinoPos>")
    print("  volver <TaxiId>")

    while True:
        try:
            command = input("Ingrese un comando: ")
            parts = command.strip().split()
            if not parts:
                continue  # Si no se ingresó nada, continuar
            action = parts[0].lower()
            if action == 'parar':
                if len(parts) >= 2:
                    taxi_id = parts[1]
                    command_message = {
                        "TaxiId": taxi_id,
                        "Action": "parar"
                    }
                else:
                    print("Uso: parar <TaxiId>")
                    continue
            elif action == 'reanudar':
                if len(parts) >= 2:
                    taxi_id = parts[1]
                    command_message = {
                        "TaxiId": taxi_id,
                        "Action": "reanudar"
                    }
                else:
                    print("Uso: reanudar <TaxiId>")
                    continue
            elif action == 'ir':
                if len(parts) >=3:
                    taxi_id = parts[1]
                    destino = parts[2]
                    command_message = {
                        "TaxiId": taxi_id,
                        "Action": "ir",
                        "DestinoPos": destino
                    }
                else:
                    print("Uso: ir <TaxiId> <DestinoPos>")
                    continue
            elif action == 'volver':
                if len(parts) >=2:
                    taxi_id = parts[1]
                    command_message = {
                        "TaxiId": taxi_id,
                        "Action": "volver"
                    }
                else:
                    print("Uso: volver <TaxiId>")
                    continue
            else:
                print("Comando no reconocido. Comandos válidos: parar, reanudar, ir, volver.")
                continue

            # Enviar el comando al servidor central
            message = json.dumps(command_message)
            message_length = str(len(message)).zfill(HEADER)
            client_socket.send(message_length.encode(FORMAT))
            client_socket.send(message.encode(FORMAT))
            print(f"Comando '{action}' enviado al Taxi {taxi_id}.")

        except KeyboardInterrupt:
            print("\nCliente de comandos detenido.")
            client_socket.close()
            break

if __name__ == "__main__":
    main()
