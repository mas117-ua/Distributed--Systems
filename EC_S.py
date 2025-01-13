import socket
import sys
import threading
import time
import tty
import termios  # Para capturar teclas en Linux/Unix


# Función para detectar una incidencia cuando se presiona cualquier tecla
def detectar_incidencia():
    global incidente
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while True:
            if sys.stdin.read(1):  # Lee un carácter sin esperar Enter
                incidente = True
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def enviar_estado(ip, puerto):
    global incidente
    incidente = False
    direccion = (ip, int(puerto))

    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                s.connect(direccion)
                print(f"Conectado al Digital Engine en {ip}:{puerto}")

                while True:
                    if incidente:
                        # Enviar KO una vez y esperar
                        estado = "KO"
                        s.sendall(estado.encode())
                        print(f"Mensaje enviado: {estado} - Vehículo detenido por 3 segundos")
                        time.sleep(3)  # Detención de 3 segundos en EC_S también
                        incidente = False  # Reseteamos la incidencia

                        # Enviar OK de nuevo para continuar
                        estado = "OK"
                        s.sendall(estado.encode())
                        print(f"Mensaje enviado: {estado}")
                    else:
                        # Enviar OK si no hay incidencia
                        estado = "OK"
                        s.sendall(estado.encode())
                        print(f"Mensaje enviado: {estado}")

                    time.sleep(1)  # Enviamos el estado cada segundo

            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError):
                print(
                    f"Error de conexión con el Digital Engine en {ip}:{puerto}. Intentando reconectar en 5 segundos...")
                time.sleep(5)  # Espera antes de intentar reconectar

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python EC_S.py <IP:PUERTO>")
        sys.exit(1)

    # Extraemos IP y puerto del argumento
    ip_puerto = sys.argv[1]
    try:
        ip_ec_de, puerto_ec_de = ip_puerto.split(":")
    except ValueError:
        print("Error: formato incorrecto, debe ser IP:PUERTO")
        sys.exit(1)

    # Creamos un hilo para manejar la simulación de incidencias con cualquier tecla
    hilo_incidencia = threading.Thread(target=detectar_incidencia)
    hilo_incidencia.daemon = True  # Hilo en segundo plano
    hilo_incidencia.start()

    # Iniciamos la función de enviar estado
    enviar_estado(ip_ec_de, puerto_ec_de)
