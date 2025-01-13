# sensors.py
import socket
import time

class Sensor:
    def __init__(self, taxi_id, engine_ip, engine_port):
        self.id = taxi_id
        self.engine_ip = engine_ip
        self.engine_port = engine_port

    def send_sensor_data(self):
        # Enviar datos del sensor cada segundo
        while True:
            # Simulación de un mensaje 'OK' o 'KO'
            time.sleep(1)

if __name__ == "__main__":
    sensor = Sensor(1, 'localhost', 6000)
    sensor.send_sensor_data()
