import threading
import pygame
import sys
import socket
import json
from map import Map
from kafka import KafkaConsumer
consumer = KafkaConsumer(
    'taxi_updates',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


# Function to load taxis from taxis.json
def load_taxis(json_file):
    try:
        with open(json_file, 'r') as file:
            data = json.load(file)
            return data['taxis']  # Return the list of taxis
    except Exception as e:
        print(f"Error loading taxis: {e}")
        return []

# Function to update taxis based on the received message
def update_taxis(msg):
    try:
        print(f"update_taxis called with msg: {msg}")  # <-- Agrega este print

        taxi_data = json.loads(msg)  # Assume the server sends JSON data with taxi positions
        for taxi in taxi_data:  # Iterate through the list of taxis
            taxi_id = taxi['Id']
            pos = tuple(map(int, taxi['POS'].split(',')))  # Get position as a tuple
            print(f"Setting taxi {taxi_id} to position {pos}")  # <-- Agrega este print para verificar los taxis
            city_map.set_position(pos[0], pos[1], 1, taxi_id)  # Update the taxi's position on the map
        city_map.draw(screen, taxis)  # Redraw the map with updated taxi positions
    except Exception as e:
        print(f"Error updating taxis: {e}")

# Function to receive updates from the server
def receive_updates():
    for message in consumer:
        taxi_info = message.value
        print(f"Received taxi info: {taxi_info}")
        # Aquí actualiza la posición del taxi en el mapa
        pos = tuple(map(int, taxi_info['POS'].split(',')))
        city_map.set_position(pos[0], pos[1], 1, taxi_info['Id'])
        city_map.draw(screen, taxis)

# Initialize Pygame
pygame.init()

# Create the screen
screen = pygame.display.set_mode((600, 600))
pygame.display.set_caption("City Map")

# Create the map instance
city_map = Map()

if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 3:
        print("Usage: python3 main.py <EC_Central IP> <EC_Central Port>")
        sys.exit(1)

    # Get EC_Central IP and Port from command-line arguments
    central_ip = sys.argv[1]
    central_port = int(sys.argv[2])

    # Load initial taxis from JSON file and add them to the map
    taxis_json_file = 'taxis.json'
    taxis = load_taxis(taxis_json_file)  # Load taxis into a list
    print(f"Taxis loaded: {taxis}")  # Print the loaded taxis  # <-- Agrega aquí

    for taxi in taxis:
        taxi_id = taxi['Id']
        pos = tuple(map(int, taxi['POS'].split(',')))  # Convert POS string to a tuple
        city_map.set_position(pos[0], pos[1], 1, taxi_id)  # Add initial taxi positions to the map

    # Connect to the EC_Central server
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.connect((central_ip, central_port))
    except Exception as e:
        print(f"Error connecting to central server: {e}")
        sys.exit(1)

    receive_thread = threading.Thread(target=receive_updates)
    receive_thread.start()

    # Main loop
    running = True
    while running:
        #
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
        screen.fill((255, 255, 255))  # White background

        # Draw the map and taxis
        city_map.draw(screen, taxis)  # Draw the updated map with taxis

        # Refresh the display
        pygame.display.flip()  # Update the screen with the new frame

        # Optional: Limit the frame rate
        pygame.time.delay(2000)  # EL MAP DATA POR TERMINAL SE ACTUALIZA CADA 2 SEG
                                # EL ENTORNO GRAFICO EN PPRINCIPIO TAMBIEN PERO HAYQ IMPLEMENTAR MOVIMIENTOS

    server_socket.close()
    pygame.quit()
