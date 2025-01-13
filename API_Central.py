from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import json
import requests
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Constants
CTC_TEMPERATURE_URL = "http://192.168.1.99:5002/get_temperature"
TAXI_REGISTRY_URL = "https://192.168.1.99:5001/status/"


def load_map():
    """Load the map from JSON file or create a new one if it doesn't exist."""
    try:
        with open('city_map.json', 'r') as file:
            city_map = json.load(file)
        return city_map
    except FileNotFoundError:
        city_map = [['' for _ in range(20)] for _ in range(20)]
        with open('city_map.json', 'w') as file:
            json.dump(city_map, file)
        return city_map


def load_taxis():
    """Load taxis data from JSON file."""
    try:
        with open('taxis.json', 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {"taxis": []}


def load_locations():
    """Load service locations from JSON file."""
    try:
        with open('EC_locations.json', 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {"locations": []}


# Main frontend route
@app.route('/')
def index():
    return render_template('index.html')


# API Routes
@app.route('/api/map', methods=['GET'])
def get_map():
    """Get current map state."""
    city_map = load_map()
    return jsonify({"map": city_map})


@app.route('/api/taxis', methods=['GET'])
def get_taxis():
    """Get all taxis information."""
    return jsonify(load_taxis())


@app.route('/api/taxi/<taxi_id>', methods=['GET'])
def get_taxi(taxi_id):
    """Get specific taxi information."""
    taxis_data = load_taxis()
    taxi = next((t for t in taxis_data['taxis'] if t['Id'] == taxi_id), None)
    if taxi:
        return jsonify(taxi)
    return jsonify({"error": "Taxi not found"}), 404


@app.route('/api/locations', methods=['GET'])
def get_locations():
    """Get all service locations."""
    return jsonify(load_locations())


@app.route('/api/temperature', methods=['GET'])
def get_temperature():
    """Get current temperature from CTC."""
    try:
        response = requests.get(CTC_TEMPERATURE_URL)
        if response.status_code == 200:
            return jsonify(response.json())
        return jsonify({"error": "Failed to get temperature"}), 500
    except requests.RequestException:
        return jsonify({"error": "CTC service unavailable"}), 503


@app.route('/api/clients', methods=['GET'])
def get_clients():
    """Get current active clients in the system."""
    city_map = load_map()
    clients = []
    for i in range(len(city_map)):
        for j in range(len(city_map[i])):
            cell = city_map[i][j]
            if isinstance(cell, str) and len(cell) == 1 and cell.islower():
                clients.append({
                    "id": cell,
                    "position": f"{i},{j}"
                })
    return jsonify({"clients": clients})


@app.route('/api/taxi/<taxi_id>/status', methods=['PUT'])
def update_taxi_status(taxi_id):
    """Update taxi status."""
    taxis_data = load_taxis()
    taxi = next((t for t in taxis_data['taxis'] if t['Id'] == taxi_id), None)
    if not taxi:
        return jsonify({"error": "Taxi not found"}), 404

    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Update allowed fields
    allowed_fields = ['Estado', 'POS', 'Libre', 'conectado']
    for field in allowed_fields:
        if field in data:
            taxi[field] = data[field]

    with open('taxis.json', 'w') as file:
        json.dump(taxis_data, file, indent=4)

    return jsonify(taxi)


@app.route('/api/system-status', methods=['GET'])
def get_system_status():
    """Get overall system status including taxis, clients, and temperature."""
    try:
        temp_response = requests.get(CTC_TEMPERATURE_URL)
        temperature = temp_response.json() if temp_response.status_code == 200 else {"error": "Temperature unavailable"}

        taxis_data = load_taxis()
        city_map = load_map()
        locations = load_locations()

        active_taxis = sum(1 for taxi in taxis_data['taxis'] if taxi.get('conectado') == 'si')
        busy_taxis = sum(1 for taxi in taxis_data['taxis'] if taxi.get('Libre') == 'no')

        return jsonify({
            "temperature": temperature,
            "active_taxis": active_taxis,
            "busy_taxis": busy_taxis,
            "total_taxis": len(taxis_data['taxis']),
            "total_locations": len(locations['locations']),
            "map_status": "active" if city_map else "error"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({"error": "Resource not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500



# Add at the beginning of your Flask app


client_requests = {}  # Global dictionary to store client requests


@app.route('/api/client-request', methods=['POST'])
def register_client_request():
    """Register a new client request with their destinations"""
    try:
        data = request.json
        if 'Requests' not in data:
            return jsonify({"error": "Invalid request format"}), 400

        # Store the destinations for each request
        for req in data['Requests']:
            client_id = req['Id'].lower()  # Ensure lowercase
            if client_id not in client_requests:
                client_requests[client_id] = []
            client_requests[client_id].append(req['Id'])

        return jsonify({"message": "Request registered successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/active-clients', methods=['GET'])
def get_active_clients():
    """Get all active clients and their destinations"""
    try:
        city_map = load_map()
        active_clients = {}
        client_positions = {}
        destination_positions = {}

        # First pass: Find destinations
        for i in range(len(city_map)):
            for j in range(len(city_map[i])):
                cell = str(city_map[i][j])
                if len(cell) == 1 and cell.isupper():
                    destination_positions[cell] = f"{i},{j}"

        # Second pass: Find clients and determine their status
        for i in range(len(city_map)):
            for j in range(len(city_map[i])):
                cell = str(city_map[i][j])

                # If it's a standalone client
                if len(cell) == 1 and cell.islower():
                    client_id = cell
                    current_pos = f"{i},{j}"
                    # Get destinations from client_requests
                    destinations = client_requests.get(client_id, [])

                    # Check if client is at any of their destinations
                    status = "esperando"
                    for dest in destinations:
                        if current_pos == destination_positions.get(dest):
                            status = "en_destino"
                            break

                    active_clients[client_id] = {
                        "destinations": destinations,  # Now it's a list
                        "current_position": current_pos,
                        "status": status
                    }

                # If it's a taxi with client
                elif len(cell) > 1 and cell[-1].islower() and cell[:-1].isdigit():
                    client_id = cell[-1]
                    current_pos = f"{i},{j}"
                    destinations = client_requests.get(client_id, [])

                    active_clients[client_id] = {
                        "destinations": destinations,  # Now it's a list
                        "current_position": current_pos,
                        "status": "en_taxi"
                    }

        return jsonify({
            "active_clients": active_clients,
            "destinations": destination_positions
        })

    except Exception as e:
        print(f"Error in get_active_clients: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, ssl_context=('server.crt', 'server.key'), debug=True)