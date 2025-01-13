from flask import Flask, request, jsonify
import ssl
import json
import os

app = Flask(__name__)

# Ruta del archivo JSON
TAXIS_JSON_FILE = "taxis.json"

def load_taxis():
    """Cargar taxis desde el archivo JSON."""
    if os.path.exists(TAXIS_JSON_FILE):
        with open(TAXIS_JSON_FILE, "r") as file:
            try:
                data = json.load(file)
                return data.get("taxis", [])
            except json.JSONDecodeError:
                return []
    return []

def save_taxis(taxis):
    """Guardar taxis en el archivo JSON."""
    with open(TAXIS_JSON_FILE, "w") as file:
        json.dump({"taxis": taxis}, file, indent=4)

@app.route('/register', methods=['POST'])
def register_taxi():
    """Registrar un taxi en el sistema."""
    data = request.get_json()
    taxi_id = data.get("taxi_id")
    if not taxi_id:
        return jsonify({"error": "Missing taxi_id"}), 400

    # Cargar taxis desde el archivo
    taxis = load_taxis()

    # Verificar si el taxi ya está registrado
    for taxi in taxis:
        if taxi["Id"] == taxi_id:
            return jsonify({"message": f"Taxi {taxi_id} is already registered."}), 200

    # Agregar el taxi al archivo JSON
    new_taxi = {
        "Id": taxi_id,
        "Estado": "rojo",
        "POS": "0,0",
        "Libre": "si",
        "registrado": "si",  # Marcar como registrado
        "conectado": "no"  # Inicialmente no está conectado
    }
    taxis.append(new_taxi)
    save_taxis(taxis)

    return jsonify({"message": f"Taxi {taxi_id} successfully registered."}), 201


@app.route('/unregister/<taxi_id>', methods=['DELETE'])
def unregister_taxi(taxi_id):
    """Eliminar un taxi del sistema."""
    # Cargar taxis desde el archivo
    taxis = load_taxis()

    # Verificar si el taxi está registrado
    taxi_found = False
    for taxi in taxis:
        if taxi["Id"] == taxi_id:
            taxis.remove(taxi)
            taxi_found = True
            break

    if not taxi_found:
        return jsonify({"error": f"Taxi {taxi_id} is not registered."}), 404

    # Guardar los cambios en el archivo JSON
    save_taxis(taxis)

    return jsonify({"message": f"Taxi {taxi_id} successfully unregistered."}), 200


@app.route('/status/<taxi_id>', methods=['GET'])
def check_taxi_status(taxi_id):
    """Verificar si un taxi está registrado."""
    # Cargar taxis desde el archivo
    taxis = load_taxis()

    # Verificar si el taxi está registrado
    for taxi in taxis:
        if taxi["Id"] == taxi_id:
            return jsonify({"status": "registered"}), 200

    return jsonify({"status": "not registered"}), 404


if __name__ == '__main__':
    # Configurar HTTPS con autenticación mutua (mTLS)
    context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile="server.crt", keyfile="server.key")  # Certificado del servidor
    context.load_verify_locations("ca.crt")  # Certificado de la CA para validar clientes
    context.verify_mode = ssl.CERT_REQUIRED  # Requiere que el cliente presente un certificado válido

    app.run(host='0.0.0.0', port=5001, ssl_context=context)
