import requests
from flask import Flask, jsonify, request
import threading
import time

app = Flask(__name__)


class EC_CTC:
    def __init__(self):
        self.city = "Alicante"
        self.api_key = ""
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        self.current_temperature = None
        self.last_temperature = None

    def set_city(self, city):
        self.city = city
        self.current_temperature = None  # Reset temperatures when changing city
        self.last_temperature = None

    def fetch_temperature(self):
        params = {
            'q': self.city,
            'appid': self.api_key,
            'units': 'metric'
        }
        response = requests.get(self.base_url, params=params)
        data = response.json()
        if response.status_code == 200:
            return data['main']['temp']
        else:
            print(f"Error fetching temperature: {data['message']}")
            return None

    def update_temperature(self):
        new_temperature = self.fetch_temperature()
        if new_temperature is not None:
            self.last_temperature = self.current_temperature
            self.current_temperature = new_temperature

    def temperature_changed(self):
        return (
                self.last_temperature is not None
                and self.current_temperature != self.last_temperature
        )


ec_ctc = EC_CTC()


@app.route('/set_city', methods=['POST'])
def set_city():
    city = request.json.get('city')
    ec_ctc.set_city(city)
    return jsonify({"message": "City set successfully"}), 200


@app.route('/get_temperature', methods=['GET'])
def get_temperature():
    ec_ctc.update_temperature()
    if ec_ctc.current_temperature is not None:
        return jsonify({"temperature": ec_ctc.current_temperature}), 200
    else:
        return jsonify({"error": "Unable to fetch temperature"}), 500


def periodic_temperature_check():
    while True:
        ec_ctc.update_temperature()
        if ec_ctc.temperature_changed():
            print(f"Temperature in {ec_ctc.city} changed! New: {ec_ctc.current_temperature}°C")
        time.sleep(10)  # Check every 10 seconds


def run_flask_app(ip):
    app.run(host=ip, port=5002, threaded=True)


def menu():
    while True:
        print("\nMenu:")
        print("1. Set City")
        print("2. Get Temperature")
        print("3. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            city = input("Enter city name: ")
            ec_ctc.set_city(city)
            print(f"City set to {city}")
        elif choice == '2':
            if ec_ctc.current_temperature is not None:
                print(f"The current temperature in {ec_ctc.city} is {ec_ctc.current_temperature}°C.")
            else:
                print("Unable to fetch temperature. Please check the city name or API.")
        elif choice == '3':
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == '__main__':
    ip = input("Enter the IP address to bind the application (e.g., 127.0.0.1 or your LAN IP): ")

    flask_thread = threading.Thread(target=run_flask_app, args=(ip,))
    flask_thread.start()

    # Start a background thread to check temperature periodically
    temperature_thread = threading.Thread(target=periodic_temperature_check, daemon=True)
    temperature_thread.start()

    menu()
