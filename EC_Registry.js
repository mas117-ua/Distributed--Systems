const express = require('express');
const fs = require('fs');
const https = require('https');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json()); // Middleware para manejar JSON

// Base de datos en memoria para registrar taxis
const registeredTaxis = {};

// Ruta para registrar un taxi
app.post('/register', (req, res) => {
    const { taxi_id } = req.body;
    if (!taxi_id) {
        return res.status(400).json({ error: 'Missing taxi_id' });
    }

    if (registeredTaxis[taxi_id]) {
        return res.status(200).json({ message: `Taxi ${taxi_id} is already registered.` });
    }

    registeredTaxis[taxi_id] = { status: 'registered' };
    return res.status(201).json({ message: `Taxi ${taxi_id} successfully registered.` });
});

// Ruta para eliminar un taxi
app.delete('/unregister/:taxi_id', (req, res) => {
    const { taxi_id } = req.params;

    if (!registeredTaxis[taxi_id]) {
        return res.status(404).json({ error: `Taxi ${taxi_id} is not registered.` });
    }

    delete registeredTaxis[taxi_id];
    return res.status(200).json({ message: `Taxi ${taxi_id} successfully unregistered.` });
});

// Ruta para verificar el estado de un taxi
app.get('/status/:taxi_id', (req, res) => {
    const { taxi_id } = req.params;

    if (registeredTaxis[taxi_id]) {
        return res.status(200).json({ status: 'registered' });
    }
    return res.status(404).json({ status: 'not registered' });
});

// Configuración de HTTPS y mTLS
const options = {
    key: fs.readFileSync('server.key'), // Clave privada del servidor
    cert: fs.readFileSync('server.crt'), // Certificado del servidor
    ca: fs.readFileSync('ca.crt'), // Certificado de la CA
    requestCert: true, // Solicitar certificado al cliente
    rejectUnauthorized: true, // Rechazar conexiones no autorizadas
};

// Iniciar el servidor HTTPS
https.createServer(options, app).listen(5001, () => {
    console.log('EC_Registry is running on https://0.0.0.0:5001');
});
