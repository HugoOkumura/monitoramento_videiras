import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import kafka from 'kafka-node';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve os arquivos estáticos compilados pelo React (produção)
app.use(express.static(path.join(__dirname, 'dist')));

app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

// Consumidor nativo do Kafka
const KAFKA_HOST = process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:29092';
console.log(`Conectando o Gateway Node.js ao Kafka em: ${KAFKA_HOST}`);

const kafkaClient = new kafka.KafkaClient({ kafkaHost: KAFKA_HOST });
const consumer = new kafka.Consumer(
    kafkaClient,
    [{ topic: 'daily.climate.bhc', partition: 0 }],
    { autoCommit: true }
);

consumer.on('message', (message) => {
    // Retransmite a string JSON vinda do Flink para todos os navegadores conectados
    wss.clients.forEach((client) => {
        if (client.readyState === 1) { // 1 = WebSocket.OPEN
            client.send(message.value);
        }
    });
});

consumer.on('error', (err) => {
    console.error('Erro no consumidor Kafka do Frontend:', err);
});

server.listen(8080, () => {
    console.log('🚀 Servidor Gateway React-Kafka rodando na porta 8080!');
});