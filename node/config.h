#ifndef CONFIG_H
#define CONFIG_H

// wifi network
const char* ssid; // O nome da sua rede
const char* password; // Senha da rede

// MQTT Gateway
const char* gtip; // IP da máquina que está executando o sistema
const char* topic = "uva/mesa/vitoria/leitura";
const int port = 1883;

#endif