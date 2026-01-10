#include <sMQTTBroker.h>
#include <ESP8266WiFi.h>
#include <WiFiClient.h>

#include "config.h"

sMQTTBroker broker;

// IPAddress ip4_addr(192, 168, 237, 100);
// IPAddress gateway(192, 168, 237, 254);
// IPAddress subnet(255,255,255,0);


void setup() {
  delay(10000);

  Serial.begin(115200);
  WiFi.begin(ssid, password);

  Serial.print("Connecting to ");
  Serial.println(ssid);
  while(WiFi.status() != WL_CONNECTED ){
    delay(500);
    Serial.print(".");
  }

  Serial.print("Connected! ");
  Serial.println(WiFi.localIP());
  Serial.println(WiFi.macAddress());
  broker.init(port);

}

void loop() {
  
  broker.update();

}
