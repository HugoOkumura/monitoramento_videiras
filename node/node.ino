#include <PubSubClient.h>
#include <WiFi.h>
#include <DHT11.h>

#include "config.h"

#define DHT_PIN 23
#define MSG_SIZE 50
#define READING_BUFFER_SIZE 10

typedef struct{
  float temp;
  float humidity;
} readingData;

WiFiClient espClient;
PubSubClient client(espClient);

// timers
hw_timer_t *readingTimer = NULL;
const int readTimerOut = 5000; // time in miliseconds 

hw_timer_t *sendingTimer = NULL;
const int sendTimerOut = 16000;

// state tracking
volatile bool readFlag = false;
volatile bool sendFlag = false;

// DHT11 sensor
DHT11 dht11(DHT_PIN);
float tempMed;
float humMed;

// buffers
char msg[MSG_SIZE];
readingData readings[READING_BUFFER_SIZE];
uint8_t count = 0;

//interrupt functions
void IRAM_ATTR onReadTimer(){
  readFlag = true;
}

void IRAM_ATTR onSendTimer(){
  sendFlag = true;
}

void setTimers(){
  readingTimer = timerBegin(1000000);  // timer 1MHz resolution
  timerAttachInterrupt(readingTimer, &onReadTimer);
  timerAlarm(readingTimer, (readTimerOut * 1000), true, 0);

  sendingTimer = timerBegin(1000000);
  timerAttachInterrupt(sendingTimer, &onSendTimer);
  timerAlarm(sendingTimer, (sendTimerOut * 1000), true, 0);
}

void endTimers(){
  timerEnd(readingTimer);
  timerEnd(sendingTimer);
}

void initWifi(){
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  Serial.print("Connecting to ");
  Serial.println(ssid);
  while(WiFi.status() != WL_CONNECTED){
    Serial.print(".");
    delay(500);
  }

  Serial.print("Connected! ");
  Serial.println(WiFi.localIP());
}

void reconnect(){
  endTimers();      
  while(!client.connected()){
    Serial.println("Connecting to MQTT Server...");
    if(client.connect("WS001")){
      Serial.println("connected!");
    } else{
      Serial.print("failed, rc=");
      Serial.print(client.state());
      Serial.println(" trying again in 5 seconds");
      delay(5000);
    }
  }
  setTimers();
}

void readDHT11(){
  int temp = 0;
  int hum = 0;

  int result = dht11.readTemperatureHumidity(temp, hum);
  if(result == 0){
    readingData data = {temp, hum};
    readings[count] = data;
  }

  count++;
}

void sendMSG(){
  int tempSum = 0;
  int humSum = 0;

  for(int i = 0; i < count; i++){
    tempSum += readings[i].temp;
    humSum += readings[i].humidity;
  }

  tempMed = tempSum / float(count);
  humMed  = humSum  / float(count);

  String payload = "{";
  payload += "\"temperatura\":" + String(tempMed) + ",";
  payload += "\"umidade\":" + String(humMed);
  payload += "}";

  payload.toCharArray(msg, MSG_SIZE);

  Serial.println("Publishing...");
  client.publish(topic, msg);
    
}

void callback(void* topic, byte* payload, unsigned int length){
    Serial.println("Published");
    Serial.print("Median Temperature: ");
    Serial.print(tempMed);
    Serial.print("  -  Median Humidity: ");
    Serial.println(humMed);
}

void setup() {
  delay(2000);
  Serial.begin(115200);
  initWifi();

  client.setServer(gtip,port);
  client.setCallback(callback);

}

void loop() {

  if(!client.connected()){
    reconnect();
  }

  client.loop();

  if(readFlag){
    readDHT11();
    readFlag = false;
  }

  if(sendFlag){
    sendMSG();
    sendFlag = false;
    count = 0;
  }

}