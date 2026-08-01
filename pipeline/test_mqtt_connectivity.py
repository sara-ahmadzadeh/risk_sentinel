import paho.mqtt.client as mqtt

def on_message(client, userdata, msg):
    print(f"Received: {msg.topic} → {msg.payload}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message
client.connect("test.mosquitto.org", 1883, 60)
client.subscribe("risk_sentinel/#")  # Subscribe to ALL risk_sentinel topics
client.loop_forever()