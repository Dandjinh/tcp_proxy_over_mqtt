# Tcp Proxy Over Mqtt

This project aims to build a tcp tunnel between two devices over mqtt connection.

## Usage

1. run `python client.py` on the device A that runs ssh server with port 9022(can be changed in `server.py`)
2. run `python server.py` on the device B
3. run `ssh -p 12345 root@127.0.0.1` on device B and build the ssh connection with device A

## Enviroment Configuration

| Item | Description |
| :--: | :--: |
| GATEWAY_SN | topic identifier |
| MQTT_HOST | server of mqtt broker |
