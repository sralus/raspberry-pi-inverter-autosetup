[Unit]
Description={{service_description}}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory={{runtime_dir}}
Environment=MQTT_ENABLED={{mqtt_enabled}}
Environment=BROKER_HOST={{mqtt_host}}
Environment=BROKER_PORT={{mqtt_port}}
Environment=BROKER_USERNAME={{mqtt_username}}
Environment=BROKER_PASSWORD={{mqtt_password}}
ExecStart=/usr/bin/python3 {{runtime_dir}}/wr2_infini_live_mqtt_loop.py {{device_path}} {{device_name}} {{poll_interval}}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
