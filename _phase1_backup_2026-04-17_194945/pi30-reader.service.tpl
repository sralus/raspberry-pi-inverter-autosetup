[Unit]
Description={{service_description}}
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi
ExecStart=/usr/bin/python3 {{install_runtime_dir}}/wr1_live_mqtt_loop.py {{device_path}} {{device_name}} {{poll_interval}}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
