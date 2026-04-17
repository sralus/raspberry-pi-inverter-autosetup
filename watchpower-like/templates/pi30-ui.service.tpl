[Unit]
Description={{ui_description}}
After=network-online.target {{reader_service_name}}
Wants=network-online.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory={{tools_dir}}
ExecStart=/usr/bin/python3 {{tools_dir}}/ui_server.py --dir {{ui_dir}} --port {{ui_port}} --state-json {{state_json}} --ctl {{runtime_dir}}/wr1_ctl.py --device {{device_path}} --reader-service {{reader_service_name}}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
