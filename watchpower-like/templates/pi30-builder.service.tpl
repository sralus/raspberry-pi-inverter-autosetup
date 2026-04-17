[Unit]
Description={{builder_description}}
After={{reader_service_name}}

[Service]
Type=oneshot
User=pi
Group=pi
WorkingDirectory={{runtime_dir}}
Environment=OUTPUT_JSON={{state_json}}
ExecStart=/usr/bin/python3 {{runtime_dir}}/wr1_state_builder.py
