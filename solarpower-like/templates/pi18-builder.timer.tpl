[Unit]
Description={{timer_description}}

[Timer]
OnBootSec=20
OnUnitActiveSec=5
Unit={{builder_service_name}}

[Install]
WantedBy=timers.target
