wd=$(pwd)
# echo $wd
# echo $https_proxy
sudo echo "
[Unit]
Description=MRSclient


[Service]
WorkingDirectory=$wd
Environment="https_proxy=$https_proxy"
ExecStart=$wd/mrs-client
Restart=always

[Install]
WantedBy=multi-user.target
" > mrs-client.service

sudo cp mrs-client.service /etc/systemd/system/mrs-client.service
sudo systemctl daemon-reload
sudo systemctl enable mrs-client
sudo systemctl restart mrs-client
sudo systemctl status mrs-client
