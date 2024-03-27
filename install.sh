# !/bin/sh

sudo apt update -y
sudo apt install python3-pip -y
sudo apt install python3-dev
sudo apt-get install -y --no-install-recommends openmpi-bin
sudo apt-get install -y libopenmpi-dev
sudo pip3 install -r requirements_node.txt
sudo pip3 install gdown
gdown "1hwZZT1IgYVfc6dKbM3mDP9ygR2vmhUAQ" --folder --output data
