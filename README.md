### To install the required packages ###
pip install -r requirement.txt

### to run the code ###
bash run

### or run in docker ###
sudo docker build -t data_collector .
sudo docker run -d -p 8123:8123 -v /home/tbjc1magic/log:/log image bash run \
  --port 8123 --log_storage /log
