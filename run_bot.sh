cd /root/Desktop/btc/get_data/get_btc_data

docker run --rm \
  -v /root/Desktop/btc/get_data/get_btc_data:/app \
  -w /app \
  btc-bot \
  python data_process.py >> /root/Desktop/btc/get_data/get_btc_data/log.txt 2>&1