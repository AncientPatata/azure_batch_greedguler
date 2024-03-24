import requests

def create_logging_function(machine_id):
    try:
        with open('ngrok_url.txt', 'r') as url_file:
            ngrok_url = url_file.read().strip()
        log_endpoint = f"{ngrok_url}/log"
        def logger(msg):
            requests.post(log_endpoint, json={"machine": machine_id,"message":msg})
        return logger
    except Exception as e:
        print(f"Error sending log message: {e}")
        return print
