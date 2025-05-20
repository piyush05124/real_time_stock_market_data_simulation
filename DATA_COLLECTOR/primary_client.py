import logging
import socket,redis,re
from ast import literal_eval
import threading,queue,json
from datetime import datetime
#-----------------------------------------------#
from configparser import ConfigParser
config_object = ConfigParser()
config_object.read("DATA_COLLECTOR/generatorConfig.ini")
cred = config_object['REDIS']
host,port,db = cred['host'],cred['port'],cred['db']
symbol_list = cred['symbols']

symbol_list = literal_eval(symbol_list)
r = redis.StrictRedis(host=host, port=port, db=db)
#-------------------------------------------------#
date = str(datetime.now().date())

data_queue = queue.Queue()
time_data_list = []
flag = 0



class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "datetime": datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S'),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        return json.dumps(log_record)

def get_json_logger(name: str, log_file: str = "app_json.log", level=logging.INFO) -> logging.Logger:
    """
    Creates a logger that outputs logs in JSON format with datetime, module, function, and line number.

    Args:
        name (str): Name of the logger.
        log_file (str): Path to the log file.
        level: Logging level (default is logging.INFO).

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = JSONFormatter()

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger





logger = get_json_logger("receivingLogger")


#============================================================================ redis data pushing
def redis_list(key,data,symbol_list):
    try:
        incoming_symbol = data["symbol"]

        matches = [symbol for symbol in symbol_list if incoming_symbol.startswith(symbol)]
        if len(matches)>0:
            key=f"{key}_{matches[-1]}"
        r.rpush(key,json.dumps(data))
        # print(f'Successfully pushed to {key} list ')
    except Exception as e:
        print("Unable to push into list")

#============================================================================ socket data receiving
def receive_data(host,port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        # Put the received data into the queue
        data_queue.put(data.decode('utf-8'))

    client_socket.close()


#============================================================================ data processing & redis pushing
def purify_data(z):
    json_objects = re.findall(r'\{.*?\}', z)
    dict_list = [literal_eval(obj) for obj in json_objects]
    res = []
    for d in dict_list:
        res.append(d)
    return res



def process_data(flag = flag,key=date):
    while True:
        data = data_queue.get()
        if flag == 0:
            key = date
            flag = 1
        #----------------|
        if data is None:#|   termination sig
            break       #|
        #----------------|
        try:
            if type(data)==str:
                j_data = literal_eval(data)
                redis_list(f"{key}",j_data,symbol_list)  # ----------redis pushing
                logger.info("successfully fetched")
        except Exception as e:
            # print("Error occured")
            p_data  = purify_data(data)
            for j_data in p_data: redis_list(f"{key}",j_data,symbol_list)
            logger.warnings("successfully fetched after purifyication")

        
#                           MAIN
#============================================================================
#============================================================================

if __name__ == "__main__":
    # Create threads for each server
    server_1 = 'localhost'
    server_2 = '10.1.35.167'
    thread1 = threading.Thread(target=receive_data, args=(server_1,5001))
    thread2 = threading.Thread(target=receive_data, args=(server_2,5008))
    
    # Create a thread for processing data
    processing_thread = threading.Thread(target=process_data)
    
    thread1.start()
    thread2.start()
    processing_thread.start()
    
    # Wait for the receiving threads to finish
    thread1.join()
    thread2.join()
    
    # Signal the processing thread to exit
    data_queue.put(None)  # Send a termination signal
    processing_thread.join()



