import subprocess
import time
import sys

def start_kafka():
    # Clear old processes
    subprocess.run("taskkill /F /IM java.exe /T", shell=True, capture_output=True)
    time.sleep(2)
    # Start Zookeeper
    zookeeper_cmd = "D:\\kafka_2.12-3.7.0\\bin\\windows\\zookeeper-server-start.bat D:\\kafka_2.12-3.7.0\\config\\zookeeper.properties"
    subprocess.Popen(["cmd", "/c", "start", "cmd", "/k", zookeeper_cmd], shell=True)
    time.sleep(10)  # Wait longer for Zookeeper
    # Start Kafka
    kafka_cmd = "D:\\kafka_2.12-3.7.0\\bin\\windows\\kafka-server-start.bat D:\\kafka_2.12-3.7.0\\config\\server.properties"
    subprocess.Popen(["cmd", "/c", "start", "cmd", "/k", kafka_cmd], shell=True)
    time.sleep(5)
    print("Kafka started")

def stop_kafka():
    subprocess.run("taskkill /F /IM java.exe /T", shell=True, capture_output=True)
    print("Kafka and Zookeeper stopped")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        stop_kafka()
    else:
        start_kafka()