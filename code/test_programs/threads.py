import threading
import time

def handle_client_connection(service):
    print("Handling client connection for service:", service)
    time.sleep(2)  # Simulate some work being done
    print("Finished handling client connection.")


# Start threads for each link
threads = []

service_1 = "Service 1"
service_2 = "Service 2"
service_3 = "Service 3"
# Using `args` to pass positional arguments and `kwargs` for keyword arguments
t = threading.Thread(target=handle_client_connection, args=(service_1,))
threads.append(t)
t = threading.Thread(target=handle_client_connection, args=(service_2,))
threads.append(t)
t = threading.Thread(target=handle_client_connection, args=(service_3,))
threads.append(t)

print("All threads have been created. Starting threads now...")

# Start each thread
for t in threads:
    t.start()

# Wait for all threads to finish
for t in threads:
    t.join()

print("All client connections have been handled.")