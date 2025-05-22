import multiprocessing
import time
import pika
import json
import boto3

def worker_process(rabbitmq_params, queue_name, function):
    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=False)

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body)
            #print("Worker received message:", message)
            #print(".", end="")
            function(message)  # Aquí cridem la funció passada per paràmetre
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print("Error processing message:", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()


def primitive_stream_operation(function, maxfunc, queue_name):
    rabbitmq_params = pika.ConnectionParameters(host='54.209.134.51', credentials=pika.PlainCredentials('user', 'password123'))
    workers = []

    while True:
        connection = pika.BlockingConnection(rabbitmq_params)
        channel = connection.channel()
        queue = channel.queue_declare(queue=queue_name, durable=False, passive=True)
        message_count = queue.method.message_count
        connection.close()

        print(f"Missatges pendents a la cua: {message_count}, nWorkers: {len(workers)}")

        if message_count > 0 and message_count < 2000:
            desired_workers = 1
        else:
            desired_workers = min(maxfunc, int(message_count / 2000))   #Fem un worker per a cada 2000 missatges

        while len(workers) < desired_workers:
            p = multiprocessing.Process(target=worker_process, args=(rabbitmq_params, queue_name, function))
            p.start()
            workers.append(p)
            print(f"Worker creat. Total workers: {len(workers)}")

        while len(workers) > desired_workers:
            p = workers.pop()
            p.terminate()
            p.join()
            print(f"Worker aturat. Total workers: {len(workers)}")


        time.sleep(1)


# Exemples de funció processadora
def process_message_lambda_invoke(message):
    insults=["tonto", "burro", "rata"]

    text = message.get('text', '')

    for insult in insults:
        text = text.replace(insult, "CENSORED")

    #print(f"[InsultFilter] Texto filtrado: {text}")
    #print("_", end="")


def fill_queue(rabbitmq_params, queue_name):
    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=False)

    missatge = "Ets un tonto burro i rata"
    for _ in range(20000):
        message_json = json.dumps({'text': missatge})
        channel.basic_publish(exchange='', routing_key=queue_name, body=message_json)

    print(f"Missatge '{missatge}' enviat 20000 cops a la cua '{queue_name}'")

    connection.close()


if __name__ == "__main__":
    rabbitmq_params = pika.ConnectionParameters(host='54.209.134.51', credentials=pika.PlainCredentials('user', 'password123'))
    queue_name = 'text_queue_ex2'

    # Omplim la cua abans d'executar l'autoescalat
    fill_queue(rabbitmq_params, queue_name)

    # Ara passem la funció processadora al nostre sistema d’autoescalat
    primitive_stream_operation(process_message_lambda_invoke, maxfunc=10, queue_name='text_queue_ex2')