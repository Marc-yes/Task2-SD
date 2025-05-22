import json

insults=["tonto", "burro", "rata"]

def lambda_handler(event, context):
    order = event.get('order')
    print (f"Orden recibida: {order}")

    for insult in insults:
        order = order.replace(insult, "CENSORED")
    
    print(f"[InsultFilter] Texto filtrado: {order}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Order delivered",
            "order": order
        })
    }
