from kafka import KafkaProducer
from django.shortcuts import render
from django.http import JsonResponse
import json

def sendToKafka(request):
    if request.method == 'POST':
        data = request.POST.info
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        producer.send('sparkInfo', json.dumps({"info":data}))
    else:
        render(HttpResponse('<h1> Please send a POST request </h1>') 