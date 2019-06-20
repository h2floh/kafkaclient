docker build -t h2floh.acurecr.io/kafka/consumer kafka-consumer/.
docker build -t h2floh.acurecr.io/kafka/producer kafka-producer/.
docker push h2floh.acurecr.io/kafka/consumer h2floh.acurecr.io/kafka/producer