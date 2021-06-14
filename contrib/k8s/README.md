# How to configure

We use [Jsonnet](https://jsonnet.org/) to automate the generation of the YAML config files for Kubernetes.

You should only need to update `deployment.jsonnet` and `secrets.jsonnet`, then you can generate the YAML files using:
```
mkdir yaml
jsonnet -S -m yaml/ deployment.jsonnet
jsonnet -S -m yaml/ secrets.jsonnet
```

# Test deployment on minikube

Start minikube:
```
minikube start --memory 4096 --kubernetes-version=v1.14.10
```

Set up the secrets:
```
kubectl apply -f yaml/secrets.yml
```

Set up volumes:
```
kubectl apply -f yaml/volumes.yml
```

Set up the services:
```
kubectl apply -f yaml/elasticsearch.yml
kubectl apply -f yaml/rabbitmq.yml
kubectl apply -f yaml/redis.yml
```

Build images locally and load them up in minikube:
```
(cd ../.. && docker-compose build --build-arg version=$(git describe) && docker-compose pull)
../../scripts/minikube-load-images.sh
```

Get the data:
```
kubectl apply -f yaml/get-data.yml
kubectl get job -w  # Ctrl-C when done
kubectl delete -f yaml/get-data.yml
```

Set up the application:
```
kubectl apply -f yaml/auctus.yml
```

You should be able to see Datamart at [`http://192.168.99.100:30080/`](http://192.168.99.100:30080)
