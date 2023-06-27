## Kafka on Kubernetes via rancher-desktop
 1. Download Rancher desktop https://github.com/rancher-sandbox/rancher-desktop/releases

 2. Switch context to rancher-desktop.
```bash
kubectx rancher-desktop
 ```

 3. Create kafka namespace and change to that namespace
 ```bash
 kubectl create namespace kafka
 kubens kafka
 ```

 4. Setup the Strimzi kafka operator:
 ```bash
 kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka'
 ```

 5. Apply kafka.yaml (in this repo) with kubectl to create the cluste:
 ```bash
 kubectl apply -f kafka.yaml
 ```

 6. Check if things are running
 ```bash
 kubectl get kafkas
 kubectl get pods
 ```

 7. get nodeport that is forwarded to local-host to connect outside k8s
 ```bash
 kubectl get service my-cluster-kafka-server-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}'
 ```
 Use `localhost:<nodePort>` to connect to Kafka.
