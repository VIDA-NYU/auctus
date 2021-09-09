# How to configure

We use [Jsonnet](https://jsonnet.org/) to automate the generation of the YAML config files for Kubernetes.

You should only need to update `deployment.jsonnet` and `secrets.jsonnet`, then you can generate the YAML files using:
```
mkdir yaml
jsonnet -S -m yaml/ deployment.jsonnet
jsonnet -S -m yaml/ secrets.jsonnet
```
