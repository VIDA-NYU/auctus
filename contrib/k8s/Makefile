.PHONY: all clean generate

all: clean generate

clean:
	rm -rf yaml

generate: *.jsonnet *.libsonnet discovery/*.libsonnet
	mkdir -p yaml
	jsonnet -S -m yaml deployment.jsonnet
	jsonnet -S -m yaml secrets.jsonnet
