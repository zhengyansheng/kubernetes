.PHONY: build


build: 
	@printf "building...\n"
	@mkdir -p bin
	@/usr/local/go/bin/go build -o bin/scheduler-plugin  scheduler_plugins.go


docker-push:
	@docker build -t zhengyscn/sample-scheduler-framework:v1.2 .
	@docker push zhengyscn/sample-scheduler-framework:v1.2


clean:
	@rm -rf bin/*
