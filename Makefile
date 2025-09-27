DOCKER_ACCOUNT = cherts
APPNAME = pgscv
APPOS = linux
#APPOS = ${GOOS}

TAG_COMMIT := $(shell git rev-list --abbrev-commit --tags --max-count=1)
TAG := $(shell git describe --abbrev=0 --tags ${TAG_COMMIT} 2>/dev/null || true)
COMMIT := $(shell git rev-parse --short HEAD)
DATE := $(shell git log -1 --format=%cd --date=format:"%Y%m%d")
ifeq ($(TAG),)
	VERSION := 1.0
else
	#VERSION := $(TAG:v%=%)
	VERSION := $(TAG)
endif
ifneq ($(COMMIT), $(TAG_COMMIT))
    VERSION := $(VERSION)-next-$(DATE)
endif
ifeq ($(VERSION),)
    VERSION := $(COMMIT)-$(DATA)
endif
ifneq ($(shell git status --porcelain),)
    VERSION := $(VERSION)-dirty
endif
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

VERSION_BETA=1.0-$(BRANCH)-$(COMMIT)-$(DATE)-beta

LDFLAGS = -a -installsuffix cgo -ldflags "-X main.appName=${APPNAME} -X main.gitTag=${VERSION} -X main.gitCommit=${COMMIT} -X main.gitBranch=${BRANCH}"
MODERNIZE_CMD = go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.18.1

.PHONY: help \
		clean lint test race \
		build docker-build docker-push go-update \
		modernize modernize-fix modernize-check

.DEFAULT_GOAL := help

help: ## Display this help screen
	@echo "Makefile available targets:"
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  * \033[36m%-25s\033[0m %s\n", $$1, $$2}'

clean: ## Clean
	rm -f ./bin/${APPNAME} ./bin/${APPNAME}.tar.gz ./bin/${APPNAME}.version ./bin/${APPNAME}.sha256
	rm -rf ./bin

go-update: ## Update go mod
	go mod tidy -compat=1.24
	go get -u ./cmd
	go mod download
	go get -u ./cmd
	go mod download

dep: ## Get the dependencies
	go mod download

lint: ## Lint the source files
	go env -w GOFLAGS="-buildvcs=false"
	REVIVE_FORCE_COLOR=1 revive -formatter friendly ./...
	gosec -quiet ./...

test: dep lint ## Run tests
	go test -race -timeout 300s -coverprofile=.test_coverage.txt ./... && \
    	go tool cover -func=.test_coverage.txt | tail -n1 | awk '{print "Total test coverage: " $$3}'
	@rm .test_coverage.txt

race: dep ## Run data race detector
	go test -race -short -timeout 300s -p 1 ./...

build: dep ## Build
	mkdir -p ./bin
	CGO_ENABLED=0 GOOS=${APPOS} GOARCH=${GOARCH} go build ${LDFLAGS} -o bin/${APPNAME} ./cmd

docker-build: ## Build docker image
	docker build -t ${DOCKER_ACCOUNT}/${APPNAME}:${TAG} .
	docker image prune --force --filter label=stage=intermediate
	docker tag ${DOCKER_ACCOUNT}/${APPNAME}:${TAG} ${DOCKER_ACCOUNT}/${APPNAME}:latest

docker-push: ## Push docker image
	docker push ${DOCKER_ACCOUNT}/${APPNAME}:${TAG}
	docker push ${DOCKER_ACCOUNT}/${APPNAME}:latest

docker-build-beta: ## Build docker image (beta)
	docker build -t ${DOCKER_ACCOUNT}/${APPNAME}:v${VERSION_BETA} .
	docker image prune --force --filter label=stage=intermediate

docker-push-beta: ## Push docker image (beta)
	docker push ${DOCKER_ACCOUNT}/${APPNAME}:v${VERSION_BETA}

docker-build-test-runner: ## Build docker image with testing environment for CI
	$(eval VERSION := $(shell grep -E 'LABEL version' testing/docker-test-runner/Dockerfile |cut -d = -f2 |tr -d \"))
	cd ./testing/docker-test-runner; \
		docker build -t ${DOCKER_ACCOUNT}/pgscv-test-runner:${VERSION} .

docker-push-test-runner: ## Push testing docker image to registry
	$(eval VERSION := $(shell grep -E 'LABEL version' testing/docker-test-runner/Dockerfile |cut -d = -f2 |tr -d \"))
	cd ./testing/docker-test-runner; \
		docker push ${DOCKER_ACCOUNT}/pgscv-test-runner:${VERSION}

modernize: modernize-fix ## Run gopls modernize check and fix

modernize-fix: ## Run gopls modernize fix
	@echo "Running gopls modernize with -fix..."
	go env -w GOFLAGS="-buildvcs=false"
	$(MODERNIZE_CMD) -test -fix ./...

modernize-check: ## Run gopls modernize only check
	@echo "Checking if code needs modernization..."
	go env -w GOFLAGS="-buildvcs=false"
	$(MODERNIZE_CMD) -test ./...