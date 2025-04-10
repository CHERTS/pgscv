# cherts/pgscv-test-runner
# __release_tag__ postrges v16.8 was released 2025-02-20
# __release_tag__ golang v1.23.6 was released 2025-02-04
# __release_tag__ revive v1.7.0 was released 2025-02-16
# __release_tag__ gosec v2.22.1 was released 2025-02-13
FROM postgres:16.8

LABEL version="1.0.8"

# install dependencies
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y vim make gcc git curl pgbouncer && \
    curl -s -L https://go.dev/dl/go1.23.6.linux-amd64.tar.gz -o - | tar xzf - -C /usr/local && \
    export PATH=$PATH:/usr/local/go/bin && \
    curl -s -L https://github.com/mgechev/revive/releases/download/v1.7.0/revive_linux_amd64.tar.gz | tar xzf - -C $(go env GOROOT)/bin revive && \
    curl -sfL https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s -- -b $(go env GOROOT)/bin v2.22.1 && \
    mkdir /opt/testing/ && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="${PATH}:/usr/local/bin:/usr/local/go/bin"

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/bin/
COPY fixtures.sql /opt/testing/

CMD ["echo", "I'm pgscv test runner 1.0.7"]
