# cherts/pgscv-test-runner
# __release_tag__ postrges v17.6 was released 2025-08-14
# __release_tag__ golang v1.24.5 was released 2025-07-08
# __release_tag__ revive v1.10.0 was released 2025-05-28
# __release_tag__ gosec v2.22.5 was released 2025-06-16
FROM postgres:17.6

LABEL version="1.0.10"

# install dependencies
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y vim make gcc git curl pgbouncer memcached && \
    curl -s -L https://go.dev/dl/go1.24.5.linux-amd64.tar.gz -o - | tar xzf - -C /usr/local && \
    export PATH=$PATH:/usr/local/go/bin && \
    curl -s -L https://github.com/mgechev/revive/releases/download/v1.10.0/revive_linux_amd64.tar.gz | tar xzf - -C $(go env GOROOT)/bin revive && \
    curl -sfL https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s -- -b $(go env GOROOT)/bin v2.22.5 && \
    mkdir /opt/testing/ && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="${PATH}:/usr/local/bin:/usr/local/go/bin"

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/bin/
COPY fixtures.sql /opt/testing/

CMD ["echo", "I'm pgscv test runner 1.0.10"]
