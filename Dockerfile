FROM golang:1.11.5-stretch AS builder

ENV SPHINX_VERSION 3.1.1-612d99f
RUN wget https://sphinxsearch.com/files/sphinx-${SPHINX_VERSION}-linux-amd64.tar.gz -O /tmp/sphinxsearch.tar.gz \
    && mkdir -pv /opt/sphinx && cd /opt/sphinx && tar -xf /tmp/sphinxsearch.tar.gz \
    && rm /tmp/sphinxsearch.tar.gz

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

RUN curl -fsSL -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64 && chmod +x /usr/local/bin/dep
RUN mkdir -p /go/src/github.com/superjobru/go-mysql-sphinx
WORKDIR /go/src/github.com/superjobru/go-mysql-sphinx
COPY Gopkg.lock Gopkg.toml ./
RUN dep ensure -vendor-only
COPY . .
RUN make

FROM debian:stretch-slim
RUN apt-get update && apt-get install -y default-libmysqlclient-dev rsync
COPY --from=builder /go/src/github.com/superjobru/go-mysql-sphinx/bin/go-mysql-sphinx /usr/local/bin/
COPY --from=builder /usr/local/bin/dockerize /usr/local/bin/
COPY --from=builder /opt/sphinx/sphinx-3.1.1/bin/indexer /usr/local/bin/
COPY etc/river.toml /etc/go-mysql-sphinx/river.toml
COPY etc/dict /etc/go-mysql-sphinx/dict
RUN mkdir -p /var/river
VOLUME /var/river

CMD dockerize -wait tcp://mysql:3306 -timeout 120s /usr/local/bin/go-mysql-sphinx -config /etc/go-mysql-sphinx/river.toml -data-dir /var/river -my-addr mysql:3306 -sph-addr sphinx:9308
