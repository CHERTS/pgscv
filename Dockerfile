# stage 1
# __release_tag__ 1.16 was released 2021-02-16
FROM golang:1.16 as build-stage
LABEL stage=intermediate
WORKDIR /app
COPY . .
RUN make build

# stage 2
# __release_tag__ 1.17.8 was released 2020-01-21
FROM nginx:1.17.8
RUN rm /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY ./extras/nginx.conf /etc/nginx/nginx.conf
COPY ./extras/agent.conf /etc/nginx/conf.d/agent.conf
COPY ./bin/install.sh /var/www/html/install.sh
COPY --from=build-stage /app/bin/pgscv.tar.gz /var/www/html/pgscv.tar.gz
COPY --from=build-stage /app/bin/pgscv.version /var/www/html/pgscv.version
COPY --from=build-stage /app/bin/pgscv.sha256 /var/www/html/pgscv.sha256
EXPOSE 1080
STOPSIGNAL SIGTERM
CMD ["nginx", "-g", "daemon off;"]
