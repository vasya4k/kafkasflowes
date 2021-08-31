FROM alpine:3.14
COPY . /.
ENTRYPOINT ["./kafkasflowes"]