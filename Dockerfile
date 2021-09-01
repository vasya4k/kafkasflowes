FROM alpine:3.14
COPY ./kafkasflowes /.
COPY ./ksflowes.toml /.
ENTRYPOINT ["./kafkasflowes"]