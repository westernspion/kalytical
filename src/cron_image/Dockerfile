from alpine:3.14.0
ARG VERSION_TAG
RUN apk add jq curl yq
COPY enrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]