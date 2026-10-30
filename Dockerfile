# The engine (cmd/engine). pg_query_go embeds Postgres's parser, so the
# build needs cgo; the runtime image has glibc for it.
#   docker build -t localsync-engine .
#   docker run -e LOCALSYNC_JWT_SECRET=... -v ./rules.yaml:/rules.yaml \
#     localsync-engine serve --pg postgresql://... --rules /rules.yaml
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=1 go build -trimpath -ldflags='-s -w' -o /engine ./cmd/engine

FROM gcr.io/distroless/cc-debian12
COPY --from=build /engine /engine
EXPOSE 3140
ENTRYPOINT ["/engine"]
CMD ["serve", "--rules", "/rules.yaml"]
