# build stage — vendored deps, no module downloads needed
FROM golang:1.26-alpine AS build
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -o /bin/app .

# run stage
FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -H app
USER app
COPY --from=build /bin/app /bin/app

ENV HTTP_ADDR=:8081
EXPOSE 8081
ENTRYPOINT ["/bin/app"]
