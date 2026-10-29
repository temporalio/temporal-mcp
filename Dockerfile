FROM --platform=$BUILDPLATFORM golang:1.27.2 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Stamps the version as goreleaser does; VERSION is the release tag without its v, such as 0.0.3.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-s -w -X main.version=$VERSION" -o /temporal-mcp .

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /temporal-mcp /temporal-mcp
ENTRYPOINT ["/temporal-mcp"]
