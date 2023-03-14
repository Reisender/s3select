# s3select cli utility

This repo is a basic utility to query S3 files using S3 Select.

### installation

To install it you can run

```bash
go install github.com/Reisender/s3select/cmd/s3s@latest
```

Assuming your `go/bin` dir is in your path, you can just run it like so

```bash
s3s --help
```

## troubleshooting

If you are getting an error trying to install the latest version, you can try skipping the go proxy like this.

```bash
export GOPRIVATE=github.com/Reisender/s3select
go install github.com/Reisender/s3select/cmd/s3s@latest
```
