package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "getlogstream",
		Usage: "stream out all events from a log stream",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Required: true,
				Name:     "bucket",
				Usage:    "the bucket with the object",
				Aliases:  []string{"b"},
				EnvVars:  []string{"S3S_BUCKET"},
			},
			&cli.StringFlag{
				Required: true,
				Name:     "key",
				Usage:    "the key (path and filename) of the object",
				Aliases:  []string{"k"},
				EnvVars:  []string{"S3S_OBJECT_KEY"},
			},
			&cli.StringFlag{
				Name:    "sql",
				Usage:   "the query to run",
				Value:   "SELECT * FROM S3Object",
				EnvVars: []string{"S3S_QUERY"},
			},
			&cli.StringFlag{
				Name:    "format",
				Usage:   "the format used in object [auto|csv|json|parquet]",
				Aliases: []string{"f"},
				Value:   "auto",
				EnvVars: []string{"S3S_FORMAT"},
			},
			&cli.StringFlag{
				Name:    "compression",
				Usage:   "the compression used in object [auto|none|gzip|bzip2]",
				Aliases: []string{"c"},
				Value:   "auto",
				EnvVars: []string{"S3S_COMPRESSION"},
			},
			&cli.StringFlag{
				Name:    "region",
				Usage:   "the aws region",
				Aliases: []string{"r"},
				Value:   "us-east-1",
				EnvVars: []string{"AWS_REGION", "AWS_DEFAULT_REGION", "S3S_AWS_REGION"},
			},
			&cli.IntFlag{
				Name:    "retry",
				Usage:   "how many times to retry on failure",
				Value:   0,
				EnvVars: []string{"S3S_RETRY_COUNT"},
			},
			&cli.StringFlag{
				Name:    "endpoint",
				Usage:   "the endpoint forthe s3 service",
				EnvVars: []string{"S3S_ENDPOINT"},
			},
			&cli.StringFlag{
				Name:    "endpoint-access-key",
				Usage:   "the endpoint access_key for the s3 service",
				EnvVars: []string{"S3S_ENDPOINT_ACCESS_KEY"},
			},
			&cli.StringFlag{
				Name:    "endpoint-secret",
				Usage:   "the endpoint secret for the s3 service",
				EnvVars: []string{"S3S_ENDPOINT_SECRET"},
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(c *cli.Context) error {
	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()

	// make a logger
	l := log.New(os.Stderr, "WARN", 0)

	schan := make(chan os.Signal, 1)
	signal.Notify(schan, os.Signal(syscall.SIGPIPE))

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := getAWSConfig(c)
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	var output *s3.SelectObjectContentOutput

	// Get the first page of results for ListObjectsV2 for a bucket
	err = retry(c.Int("retry"), func() error {
		var err error
		output, err = client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:              aws.String(c.String("bucket")),
			Key:                 aws.String(c.String("key")),
			Expression:          aws.String(c.String("sql")),
			ExpressionType:      types.ExpressionTypeSql,
			InputSerialization:  getInputSerialization(c, l),
			OutputSerialization: &types.OutputSerialization{JSON: &types.JSONOutput{}},
		})
		return err
	})
	if err != nil {
		return err
	}

	reader := output.GetStream().Reader
	defer reader.Close()

	for {
		select {
		case <-schan:
			return reader.Err()
		case evt := <-reader.Events():
			switch v := evt.(type) {
			case *types.SelectObjectContentEventStreamMemberCont:
				_ = v.Value // Value is types.ContinuationEvent

			case *types.SelectObjectContentEventStreamMemberEnd:
				_ = v.Value // Value is types.EndEvent
				return nil

			case *types.SelectObjectContentEventStreamMemberProgress:
				_ = v.Value // Value is types.ProgressEvent

			case *types.SelectObjectContentEventStreamMemberRecords:
				_ = v.Value // Value is types.RecordsEvent
				fmt.Println(string(bytes.Trim(v.Value.Payload, "\n")))

			case *types.SelectObjectContentEventStreamMemberStats:
				_ = v.Value // Value is types.StatsEvent

			case *types.UnknownUnionMember:
				l.Println("unknown tag:", v.Tag)
			}
		}
	}
}

func getAWSConfig(c *cli.Context) (aws.Config, error) {
	ctx := c.Context
	endpoint := c.String("endpoint")
	accessKey := c.String("endpoint-access-key")
	secret := c.String("endpoint-secret")

	options := [](func(*config.LoadOptions) error){
		config.WithRegion(c.String("region")),
	}

	// see if we are using a custom endpoint
	if len(endpoint) > 0 {
		staticResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               endpoint, // or where ever you ran minio
				SigningRegion:     region,
				HostnameImmutable: true,
			}, nil
		})
		options = append(options, config.WithEndpointResolver(staticResolver))

		if len(accessKey) > 0 {
			credsProvder := credentials.NewStaticCredentialsProvider(accessKey, secret, "")
			options = append(options, config.WithCredentialsProvider(credsProvder))
		}
	}

	return config.LoadDefaultConfig(ctx, options...)
}

func retry(times int, try func() error) error {
	var err error
	tried := 0
	for times >= tried {
		if err = try(); err == nil {
			return nil
		}
		tried++
	}
	return err
}

func getInputSerialization(c *cli.Context, l *log.Logger) *types.InputSerialization {
	ser := &types.InputSerialization{
		CompressionType: getCompressionType(c, l),
	}

	flag := strings.ToLower(c.String("format"))
	if flag == "auto" {
		// parse the file extension and set based on that
		flg := filepath.Ext(c.String("key"))
		flag = strings.TrimLeft(strings.ToLower(flg), ".")
		if flag == "gz" || flag == "gzip" || flag == "bz2" || flag == "bzip2" {
			flag = strings.TrimLeft(strings.ToLower(filepath.Ext(strings.TrimRight(c.String("key"), flg))), ".")
		}
	}

	switch flag {
	case "csv":
		ser.CSV = &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse}
	case "json":
		ser.JSON = &types.JSONInput{Type: types.JSONTypeLines}
	case "parquet":
		ser.Parquet = &types.ParquetInput{}
	default:
		// default to json
		l.Println("uknown file format, default is JSON")
		ser.JSON = &types.JSONInput{Type: types.JSONTypeLines}
	}

	return ser
}

func getCompressionType(c *cli.Context, l *log.Logger) types.CompressionType {
	flag := strings.ToLower(c.String("compression"))
	if flag == "auto" {
		// parse from file e
		flag = strings.ToLower(filepath.Ext(c.String("key")))
	}

	switch strings.TrimLeft(flag, ".") {
	case "bz2":
		fallthrough
	case "bzip2":
		return types.CompressionTypeBzip2
	case "gz":
		fallthrough
	case "gzip":
		return types.CompressionTypeGzip
	case "none":
		fallthrough
	default:
		return types.CompressionTypeNone
	}
}
