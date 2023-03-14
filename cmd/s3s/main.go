package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
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
			},
			&cli.StringFlag{
				Required: true,
				Name:     "key",
				Usage:    "the key (path and filename) of the object",
				Aliases:  []string{"k"},
			},
			&cli.StringFlag{
				Name:  "sql",
				Usage: "the query to run",
				Value: "SELECT * FROM S3Object",
			},
			&cli.StringFlag{
				Name:    "region",
				Usage:   "the aws region",
				Aliases: []string{"r"},
				Value:   "us-east-1",
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

	schan := make(chan os.Signal, 1)
	signal.Notify(schan, os.Signal(syscall.SIGPIPE))

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(c.String("region")))
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	var output *s3.SelectObjectContentOutput

	// Get the first page of results for ListObjectsV2 for a bucket
	err = retry(1, func() error {
		var err error
		output, err = client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:         aws.String(c.String("bucket")),
			Key:            aws.String(c.String("key")),
			Expression:     aws.String(c.String("sql")),
			ExpressionType: types.ExpressionTypeSql,
			InputSerialization: &types.InputSerialization{
				CompressionType: types.CompressionTypeGzip,
				JSON:            &types.JSONInput{Type: types.JSONTypeLines},
			},
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
				fmt.Println("unknown tag:", v.Tag)
			}
		}
	}
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
