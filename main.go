package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/fiuskylab/kinesis-consumer/consumer"
	"github.com/fiuskylab/kinesis-consumer/internal"
	"go.uber.org/zap"
)

func main() {
	ctx := internal.NewContext()

	l, _ := zap.NewProduction()

	cfg := consumer.Config{
		StreamName:      aws.String(ctx.Env["AWS_KINESIS_STREAM_NAME"]),
		Region:          aws.String(ctx.Env["AWS_REGION"]),
		AccessKeyID:     ctx.Env["AWS_ACCESS_KEY_ID"],
		SecretAccessKey: ctx.Env["AWS_SECRET_ACCESS_KEY"],
		SessionToken:    ctx.Env["AWS_SESSION_TOKEN"],
	}

	c, err := consumer.NewConsumer(cfg, l)

	c.Listen()

	if err != nil {
		panic(err)
	}

	for {
		select {
		case msg := <-c.ChData:
			fmt.Println(msg)
		}
	}
}
