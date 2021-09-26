package internal

import (
	"os"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

type Context struct {
	Logger *zap.Logger
	Env    map[string]string
}

func NewContext() *Context {
	if err := godotenv.Load(); err != nil {
		panic(err)
	}
	l, _ := zap.NewProduction()
	return &Context{
		Logger: l,
		Env: map[string]string{
			"AWS_REGION":              os.Getenv("AWS_REGION"),
			"AWS_KINESIS_STREAM_NAME": os.Getenv("AWS_KINESIS_STREAM_NAME"),
			"AWS_ENDPOINT":            os.Getenv("AWS_ENDPOINT"),
			"AWS_ACCESS_KEY_ID":       os.Getenv("AWS_ACCESS_KEY_ID"),
			"AWS_SECRET_ACCESS_KEY":   os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"AWS_SESSION_TOKEN":       os.Getenv("AWS_SESSION_TOKEN"),
		},
	}
}
