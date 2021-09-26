package consumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"go.uber.org/zap"
)

type Consumer struct {
	l             *zap.Logger
	cfg           Config
	session       *session.Session
	kinesisClient *kinesis.Kinesis
	streams       *kinesis.DescribeStreamOutput
	shards        []*kinesis.Shard
	ShardIterator []*ShardIterator
	ChData        chan string
}

type ShardIterator struct {
	ID       string
	Iterator *kinesis.GetShardIteratorOutput
	Position *string
}

type Config struct {
	StreamName      *string
	Region          *string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

func NewConsumer(cfg Config, l *zap.Logger) (*Consumer, error) {
	c := Consumer{
		l:      l,
		cfg:    cfg,
		ChData: make(chan string, 20),
	}

	if err := c.
		setSession().
		setKinesis().
		setStreams(); err != nil {
		return &c, err
	}

	if err := c.
		setShards().
		setIterators(); err != nil {
		return &c, err
	}

	return &c, nil
}

func (c *Consumer) setSession() *Consumer {
	c.session = session.New(&aws.Config{
		Region:      c.cfg.Region,
		Endpoint:    aws.String(string("kinesis." + *c.cfg.Region + ".amazonaws.com")),
		Credentials: credentials.NewStaticCredentials(c.cfg.AccessKeyID, c.cfg.SecretAccessKey, c.cfg.SessionToken),
	})

	return c
}

func (c *Consumer) setKinesis() *Consumer {
	c.kinesisClient = kinesis.New(c.session)
	return c
}

func (c *Consumer) setStreams() error {
	streams, err := c.kinesisClient.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: c.cfg.StreamName,
	})

	if err != nil {
		c.l.Error(err.Error())
	}

	c.streams = streams

	return err
}

func (c *Consumer) setShards() *Consumer {
	c.shards = c.streams.StreamDescription.Shards
	return c
}

func (c *Consumer) setIterators() error {
	for _, s := range c.shards {
		output, err := c.kinesisClient.GetShardIterator(&kinesis.GetShardIteratorInput{
			ShardId:           s.ShardId,
			ShardIteratorType: aws.String("TRIM_HORIZON"),
			StreamName:        c.cfg.StreamName,
		})
		if err != nil {
			c.l.Error(err.Error())
			return err
		}
		c.ShardIterator = append(c.ShardIterator, &ShardIterator{
			ID:       *s.ShardId,
			Iterator: output,
			Position: output.ShardIterator,
		})
	}
	return nil
}

func (c *Consumer) Listen() {
	for _, s := range c.ShardIterator {
		go c.readRecords(s)
	}
}

func (c *Consumer) readRecords(s *ShardIterator) {
	var a *string
	for {
		records, err := c.kinesisClient.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: s.Position,
		})

		if err != nil {
			c.l.Error(err.Error())
			continue
		}

		if len(records.Records) > 0 {
			for _, r := range records.Records {
				c.ChData <- string(r.Data)
			}
		} else if records.NextShardIterator == a || s.Position == records.NextShardIterator {
			c.l.Error(string("GetRecords Error: "))
			break
		}
		s.Position = records.NextShardIterator
	}
}
