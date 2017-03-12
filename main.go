package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"github.com/go-ini/ini"
	flag "github.com/ogier/pflag"
	"log"
	"os"
	"strconv"
)

type Config struct {
	S3_KEY_ID        string
	S3_SECRET        string
	BUCKET           string
	OBJECT_KEY       string
	HOST             string
	CONCURRENCY      int
	CHUNK_SIZE       int64
	MAX_RETRIES      int
	S3_STORAGE_CLASS string
	SNAPSHOT_PREFIX  string
	FILESYSTEM       string
	QUIET            bool
	PROGRESS         bool
	ESTIMATED        int
}

const MINIMUM_CHUNK_SIZE int64 = 5
const DEFAULT_CHUNK_SIZE int64 = 256
const DEFAULT_CONCURRENCY int = 64
const DEFAULT_MAX_RETRIES int = 3
const DEFAULT_S3_STORAGE_CLASS string = "STANDARD_IA"
const DEFAULT_HOST string = "s3.amazonaws.com"
const DEFAULT_QUIET bool = true
const DEFAULT_PROGRESS bool = false

func main() {
	var cfg = ReadConfig()
	client := GetUploader(cfg)

	err := upload(client, cfg.BUCKET, cfg.OBJECT_KEY, bufio.NewReader(os.Stdin))

	if err != nil {
		log.Fatal("Failed to Upload: ", err)
	}
}

func ReadConfig() *Config {
	cfg, err := ini.Load("z3.conf")
	cfg.BlockMode = false

	if err != nil {
		log.Fatal("Unable to load config file")
	}

	//default config
	CHUNK_SIZE := fmt.Sprintf("%dMB", DEFAULT_CHUNK_SIZE)
	conf := &Config{
		HOST:             DEFAULT_HOST,
		CONCURRENCY:      DEFAULT_CONCURRENCY,
		S3_STORAGE_CLASS: DEFAULT_S3_STORAGE_CLASS,
		MAX_RETRIES:      DEFAULT_MAX_RETRIES,
		CHUNK_SIZE:       DEFAULT_CHUNK_SIZE,
		QUIET:            DEFAULT_QUIET,
		PROGRESS:         DEFAULT_PROGRESS,
	}

	flag.StringVarP(&CHUNK_SIZE, "chunk-size", "s", CHUNK_SIZE, "multipart chunk size, eg: 10M, 1G")
	flag.IntVar(&conf.ESTIMATED, "estimated", 0, "Estimated upload size")
	flag.IntVar(&conf.CONCURRENCY, "concurrency", conf.CONCURRENCY, "number of worker threads to use")
	flag.StringVar(&conf.S3_STORAGE_CLASS, "storage-class", conf.S3_STORAGE_CLASS, "The S3 storage class")
	flag.BoolVar(&conf.PROGRESS, "progress", conf.PROGRESS, "show progress report")
	flag.BoolVar(&conf.QUIET, "quiet", conf.QUIET, "don't emit any output at all")

	err = cfg.Section("main").MapTo(conf)

	if err != nil {
		log.Fatal("Can't map config: ", err)
	}

	CHUNK_SIZE = cfg.Section("main").Key("CHUNK_SIZE").String()
	flag.Parse()

	//left over positional argument should be the S3 key to upload to
	switch argCount := flag.NArg(); argCount {
	case 1:
		conf.OBJECT_KEY = flag.Arg(0)
	case 0:
		fmt.Println("S3 Key to use not provided")
		flag.PrintDefaults()
		os.Exit(1)
	default:
		fmt.Println("unknown options:", flag.Args())
		flag.PrintDefaults()
		os.Exit(1)
	}

	if conf.PROGRESS && conf.QUIET {
		conf.QUIET = false
	}

	if conf.ESTIMATED > 0 {
		conf.CHUNK_SIZE = 0
	} else {
		conf.CHUNK_SIZE = ParseChunkSize(CHUNK_SIZE)
	}

	return conf
}

func ParseChunkSize(size string) int64 {
	var bytes int64 = DEFAULT_CHUNK_SIZE * 1024 * 1024
	var minimum_bytes int64 = MINIMUM_CHUNK_SIZE * 1024 * 1024

	if _, err := strconv.Atoi(size); err == nil {
		//if no unit supplied, assume MB
		size = fmt.Sprintf("%dMB", size)
	}

	if size, err := humanize.ParseBytes(size); err == nil {
		bytes = int64(size)
	}

	if bytes < minimum_bytes {
		bytes = minimum_bytes
	}
	return bytes
}

func GetUploader(cfg *Config) *s3manager.Uploader {
	S3Config := &aws.Config{
		Region:      aws.String("ap-southeast-2"),
		Endpoint:    aws.String(cfg.HOST),
		Credentials: credentials.NewStaticCredentials(cfg.S3_KEY_ID, cfg.S3_SECRET, ""),
	}

	S3Service := s3.New(session.New(S3Config))

	uploader := s3manager.NewUploaderWithClient(S3Service, func(u *s3manager.Uploader) {
		u.PartSize = cfg.CHUNK_SIZE
		u.Concurrency = cfg.CONCURRENCY
	})
	return uploader
}
