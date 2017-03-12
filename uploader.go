package main

import (
	"io"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func upload(uploader *s3manager.Uploader, bucket string, key string, file io.Reader) error {
	upParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   file,
	}

	_, err := uploader.Upload(upParams)
	if (err != nil) {
		return err
	}

	return nil
}
