package main

import (
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	var bucket, region, path, cannedACL, startAfter string
	var maxKeys int64

	var wg sync.WaitGroup
	var counter int64

	flag.StringVar(&region, "region", "ap-northeast-1", "AWS region")
	flag.StringVar(&bucket, "bucket", "s3-bucket", "Bucket name")
	flag.StringVar(&path, "path", "/", "Path to recurse under")
	flag.StringVar(&cannedACL, "acl", "public-read", "Canned ACL to assign objects")
	flag.Int64Var(&maxKeys, "max-keys", 1000, "Maximum keys per page")
	flag.StringVar(&startAfter, "start-after", "", "Key to start after")
	flag.Parse()

	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String(region),
	})

	err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Prefix:     aws.String(path),
		Bucket:     aws.String(bucket),
		MaxKeys:    &maxKeys,
		StartAfter: aws.String(startAfter),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		fmt.Println(fmt.Sprintf("Done '%d'", counter))
		for _, object := range page.Contents {
			key := *object.Key
			counter++
			go func(bucket string, key string, cannedACL string) {
				wg.Add(1)
				_, err := svc.PutObjectAcl(&s3.PutObjectAclInput{
					ACL:    aws.String(cannedACL),
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				fmt.Println(fmt.Sprintf("Updating '%s'", key))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to change permissions on '%s', %v", key, err)
				}
				defer wg.Done()
			}(bucket, key, cannedACL)
		}
		//wg.Wait()
		return true
	})

	wg.Wait()

	if err != nil {
		panic(fmt.Sprintf("Failed to update object permissions in '%s', %v", bucket, err))
	}

	fmt.Println(fmt.Sprintf("Successfully updated permissions on %d objects", counter))
}
