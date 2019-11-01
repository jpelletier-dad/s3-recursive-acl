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
	var bucket, region, path, startAfter string
	var maxKeys int64

	// TODO create a worker group instead of waitgroup (prevent dead stop from single request timing out)
	var wg sync.WaitGroup
	var counter int64

	// TODO these should be outside
	flag.StringVar(&region, "region", "ap-northeast-1", "AWS region")
	flag.StringVar(&bucket, "bucket", "s3-bucket", "Bucket name")
	flag.StringVar(&path, "path", "/", "Path to recurse under")
	flag.Int64Var(&maxKeys, "max-keys", 1000, "Maximum keys per page")
	flag.StringVar(&startAfter, "start-after", "", "Key to start after")
	flag.Parse()

	// TODO I believe regions are auto-parsed from config?
	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String(region),
	})

	err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Prefix:     aws.String(path),
		Bucket:     aws.String(bucket),
		MaxKeys:    &maxKeys,
		StartAfter: aws.String(startAfter),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		fmt.Println(fmt.Sprintf("Starting '%s'", *page.Contents[0].Key))
		for _, object := range page.Contents {
			counter++
			key := *object.Key
			// TODO pull this into a separate function
			go func(bucket string, key string) {
				wg.Add(1)
				result, err := svc.GetObjectAcl(&s3.GetObjectAclInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed retrieve permissions on '%s', %v", key, err)
				}
				// TODO make this repair as well?
				if !checkACL(result) {
					fmt.Println(fmt.Sprintf("Failed '%s'", key))
				}
				defer wg.Done()
			}(bucket, key)
		}
		wg.Wait()
		fmt.Println(fmt.Sprintf("%d", counter))
		return true
	})

	wg.Wait()

	if err != nil {
		panic(fmt.Sprintf("Failed to check object permissions in '%s', %v", bucket, err))
	}

	fmt.Println(fmt.Sprintf("Successfully updated permissions on %d objects", counter))
}

func checkACL(output *s3.GetObjectAclOutput) bool {
	for _, grant := range output.Grants {
		if *grant.Grantee.DisplayName != "aws_nodes" {
			continue
		}
		return *grant.Permission == "FULL_CONTROL"
	}
	return false
}
