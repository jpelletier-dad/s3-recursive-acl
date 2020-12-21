package main

import (
	"flag"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	var bucket, delim, root_prefix string
	//var maxKeys int64

	// TODO create a worker group instead of waitgroup (prevent dead stop from single request timing out)
	//var wg sync.WaitGroup
	//var counter int64

	// TODO these should be outside
	//flag.StringVar(&region, "region", "us-east-1", "AWS region")
	flag.StringVar(&bucket, "bucket", "s3-bucket", "Bucket name")
	flag.StringVar(&delim, "delimiter", "/", "Delim")
	flag.StringVar(&root_prefix, "root_prefix", "", "Root prefix")
	flag.Parse()

	// TODO I believe regions are auto-parsed from config?
	//svc := s3.New(session.New(), &aws.Config{
	//	Region: aws.String(region),
	//})

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := s3.New(sess)

	var latestObj *s3.Object

	latestObj, _ = listObjectsPrefix(svc, bucket, delim, root_prefix)

	fmt.Println(fmt.Sprintf("BUCKET %s", bucket))
	fmt.Println(fmt.Sprintf("KEY %s", *latestObj.Key))
	fmt.Println(fmt.Sprintf("MODIFIED %s", *latestObj.LastModified))
	//fmt.Println(fmt.Sprintf("Successfully updated permissions on %d objects", counter))
}

func listObjectsPrefix(svc *s3.S3, bucket string, delim string, prefix string) (oldestObject *s3.Object, err error) {
	var latestObj *s3.Object

	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String(delim),
		Prefix:    aws.String(prefix),
		// TODO func (c *S3) ListObjectsV2PagesWithContext(ctx aws.Context, input *ListObjectsV2Input, fn func(*ListObjectsV2Output, bool) bool, opts ...request.Option) error
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {

		//fmt.Println(fmt.Sprintf("Starting '%s'", *page.Contents[0].Key))
		for _, object := range page.Contents {
			int64_zero := new(int64)
			if object.Size == int64_zero {
				continue
			}

			if latestObj == nil || object.LastModified.After(*latestObj.LastModified) {
				latestObj = object
			}
			//fmt.Println(fmt.Sprintf("%s", *object.LastModified))
			//fmt.Println(fmt.Sprintf("%s", *object.Key))
		}

		for _, cp := range page.CommonPrefixes {
			latestObj2, err := listObjectsPrefix(svc, bucket, delim, *cp.Prefix)
			if err != nil {
				fmt.Println(fmt.Sprintf("ERROR '%v'", err))
				panic(fmt.Sprintf("Failed to check object permissions in '%s', %v", bucket, err))
				// TODO here?
				return true
			}
			// TODO lock
			if latestObj == nil || latestObj2.LastModified.After(*latestObj.LastModified) {
				latestObj = latestObj2
			}
		}

		//fmt.Println(fmt.Sprintf("%d", counter))
		return !lastPage
	})

	return latestObj, err
}
