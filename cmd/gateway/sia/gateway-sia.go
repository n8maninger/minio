/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sia

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/sha256-simd"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/node/api/client"
)

const (
	siaBackend = "sia"
)

type siaObjects struct {
	minio.GatewayUnsupported
	Address string // Address and port of Sia Daemon.
	RootDir string // Root directory to store files on Sia.
	client  *client.Client
}

// closeResponse close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
func closeResponse(resp *http.Response) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if resp != nil && resp.Body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}

func init() {
	const siaGatewayTemplate = `NAME:
   {{.HelpName}} - {{.Usage}}
 
 USAGE:
   {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [SIA_DAEMON_ADDR]
 {{if .VisibleFlags}}
 FLAGS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
 ENVIRONMENT VARIABLES: (Default values in parenthesis)
   ACCESS:
	  MINIO_ACCESS_KEY: Custom access key (Do not reuse same access keys on all instances)
	  MINIO_SECRET_KEY: Custom secret key (Do not reuse same secret keys on all instances)
 
   BROWSER:
	  MINIO_BROWSER: To disable web browser access, set this value to "off".
 
   DOMAIN:
	  MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.
 
   CACHE:
	  MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
	  MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
	  MINIO_CACHE_EXPIRY: Cache expiry duration in days.
	  MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).
 
   SIA_TEMP_DIR:        The name of the local Sia temporary storage directory. (.sia_temp)
   SIA_API_PASSWORD:    API password for Sia daemon. (default is empty)
 
 EXAMPLES:
   1. Start minio gateway server for Sia backend.
	  $ {{.HelpName}}
 
   2. Start minio gateway server for Sia backend with edge caching enabled.
	  $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
	  $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
	  $ export MINIO_CACHE_EXPIRY=40
	  $ export MINIO_CACHE_MAXUSE=80
	  $ {{.HelpName}}
 `

	minio.RegisterGatewayCommand(cli.Command{
		Name:               siaBackend,
		Usage:              "Sia Decentralized Cloud",
		Action:             siaGatewayMain,
		CustomHelpTemplate: siaGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway sia' command line.
func siaGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	host := ctx.Args().First()
	// Validate gateway arguments.
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &Sia{host})
}

// Sia implements Gateway.
type Sia struct {
	host string // Sia daemon host address
}

// Name implements Gateway interface.
func (g *Sia) Name() string {
	return siaBackend
}

// NewGatewayLayer returns Sia gateway layer, implements ObjectLayer interface to
// talk to Sia backend.
func (g *Sia) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	addr := g.host

	if addr == "" {
		addr = "127.0.0.1:9980"
	}

	sia := &siaObjects{
		Address: g.host,
		// RootDir uses access key directly, provides partitioning for
		// concurrent users talking to same sia daemon.
		RootDir: creds.AccessKey,
		client: client.New(client.Options{
			Address:   addr,
			UserAgent: "Sia-Agent",
			Password:  os.Getenv("SIA_API_PASSWORD"),
		}),
	}

	colorBlue := color.New(color.FgBlue).SprintfFunc()
	colorBold := color.New(color.Bold).SprintFunc()

	formatStr := "%" + fmt.Sprintf("%ds", len(sia.Address)+7)
	logger.StartupMessage(colorBlue("\nSia Configuration:"))
	logger.StartupMessage(colorBlue("  API Address:") + colorBold(fmt.Sprintf(formatStr, sia.Address)))

	return sia, nil
}

// Production - sia gateway is not ready for production use.
func (g *Sia) Production() bool {
	return false
}

// non2xx returns true for non-success HTTP status codes.
func non2xx(code int) bool {
	return code < 200 || code > 299
}

// decodeError returns the api.Error from a API response. This method should
// only be called if the response's status code is non-2xx. The error returned
// may not be of type api.Error in the event of an error unmarshalling the
// JSON.
type siaError struct {
	// Message describes the error in English. Typically it is set to
	// `err.Error()`. This field is required.
	Message string `json:"message"`
}

func (s siaError) Error() string {
	return s.Message
}

func decodeError(resp *http.Response) error {
	// Error is a type that is encoded as JSON and returned in an API response in
	// the event of an error. Only the Message field is required. More fields may
	// be added to this struct in the future for better error reporting.
	var apiErr siaError
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		return err
	}
	return apiErr
}

// MethodNotSupported - returned if call returned error.
type MethodNotSupported struct {
	method string
}

func (s MethodNotSupported) Error() string {
	return fmt.Sprintf("API call not recognized: %s", s.method)
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to Sia backend.
func (s *siaObjects) StorageInfo(ctx context.Context, _ bool) (si minio.StorageInfo, _ []error) {
	return si, nil
}

func (s *siaObjects) streamingDownload(ctx context.Context, siapath string, start, length int64) (*http.Response, error) {
	os.Stdout.WriteString(fmt.Sprintf("Downloading: %s\n", siapath))
	req, err := s.client.NewRequest("GET", "/renter/stream/"+siapath, nil)

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, length))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		closeResponse(resp)
		return nil, MethodNotSupported{"/renter/stream"}
	}

	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		closeResponse(resp)
		return nil, err
	}

	return resp, nil
}

// MakeBucket creates a new container on Sia backend.
func (s *siaObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string, lockEnabled bool) error {
	if lockEnabled {
		return minio.NotImplemented{}
	}

	sha256sum := sha256.Sum256([]byte(bucket))
	siaObj := path.Join(s.RootDir, bucket, hex.EncodeToString(sha256sum[:]))

	bucketPath, err := modules.NewSiaPath(siaObj)
	if err != nil {
		return err
	}

	return s.client.RenterUploadStreamPost(bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), bucketPath, 10, 30, false)
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(ctx context.Context, bucket string) (minio.BucketInfo, error) {
	sha256sum := sha256.Sum256([]byte(bucket))
	bucketFilePath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket, hex.EncodeToString(sha256sum[:])))
	if err != nil {
		return minio.BucketInfo{}, err
	}

	resp, err := s.client.RenterFileGet(bucketFilePath)
	if err != nil {
		return minio.BucketInfo{}, minio.ObjectNotFound{
			Bucket: bucket,
		}
	}

	return minio.BucketInfo{
		Name:    bucket,
		Created: resp.File.CreateTime,
	}, nil
}

// ListBuckets will detect and return existing buckets on Sia.
func (s *siaObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	rootPath, err := modules.NewSiaPath(s.RootDir)
	if err != nil {
		return
	}

	directories, err := s.client.RenterDirGet(rootPath)
	if err != nil {
		return
	}

	for _, dir := range directories.Directories {
		if dir.SiaPath == rootPath {
			continue
		}

		buckets = append(buckets, minio.BucketInfo{
			Name:    dir.Name(),
			Created: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		})
	}

	return
}

// DeleteBucket deletes a bucket on Sia.
func (s *siaObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	bucketPath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket))
	if err != nil {
		return err
	}

	return s.client.RenterDirDeletePost(bucketPath)
}

func (s *siaObjects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	bucketPath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket))
	if err != nil {
		return
	}

	files, err := s.client.RenterDirGet(bucketPath)
	if err != nil {
		return
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	sha256sum := sha256.Sum256([]byte(bucket))
	bucketFile := hex.EncodeToString(sha256sum[:])
	// FIXME(harsha) - No paginated output supported for Sia backend right now, only prefix
	// based filtering. Once list renter files API supports paginated output we can support
	// paginated results here as well - until then Listing is an expensive operation.
	for _, file := range files.Files {
		name := path.Base(file.Name())

		// Skip the file created specially when bucket was created.
		if name == bucketFile {
			continue
		}

		if strings.HasPrefix(name, prefix) {
			loi.Objects = append(loi.Objects, minio.ObjectInfo{
				Bucket:  bucket,
				Name:    name,
				ModTime: file.ChangeTime,
				Size:    int64(file.Filesize),
				IsDir:   false,
			})
		}
	}

	return
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *siaObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = s.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := s.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)
}

func (s *siaObjects) GetObject(ctx context.Context, bucket string, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	siaObj := path.Join(s.RootDir, url.PathEscape(bucket), url.PathEscape(object))
	resp, err := s.streamingDownload(ctx, siaObj, startOffset, length)
	if err != nil {
		if strings.Contains(err.Error(), "path does not exist") {
			return minio.ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
		}
		return err
	}

	defer closeResponse(resp)

	_, err = io.Copy(writer, resp.Body)

	return err
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	objectPath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket, object))
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	so, err := s.client.RenterFileGet(objectPath)
	if err != nil {
		return minio.ObjectInfo{}, minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	// Metadata about sia objects is just quite minimal. Sia only provides file size.
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: so.File.ChangeTime,
		Size:    int64(so.File.Filesize),
		IsDir:   false,
	}, nil
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	bucketPath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket))
	if err != nil {
		return
	}

	if _, err = s.client.RenterDirGet(bucketPath); err != nil {
		return minio.ObjectInfo{}, minio.BucketNotFound{
			Bucket: bucket,
		}
	}

	objectPath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket, object))
	if err != nil {
		return
	}

	if err = s.client.RenterUploadStreamPost(r.Reader, objectPath, 10, 30, false); err != nil {
		return
	}

	file, err := s.client.RenterFileGet(objectPath)

	return minio.ObjectInfo{
		Name:    object,
		Bucket:  bucket,
		ModTime: file.File.ChangeTime,
		Size:    int64(file.File.Filesize),
		ETag:    minio.GenETag(),
	}, nil
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(ctx context.Context, bucket string, object string) error {
	objectPath, err := modules.NewSiaPath(path.Join(s.RootDir, bucket, object))
	if err != nil {
		return err
	}

	return s.client.RenterFileDeletePost(objectPath)
}

func (s *siaObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = s.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (s *siaObjects) IsCompressionSupported() bool {
	return false
}
