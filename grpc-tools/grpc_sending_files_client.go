package grpctools

import (
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/amazingchow/snippets-for-gopher/grpc-tools/api"
)

// GRPCClient gRPC客户端
// upload file stream chunks via protobuf-encoded messages.
type GRPCClient struct {
	logger    zerolog.Logger
	conn      *grpc.ClientConn
	client    api.GuploadServiceClient
	chunkSize int
}

// GRPCClientCfg gRPC客户端配置
type GRPCClientCfg struct {
	Address    string `json:"address"`
	ChunkSize  int    `json:"chunk_size"`
	RootCert   string `json:"root_cert"`
	Compressed bool   `json:"compressed"`
}

// NewGRPCClient 返回GRPCClient实例.
func NewGRPCClient(cfg *GRPCClientCfg) (*GRPCClient, error) {
	var (
		opts  = []grpc.DialOption{}
		creds credentials.TransportCredentials
		err   error
	)

	if cfg.Address == "" {
		return nil, errors.Errorf("address must be specified")
	}

	if cfg.Compressed {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}

	if cfg.RootCert != "" {
		creds, err = credentials.NewClientTLSFromFile(cfg.RootCert, "localhost")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create tls grpc client using root-cert '%s'", cfg.RootCert)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	cli := &GRPCClient{}
	cli.logger = zerolog.New(os.Stdout).With().Str("from", "client").Logger()

	switch {
	case cfg.ChunkSize == 0:
		return nil, errors.Errorf("chunk_size must be specified")
	case cfg.ChunkSize > (1 << 22):
		return nil, errors.Errorf("chunk_size must be less than 4MB")
	default:
		cli.chunkSize = cfg.ChunkSize
	}

	cli.conn, err = grpc.Dial(cfg.Address, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create grpc connection with address %s", cfg.Address)
	}

	cli.client = api.NewGuploadServiceClient(cli.conn)

	return cli, nil
}

// Close 停止运行gRPC客户端.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		c.conn.Close()
	}

	return nil
}

// UploadFile 上传文件.
func (c *GRPCClient) UploadFile(ctx context.Context, fn string) (Stats, error) {
	var (
		writing = true
		buffer  []byte
		n       int
		fd      *os.File
		status  *api.UploadStatus
		stats   Stats
		err     error
	)

	fd, err = os.Open(fn)
	if err != nil {
		return stats, errors.Wrapf(err, "failed to open file %s", fn)
	}
	defer fd.Close()

	stream, err := c.client.Upload(ctx)
	if err != nil {
		return stats, errors.Wrapf(err, "failed to create upload stream for file %s", fn)
	}
	defer stream.CloseSend()

	stats.StartedAt = time.Now()

	buffer = make([]byte, c.chunkSize)
	for writing {
		n, err = fd.Read(buffer)
		if err != nil {
			if err == io.EOF {
				writing = false
				err = nil
				continue
			}
			return stats, errors.Wrapf(err, "failed unexpectedly while copying from file to buffer")
		}

		if err = stream.Send(&api.Chunk{
			Content: buffer[:n],
		}); err != nil {
			return stats, errors.Wrapf(err, "failed to send chunk via stream")
		}
	}

	stats.FinishedAt = time.Now()

	status, err = stream.CloseAndRecv()
	if err != nil {
		return stats, errors.Wrapf(err, "failed to receive upstream status response")
	}

	if status.Code != api.UploadStatusCode_STATUS_CODE_OK {
		return stats, errors.Errorf("upload failed - msg: %s", status.Message)
	}

	return stats, nil
}
