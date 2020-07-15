package grpctools

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/amazingchow/snippets-for-gopher/grpc-tools/api"
)

// GRPCServer gRPC服务端
type GRPCServer struct {
	logger zerolog.Logger
	server *grpc.Server
	port   int
	cert   string
	key    string
}

// GRPCServerCfg gRPC服务端配置
type GRPCServerCfg struct {
	Port int    `json:"port"`
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

// NewGRPCServer 返回GRPCServer实例.
func NewGRPCServer(cfg *GRPCServerCfg) (*GRPCServer, error) {
	if cfg.Port == 0 {
		return nil, errors.Errorf("port must be specified")
	}

	srv := &GRPCServer{}
	srv.logger = zerolog.New(os.Stdout).With().Str("from", "grpc server").Logger()
	srv.port = cfg.Port
	srv.cert = cfg.Cert
	srv.key = cfg.Key

	return srv, nil
}

// Run 开始运行gRPC服务端.
func (srv *GRPCServer) Run() error {
	var (
		l     net.Listener
		opts  = []grpc.ServerOption{}
		creds credentials.TransportCredentials
		err   error
	)

	l, err = net.Listen("tcp", fmt.Sprintf(":%d", srv.port))
	if err != nil {
		return errors.Wrapf(err, "failed to listen on port %d", srv.port)
	}

	if srv.cert != "" && srv.key != "" {
		creds, err = credentials.NewServerTLSFromFile(srv.cert, srv.key)
		if err != nil {
			return errors.Wrapf(err, "failed to create tls grpc server using cert '%s' and key '%s'", srv.cert, srv.key)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	srv.server = grpc.NewServer(opts...)
	api.RegisterGuploadServiceServer(srv.server, srv)

	if err = srv.server.Serve(l); err != nil {
		return errors.Wrapf(err, "failed to listen for grpc connections")
	}

	return nil
}

// Close 停止运行gRPC服务端.
func (srv *GRPCServer) Close() error {
	if srv.server != nil {
		srv.server.Stop()
	}

	return nil
}

// Upload Upload接口.
func (srv *GRPCServer) Upload(stream api.GuploadService_UploadServer) error {
	var (
		err error
	)

RECV_LOOP:
	for {
		_, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break RECV_LOOP
			}
			return errors.Wrapf(err, "failed unexpectedly while reading chunks from stream")
		}
	}

	srv.logger.Info().Msg("upload received")

	if err = stream.SendAndClose(&api.UploadStatus{
		Message: "Successfully Upload",
		Code:    api.UploadStatusCode_STATUS_CODE_OK,
	}); err != nil {
		return errors.Wrapf(err, "failed to send status code")
	}

	return nil
}
