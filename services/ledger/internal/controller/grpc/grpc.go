package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/pkg/logger"
	ver1 "github.com/vbloher/payment-processing-system/services/ledger/internal/controller/grpc/v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase"
)

type Config struct {
	Port string `env:"GRPC_PORT" envDefault:"50051"`
}

type Server struct {
	server *grpc.Server
}

func New(ctx context.Context, c Config, uc *usecase.UseCase) (*Server, error) {
	s := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(logger.Interceptor),
	)

	reflection.Register(s)

	v1 := ver1.New(uc)
	pb.RegisterLedgerV1Server(s, v1)

	err := start(ctx, s, c.Port)
	if err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	return &Server{server: s}, nil
}

func start(ctx context.Context, server *grpc.Server, port string) error {
	var lc net.ListenConfig

	conn, err := lc.Listen(ctx, "tcp", net.JoinHostPort("", port))
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	go func() {
		err := server.Serve(conn)
		if err != nil {
			log.Error().Err(err).Msg("grpc server: Serve")
		}
	}()

	log.Info().Msg("grpc server: started on port: " + port)

	return nil
}

func (s *Server) Close() {
	s.server.GracefulStop()

	log.Info().Msg("grpc server: closed")
}
