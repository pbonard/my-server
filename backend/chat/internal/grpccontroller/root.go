package grpccontroller

import (
	"backend/chat/internal/controller"
	"backend/common"
	"backend/common/proto"
	"context"
	"log/slog"

	"github.com/google/uuid"
)

type GRPCController struct {
	controller *controller.Controller
	proto.UnimplementedMessagingServiceServer
	proto.UnimplementedSignalServiceServer
}

func NewGRPCController(controller *controller.Controller) *GRPCController {
	gc := GRPCController{controller: controller}

	return &gc
}

func (gc *GRPCController) RelayMessaging(ctx context.Context, in *proto.RelayMessagingRequest) (*proto.RelayMessagingResponse, error) {
	slog.Info("relay message GRPC request incoming...")
	pushToIds, _ := gc.controller.RelayMessaging(ctx, uuid.UUID(in.Id), in.ToIds, uuid.UUID(in.RoomId), uuid.UUID(in.FromId), in.ContentType, in.Contents)
	return &proto.RelayMessagingResponse{
		PushToIds: pushToIds,
	}, nil
}

// RelaySignal writes a call signal from another chat server, through signalrelay, to the members connected here
func (gc *GRPCController) RelaySignal(ctx context.Context, in *proto.RelaySignalRequest) (*proto.RelaySignalResponse, error) {
	gc.controller.RelaySignal(ctx, common.UUIDsOf(in.ToIds), uuid.UUID(in.FromId), in.Signal)
	// a response and not nil: grpc can't send a nil message, the caller would get an error
	return &proto.RelaySignalResponse{}, nil
}
