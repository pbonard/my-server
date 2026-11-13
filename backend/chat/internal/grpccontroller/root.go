package grpccontroller

import (
	"backend/chat/internal/controller"
	"backend/chat/internal/service"
	"backend/common"
	"backend/common/proto"
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/uuid"
)

type GRPCController struct {
	controller *controller.Controller
	service    service.Service
	proto.UnimplementedMessagingServiceServer
	proto.UnimplementedSignalServiceServer
	proto.UnimplementedInviteServiceServer
}

func NewGRPCController(controller *controller.Controller, service service.Service) *GRPCController {
	gc := GRPCController{controller: controller, service: service}

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

// CheckInvite is what holidayfriend-server asks before it sends an invite from fromId to toId
func (gc *GRPCController) CheckInvite(ctx context.Context, in *proto.CheckInviteRequest) (*proto.CheckInviteResponse, error) {
	fromId, err := uuid.FromBytes(in.FromId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "fromId is not a uuid")
	}
	toId, err := uuid.FromBytes(in.ToId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "toId is not a uuid")
	}
	res, err := gc.service.CheckInvite(ctx, fromId, toId)
	if err != nil {
		return nil, status.Error(codes.Internal, "fail to check invite")
	}
	return &proto.CheckInviteResponse{
		Blocked:       res.Blocked,
		SenderHasChat: res.SenderHasChat,
	}, nil
}
