package test

import (
	"backend/chat/internal/controller"
	"backend/chat/internal/dto"
	"backend/chat/internal/grpccontroller"
	"backend/common/proto"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func serve(svc *MockService, method, target, userId, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	controller.NewController(svc, mux)
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if userId != "" {
		req.Header.Set("X-User-Id", userId)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestController_sendMessaging(t *testing.T) {
	svc := NewMockService(t)
	msgId := uuid.New()
	svc.EXPECT().PublishMessaging(mock.Anything, memberId, "group", roomId, "text", []string{"hi"}, false).
		Return(map[string]uuid.UUID{"id": msgId}, nil)

	rec := serve(svc, http.MethodPost, "/chat/messaging/send", memberId.String(),
		fmt.Sprintf(`{"toIdType":"group","toId":"%s","contentType":"text","contents":["hi"]}`, roomId))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, fmt.Sprintf(`{"id":"%s"}`, msgId), rec.Body.String())
}

func TestController_sendMessagingWithNoPush(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().PublishMessaging(mock.Anything, memberId, "personal", otherId, "text", []string{"hi"}, true).
		Return(map[string]uuid.UUID{"id": uuid.New()}, nil)

	rec := serve(svc, http.MethodPost, "/chat/messaging/send", memberId.String(),
		fmt.Sprintf(`{"toIdType":"personal","toId":"%s","contentType":"text","contents":["hi"],"noPush":true}`, otherId))

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestController_mediaEndpointsOnlyAcceptMediaTypes(t *testing.T) {
	file := uuid.New()
	for name, rec := range map[string]*httptest.ResponseRecorder{
		"presigned": serve(NewMockService(t), http.MethodPost, "/chat/messaging/presigned", memberId.String(),
			`{"contentType":"text","n":1}`),
		"signed": serve(NewMockService(t), http.MethodGet,
			"/chat/messaging/signed?contentType=text&filename="+file.String(), memberId.String(), ""),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, "bad request", rec.Body.String())
		})
	}
}

func TestController_presignedAndSignedURL(t *testing.T) {
	svc := NewMockService(t)
	file := uuid.New()
	svc.EXPECT().GeneratePresignedURL(mock.Anything, memberId, "image", 2).
		Return([]dto.GeneratePresignedURLResponse{{Filename: file, URL: "https://s3"}}, nil)
	svc.EXPECT().GenerateSignedURL(mock.Anything, memberId, "video", file).Return(map[string]string{"url": "https://cdn"}, nil)

	presigned := serve(svc, http.MethodPost, "/chat/messaging/presigned", memberId.String(), `{"contentType":"image","n":2}`)
	signed := serve(svc, http.MethodGet, "/chat/messaging/signed?contentType=video&filename="+file.String(), memberId.String(), "")

	assert.Equal(t, http.StatusOK, presigned.Code)
	assert.Contains(t, presigned.Body.String(), `"url":"https://s3"`)
	assert.Equal(t, http.StatusOK, signed.Code)
	assert.JSONEq(t, `{"url":"https://cdn"}`, signed.Body.String())
}

func TestController_rejectsBadRequestsBeforeTheService(t *testing.T) {
	tests := []struct {
		name, method, target, userId, body string
	}{
		{"send without user", http.MethodPost, "/chat/messaging/send", "", `{}`},
		{"send with bad body", http.MethodPost, "/chat/messaging/send", memberId.String(), `{bad`},
		{"recent with bad cursor", http.MethodGet, "/chat/messaging/recent?cursor=nope", memberId.String(), ""},
		{"room info with bad id", http.MethodGet, "/chat/room/info?id=nope", memberId.String(), ""},
		{"profile with bad id", http.MethodGet, "/chat/profile?id=nope", memberId.String(), ""},
		{"my profile without user", http.MethodGet, "/chat/profile/my", "", ""},
		{"set name with bad body", http.MethodPut, "/chat/profile/name", memberId.String(), `{bad`},
		{"check block with bad id", http.MethodGet, "/chat/block/check?id=nope", memberId.String(), ""},
		{"participants with bad room", http.MethodGet, "/chat/participants?roomId=nope", memberId.String(), ""},
		{"report without user", http.MethodPost, "/chat/report/user", "", `{}`},
		{"block conversation with bad body", http.MethodPost, "/chat/block/conversation", memberId.String(), `{bad`},
		{"blocked conversations without user", http.MethodGet, "/chat/block/conversations", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serve(NewMockService(t), tt.method, tt.target, tt.userId, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, "fail to parse", rec.Body.String())
		})
	}
}

func TestController_delegatesToTheService(t *testing.T) {
	idBody := fmt.Sprintf(`{"id":"%s"}`, otherId)
	tests := []struct {
		name, method, target, body string
		expect                     func(s *MockService_Expecter)
		wantBody                   string
	}{
		{"recent messages", http.MethodGet, "/chat/messaging/recent?cursor=" + roomId.String(), "",
			func(s *MockService_Expecter) {
				s.GetRecentMessages(mock.Anything, memberId, roomId).Return([]dto.MessagingResponse{{ContentType: "text"}}, nil)
			}, `"contentType":"text"`},
		{"room info", http.MethodGet, "/chat/room/info?id=" + roomId.String(), "",
			func(s *MockService_Expecter) {
				s.GetChatRoomInfo(mock.Anything, roomId).Return(&dto.GetChatRoomInfoResponse{Name: "Seoul"}, nil)
			}, `"Seoul"`},
		{"profile", http.MethodGet, "/chat/profile?id=" + otherId.String(), "",
			func(s *MockService_Expecter) {
				s.GetProfile(mock.Anything, otherId).Return(&dto.GetProfileResponse{Name: "bob"}, nil)
			}, `"name":"bob"`},
		{"my profile", http.MethodGet, "/chat/profile/my", "",
			func(s *MockService_Expecter) {
				s.GetProfile(mock.Anything, memberId).Return(&dto.GetProfileResponse{Name: "alice"}, nil)
			}, `"name":"alice"`},
		{"set name", http.MethodPut, "/chat/profile/name", `{"name":"alice"}`,
			func(s *MockService_Expecter) { s.SetName(mock.Anything, memberId, "alice").Return(nil) }, ""},
		{"check block", http.MethodGet, "/chat/block/check?id=" + otherId.String(), "",
			func(s *MockService_Expecter) {
				s.CheckBlock(mock.Anything, memberId, otherId).Return(map[string]bool{"didBlock": true}, nil)
			}, `"didBlock":true`},
		{"participants", http.MethodGet, "/chat/participants?roomId=" + roomId.String(), "",
			func(s *MockService_Expecter) {
				s.GetChatParticipants(mock.Anything, roomId).Return([]dto.GetProfileResponse{{Id: otherId}}, nil)
			}, otherId.String()},
		{"report user", http.MethodPost, "/chat/report/user", idBody,
			func(s *MockService_Expecter) {
				s.ReportUser(mock.Anything, memberId, otherId).Return(nil)
			}, ""},
		{"block conversation", http.MethodPost, "/chat/block/conversation", idBody,
			func(s *MockService_Expecter) {
				s.BlockConversation(mock.Anything, memberId, otherId).Return(nil)
			}, ""},
		{"blocked conversations", http.MethodGet, "/chat/block/conversations", "",
			func(s *MockService_Expecter) {
				s.GetBlockedConversations(mock.Anything, memberId).Return([]dto.BlockReport{{Id: roomId}}, nil)
			}, roomId.String()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewMockService(t)
			tt.expect(svc.EXPECT())

			rec := serve(svc, tt.method, tt.target, memberId.String(), tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestController_serviceErrorIsBadRequest(t *testing.T) {
	svc := NewMockService(t)
	svc.EXPECT().SetName(mock.Anything, memberId, " ").Return(errors.New("incorrect name"))

	rec := serve(svc, http.MethodPut, "/chat/profile/name", memberId.String(), `{"name":" "}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "incorrect name", rec.Body.String())
}

func TestGRPCController_receiversWithoutConnectionAreReturnedForPush(t *testing.T) {
	gc := grpccontroller.NewGRPCController(controller.NewController(NewMockService(t), http.NewServeMux()))
	msgId := uuid.New()

	res, err := gc.RelayMessaging(context.Background(), &proto.RelayMessagingRequest{
		Id: msgId[:], ToIds: [][]byte{otherId[:]}, RoomId: roomId[:], FromId: memberId[:], ContentType: "text", Contents: []string{"hi"},
	})

	assert.NoError(t, err)
	assert.Equal(t, [][]byte{otherId[:]}, res.PushToIds)
}
