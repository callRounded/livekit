// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/psrpc"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/service/servicefakes"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
)

const (
	testRoomName = "sip_+33974994331_test"
	testRoomSID  = "RM_test"
	testIdentity = "sip_caller"
)

type fakeEgressLauncher struct {
	calls   chan *rpc.StartEgressRequest
	release chan struct{}
	err     error
}

func newFakeEgressLauncher() *fakeEgressLauncher {
	return &fakeEgressLauncher{calls: make(chan *rpc.StartEgressRequest, 8)}
}

func (l *fakeEgressLauncher) StartEgress(_ context.Context, req *rpc.StartEgressRequest) (*livekit.EgressInfo, error) {
	l.calls <- req
	if l.release != nil {
		<-l.release
	}
	if l.err != nil {
		return nil, l.err
	}
	return &livekit.EgressInfo{EgressId: "EG_test"}, nil
}

func roomAllocatorReturning(created bool) *servicefakes.FakeRoomAllocator {
	allocator := &servicefakes.FakeRoomAllocator{}
	allocator.CreateRoomStub = func(_ context.Context, req *livekit.CreateRoomRequest, _ bool) (*livekit.Room, *livekit.RoomInternal, bool, error) {
		return &livekit.Room{Name: req.Name, Sid: testRoomSID}, &livekit.RoomInternal{}, created, nil
	}
	return allocator
}

func newTestRoomManager(
	t *testing.T,
	allocator service.RoomAllocator,
	launcher rtc.EgressLauncher,
	telemetry *telemetryfakes.FakeTelemetryService,
) *service.RoomManager {
	t.Helper()
	if telemetry == nil {
		telemetry = &telemetryfakes.FakeTelemetryService{}
	}
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	currentNode, err := routing.NewLocalNode(conf)
	require.NoError(t, err)
	rm, err := service.NewLocalRoomManager(
		conf,
		service.NewLocalStore(),
		currentNode,
		&routingfakes.FakeRouter{},
		allocator,
		telemetry,
		nil,
		&servicefakes.FakeAgentStore{},
		launcher,
		utils.NewDefaultTimedVersionGenerator(),
		service.NewTURNAuthHandler(auth.NewSimpleKeyProvider("key", "secret")),
		psrpc.NewLocalMessageBus(),
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(rm.Stop)
	return rm
}

func sipParticipantInit(roomEgress *livekit.RoomEgress) routing.ParticipantInit {
	return routing.ParticipantInit{
		Identity: testIdentity,
		Client:   &livekit.ClientInfo{Sdk: livekit.ClientInfo_GO, Protocol: types.CurrentProtocol},
		Grants: &auth.ClaimGrants{
			Identity: testIdentity,
			Video:    &auth.VideoGrant{RoomJoin: true, Room: testRoomName},
		},
		AutoSubscribe: true,
		CreateRoom:    &livekit.CreateRoomRequest{Name: testRoomName, Egress: roomEgress},
	}
}

// startSession runs a join attempt the way the signal server does, with its own
// request and response streams, closed when the test ends.
func startSession(t *testing.T, rm *service.RoomManager, pi routing.ParticipantInit, requestSource *routing.MessageChannel) error {
	t.Helper()
	t.Cleanup(requestSource.Close)
	sink := routing.NewDefaultMessageChannel(requestSource.ConnectionID())
	t.Cleanup(sink.Close)
	return rm.StartSession(context.Background(), pi, requestSource, sink, false)
}

// blockFirstCall returns a function that blocks its first caller until release
// is closed, after signalling on blocked. Later callers go straight through.
func blockFirstCall() (wait func(), blocked, release chan struct{}) {
	blocked, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	return func() {
		first := false
		once.Do(func() { first = true })
		if first {
			close(blocked)
			<-release
		}
	}, blocked, release
}

// raceJoinAttempts plays a join attempt held up in room creation, and the
// signal relay's retry that joins in the meantime. slowStep picks where the
// first attempt is held up: in production it was after the room was registered
// (the room-config egress), so the retry reused that room.
func raceJoinAttempts(t *testing.T, slowStep string, closeFirstStream bool) (firstErr error, room *rtc.Room, live types.LocalParticipant) {
	t.Helper()
	wait, blocked, release := blockFirstCall()
	allocator := roomAllocatorReturning(true)
	telemetry := &telemetryfakes.FakeTelemetryService{}
	switch slowStep {
	case "allocation":
		allocator.CreateRoomStub = func(_ context.Context, req *livekit.CreateRoomRequest, _ bool) (*livekit.Room, *livekit.RoomInternal, bool, error) {
			wait()
			return &livekit.Room{Name: req.Name, Sid: testRoomSID}, &livekit.RoomInternal{}, true, nil
		}
	case "after registration":
		telemetry.RoomStartedStub = func(context.Context, *livekit.Room) { wait() }
	default:
		t.Fatalf("unknown slow step %q", slowStep)
	}
	rm := newTestRoomManager(t, allocator, newFakeEgressLauncher(), telemetry)

	firstStream := routing.NewDefaultMessageChannel("CO_first")
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- startSession(t, rm, sipParticipantInit(nil), firstStream)
	}()
	<-blocked

	// The relay times out the first attempt and retries; the retry joins.
	if closeFirstStream {
		firstStream.Close()
	}
	require.NoError(t, startSession(t, rm, sipParticipantInit(nil), routing.NewDefaultMessageChannel("CO_second")))
	room = rm.GetRoom(context.Background(), testRoomName)
	require.NotNil(t, room)
	live = room.GetParticipant(testIdentity)
	require.NotNil(t, live)

	// The first attempt finally gets its room.
	close(release)
	select {
	case firstErr = <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the abandoned join attempt did not return")
	}
	return firstErr, room, live
}

// The signal relay gives up on a join attempt after its connection timeout,
// closes that attempt's stream and starts a new one. The first attempt, still
// busy creating the room, used to come back later and evict the participant the
// retry had joined (DUPLICATE_IDENTITY), cutting a live SIP call.
func TestAbandonedJoinAttemptDoesNotEvictTheLiveParticipant(t *testing.T) {
	for _, slowStep := range []string{"allocation", "after registration"} {
		t.Run(slowStep, func(t *testing.T) {
			firstErr, room, live := raceJoinAttempts(t, slowStep, true)
			require.False(t, live.IsClosed(), "the live participant was evicted")
			require.Same(t, live, room.GetParticipant(testIdentity))
			require.ErrorIs(t, firstErr, service.ErrSignalConnectionClosed)
		})
	}
}

// The close of the abandoned attempt's stream travels over the message bus and
// can arrive late: the most recently started attempt must still win.
func TestStaleJoinAttemptDoesNotEvictANewerSessionBeforeItsStreamCloses(t *testing.T) {
	firstErr, room, live := raceJoinAttempts(t, "after registration", false)
	require.False(t, live.IsClosed(), "the live participant was evicted")
	require.Same(t, live, room.GetParticipant(testIdentity))
	require.ErrorIs(t, firstErr, service.ErrJoinAttemptSuperseded)
}

// A new connection with the same identity still replaces an older participant.
func TestNewJoinStillReplacesAnOlderParticipant(t *testing.T) {
	rm := newTestRoomManager(t, roomAllocatorReturning(true), newFakeEgressLauncher(), nil)
	require.NoError(t, startSession(t, rm, sipParticipantInit(nil), routing.NewDefaultMessageChannel("CO_old")))
	room := rm.GetRoom(context.Background(), testRoomName)
	old := room.GetParticipant(testIdentity)
	require.NotNil(t, old)

	require.NoError(t, startSession(t, rm, sipParticipantInit(nil), routing.NewDefaultMessageChannel("CO_new")))
	require.True(t, old.IsClosed())
	require.NotSame(t, old, room.GetParticipant(testIdentity))
}

// A resume whose stream the relay already gave up on must not hand the live
// session a dead stream.
func TestAbandonedResumeKeepsTheLiveSession(t *testing.T) {
	rm := newTestRoomManager(t, roomAllocatorReturning(true), newFakeEgressLauncher(), nil)
	require.NoError(t, startSession(t, rm, sipParticipantInit(nil), routing.NewDefaultMessageChannel("CO_live")))
	live := rm.GetRoom(context.Background(), testRoomName).GetParticipant(testIdentity)
	require.NotNil(t, live)
	sink := live.GetResponseSink()

	resume := sipParticipantInit(nil)
	resume.Reconnect = true
	abandoned := routing.NewDefaultMessageChannel("CO_resume")
	abandoned.Close()
	require.ErrorIs(t, startSession(t, rm, resume, abandoned), service.ErrSignalConnectionClosed)
	require.True(t, sink == live.GetResponseSink(), "the live session's stream was replaced")
	require.False(t, live.IsClosed())
}

// WHIP joins have no request stream: their null source is never closed.
func TestWHIPJoinIsNotDropped(t *testing.T) {
	rm := newTestRoomManager(t, roomAllocatorReturning(true), newFakeEgressLauncher(), nil)
	connID := livekit.ConnectionID("CO_whip")
	require.NoError(t, rm.StartSession(
		context.Background(),
		sipParticipantInit(nil),
		routing.NewNullMessageSource(connID),
		routing.NewNullMessageSink(connID),
		true,
	))
	require.NotNil(t, rm.GetRoom(context.Background(), testRoomName).GetParticipant(testIdentity))
}
