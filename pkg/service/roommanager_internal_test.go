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

package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
)

type participantWithSessionStart struct {
	*typesfakes.FakeLocalParticipant
	sessionStartTime time.Time
}

func (p participantWithSessionStart) SessionStartTime() time.Time {
	return p.sessionStartTime
}

func TestJoinedBySupersedingSession(t *testing.T) {
	attemptStart := time.Now()
	participant := func(sessionStart time.Time, closed bool) participantWithSessionStart {
		fake := &typesfakes.FakeLocalParticipant{}
		fake.IsClosedReturns(closed)
		return participantWithSessionStart{FakeLocalParticipant: fake, sessionStartTime: sessionStart}
	}

	require.True(t, joinedBySupersedingSession(participant(attemptStart.Add(time.Second), false), attemptStart),
		"a live participant from a later attempt holds the identity")
	require.False(t, joinedBySupersedingSession(participant(attemptStart.Add(-time.Second), false), attemptStart),
		"an older participant is replaced as a duplicate")
	require.False(t, joinedBySupersedingSession(participant(attemptStart.Add(time.Second), true), attemptStart),
		"a closed participant waiting to be removed does not hold the identity")
	require.False(t, joinedBySupersedingSession(&typesfakes.FakeLocalParticipant{}, attemptStart),
		"a participant without a session start time is replaced as before")
}
