package service

import (
	"testing"

	"github.com/dennwc/iters"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/sip"
)

func matchOne(t *testing.T, trunk *livekit.SIPInboundTrunkInfo, call *rpc.SIPCall) bool {
	t.Helper()
	tr, err := sip.MatchTrunkIter(iters.Slice([]*livekit.SIPInboundTrunkInfo{trunk}), withoutSpoofableFromHost(call))
	require.NoError(t, err)
	return tr != nil
}

func call(sourceIP, fromHost string) *rpc.SIPCall {
	return &rpc.SIPCall{
		SourceIp: sourceIP,
		From:     &livekit.SIPUri{User: "+33100000042", Host: fromHost},
		To:       &livekit.SIPUri{User: "+33199000111", Host: "sip.example"},
	}
}

func TestAllowedAddressesIgnoreAnIPWrittenInFrom(t *testing.T) {
	trunk := &livekit.SIPInboundTrunkInfo{
		SipTrunkId:       "ST_twilio",
		Numbers:          []string{"+33199000111"},
		AllowedAddresses: []string{"35.156.191.128/30", "54.171.127.193"},
	}

	// From anywhere, claiming an allowed address in From: refused.
	for _, host := range []string{
		"35.156.191.128/30", "54.171.127.193", "[54.171.127.193]", "35.156.191.129",
	} {
		require.False(t, matchOne(t, trunk, call("203.0.113.9", host)), host)
	}

	// From an allowed address: accepted, whatever From says.
	require.True(t, matchOne(t, trunk, call("35.156.191.130", "abc.pstn.twilio.com")))
	require.True(t, matchOne(t, trunk, call("54.171.127.193", "54.171.127.193")))
	require.True(t, matchOne(t, trunk, call("35.156.191.129", "203.0.113.9")))
}

func TestAHostnameEntryStillMatchesTheFromHost(t *testing.T) {
	trunk := &livekit.SIPInboundTrunkInfo{
		SipTrunkId:       "ST_pbx",
		Numbers:          []string{"+33199000111"},
		AllowedAddresses: []string{"pbx.example.com"},
	}
	require.True(t, matchOne(t, trunk, call("198.51.100.7", "pbx.example.com")))
	require.False(t, matchOne(t, trunk, call("198.51.100.7", "other.example.com")))
}

func TestATrunkWithoutAllowedAddressesStillTakesAnyCaller(t *testing.T) {
	trunk := &livekit.SIPInboundTrunkInfo{SipTrunkId: "ST_open", Numbers: []string{"+33199000111"}}
	require.True(t, matchOne(t, trunk, call("203.0.113.9", "35.156.191.129")))
}

func TestTheCallerIsNotModified(t *testing.T) {
	c := call("203.0.113.9", "35.156.191.129")
	_ = withoutSpoofableFromHost(c)
	require.Equal(t, "35.156.191.129", c.From.Host)
}

// Fails once protocol carries b8c797a0 (#1889): withoutSpoofableFromHost can go then.
func TestThePinnedProtocolStillMatchesAnIPWrittenInFrom(t *testing.T) {
	trunk := &livekit.SIPInboundTrunkInfo{
		SipTrunkId:       "ST_twilio",
		Numbers:          []string{"+33199000111"},
		AllowedAddresses: []string{"54.171.127.193"},
	}
	tr, err := sip.MatchTrunkIter(iters.Slice([]*livekit.SIPInboundTrunkInfo{trunk}), call("203.0.113.9", "54.171.127.193"))
	require.NoError(t, err)
	require.NotNil(t, tr)
}
