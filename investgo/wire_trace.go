package investgo

import (
	"context"
	"strings"

	"google.golang.org/grpc/stats"
)

type wireTraceKey struct{}

// orderWireTracer logs low-level gRPC attempt/send events for order-placing RPCs ONLY
// (PostOrder / PostStopOrder). It exists to diagnose below-interceptor order multiplication:
// whether a single conn.Invoke turns into N wire sends / transparent retries (client side) or
// exactly one send that the broker nevertheless multiplies (broker side).
//
// Scope is intentionally narrow (order methods only) so it stays silent for market-data streams
// and portfolio polling. Safe to keep compiled in; near-zero overhead outside order placement.
type orderWireTracer struct{ l Logger }

func (t *orderWireTracer) TagRPC(ctx context.Context, i *stats.RPCTagInfo) context.Context {
	if i != nil && (strings.Contains(i.FullMethodName, "PostOrder") || strings.Contains(i.FullMethodName, "PostStopOrder")) {
		return context.WithValue(ctx, wireTraceKey{}, i.FullMethodName)
	}
	return ctx
}

func (t *orderWireTracer) HandleRPC(ctx context.Context, s stats.RPCStats) {
	m, ok := ctx.Value(wireTraceKey{}).(string)
	if !ok || t.l == nil {
		return
	}
	short := m
	if idx := strings.LastIndex(m, "/"); idx >= 0 {
		short = m[idx+1:]
	}
	switch v := s.(type) {
	case *stats.Begin:
		t.l.Infof("WIRE %s BEGIN client=%v failfast=%v transparentRetry=%v at=%s",
			short, v.Client, v.FailFast, v.IsTransparentRetryAttempt, v.BeginTime.Format("15:04:05.000"))
	case *stats.OutPayload:
		t.l.Infof("WIRE %s OUT wireLen=%d at=%s", short, v.WireLength, v.SentTime.Format("15:04:05.000"))
	case *stats.InPayload:
		t.l.Infof("WIRE %s IN len=%d", short, v.Length)
	case *stats.End:
		t.l.Infof("WIRE %s END err=%v dur=%s", short, v.Error, v.EndTime.Sub(v.BeginTime))
	}
}

func (t *orderWireTracer) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (t *orderWireTracer) HandleConn(_ context.Context, _ stats.ConnStats) {}
