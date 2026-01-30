package redisq

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestShouldFallbackXInfoConsumers(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"wanted6", errors.New("redis: got 8 elements in XINFO CONSUMERS reply, wanted 6"), true},
		{"unexpected", errors.New("redis: unexpected content foo in XINFO CONSUMERS reply"), true},
		{"other", errors.New("some other error"), false},
	}

	for _, tt := range tests {
		if got := shouldFallbackXInfoConsumers(tt.err); got != tt.want {
			t.Fatalf("%s: got %v want %v", tt.name, got, tt.want)
		}
	}
}

func TestParseRawXInfoConsumer(t *testing.T) {
	legacy := []interface{}{"name", "c1", "pending", "2", "idle", "30"}
	consumer, ok := parseRawXInfoConsumer(legacy)
	if !ok {
		t.Fatalf("legacy consumer not parsed")
	}
	if consumer.Name != "c1" || consumer.Pending != 2 || consumer.Idle != 30 {
		t.Fatalf("legacy parsed wrong: %+v", consumer)
	}

	newFmt := []interface{}{"name", "c2", "pending", int64(5), "idle", int64(10), "inactive", int64(40)}
	consumer, ok = parseRawXInfoConsumer(newFmt)
	if !ok {
		t.Fatalf("new consumer not parsed")
	}
	if consumer.Idle != 40 { // prefers inactive
		t.Fatalf("expected idle=inactive(40), got %d", consumer.Idle)
	}

	bad := []interface{}{"pending", "1", "idle", "2"} // missing name
	if _, ok := parseRawXInfoConsumer(bad); ok {
		t.Fatalf("expected bad consumer to be rejected")
	}

	odd := []interface{}{"name", "c3", "pending"} // odd length
	if _, ok := parseRawXInfoConsumer(odd); ok {
		t.Fatalf("expected odd consumer to be rejected")
	}
}

func TestParseRawXInfoConsumersSkipsBad(t *testing.T) {
	val := []interface{}{
		[]interface{}{"name", "good", "pending", "1", "idle", "2"},
		[]interface{}{"pending", "1", "idle", "2"}, // bad
	}
	out, err := parseRawXInfoConsumers(val)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 || out[0].Name != "good" {
		t.Fatalf("unexpected parsed list: %+v", out)
	}
}

func TestXinfoConsumersFallback(t *testing.T) {
	q := &Queue{stream: "s", streamGroup: "g"}
	q.redis = &stubRedis{
		doResult: []interface{}{
			[]interface{}{"name", "c1", "pending", "1", "idle", "10", "inactive", "20"},
		},
	}

	res, err := q.xinfoConsumersFallback(context.Background())
	if err != nil {
		t.Fatalf("fallback err: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 consumer, got %d", len(res))
	}
	if res[0].Name != "c1" || res[0].Idle != 20 {
		t.Fatalf("parsed consumer mismatch: %+v", res[0])
	}
}

// stubRedis implements RedisStreamClient and Do just enough for unit tests.
type stubRedis struct {
	doResult interface{}
	doErr    error
	xinfoErr error
}

func (s *stubRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) TxPipeline() redis.Pipeliner {
	return redis.NewClient(&redis.Options{Addr: "localhost:0"}).TxPipeline()
}

func (s *stubRedis) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	return redis.NewStringResult("", nil)
}

func (s *stubRedis) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) XLen(ctx context.Context, stream string) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	return redis.NewXMessageSliceCmd(ctx)
}

func (s *stubRedis) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return redis.NewStatusResult("", nil)
}

func (s *stubRedis) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmd(ctx)
}

func (s *stubRedis) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return redis.NewXPendingExtCmd(ctx, a)
}

func (s *stubRedis) XTrim(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return redis.NewStringSliceCmd(ctx)
}

func (s *stubRedis) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return redis.NewIntResult(0, nil)
}

func (s *stubRedis) XInfoConsumers(ctx context.Context, key string, group string) *redis.XInfoConsumersCmd {
	cmd := redis.NewXInfoConsumersCmd(ctx, key, group)
	if s.xinfoErr != nil {
		cmd.SetErr(s.xinfoErr)
	}
	return cmd
}

func (s *stubRedis) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(ctx, args...)
	if s.doErr != nil {
		cmd.SetErr(s.doErr)
	} else {
		cmd.SetVal(s.doResult)
	}
	return cmd
}
