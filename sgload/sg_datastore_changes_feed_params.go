package sgload

import (
	"fmt"
	"strings"
)

type ChangesFeedType string

const FEED_TYPE_LONGPOLL = ChangesFeedType("longpoll")
const FEED_TYPE_NORMAL = ChangesFeedType("normal")

type ChangesFeedParams struct {
	feedType            ChangesFeedType // eg, "normal" or "longpoll"
	limit               int             // eg, 50
	heartbeatTimeMillis int             // eg, 300000
	feedStyle           string          // eg, "all_docs"
	since               Sincer          // eg, "3",
	channels            []string
}

func NewChangesFeedParams(sinceVal Sincer, limit int, feedType ChangesFeedType) *ChangesFeedParams {
	return &ChangesFeedParams{
		feedType:            feedType,
		limit:               limit,
		heartbeatTimeMillis: 30 * 1000,
		feedStyle:           "all_docs",
		since:               sinceVal,
	}
}

func (p ChangesFeedParams) String() string {
	params := fmt.Sprintf(
		"feed=%s&limit=%d&heartbeat=%d&style=%s",
		p.feedType,
		p.limit,
		p.heartbeatTimeMillis,
		p.feedStyle,
	)
	if !p.since.Empty() {
		params = fmt.Sprintf("%v&since=%s", params, p.since)
	}
	if len(p.channels) > 0 {
		params = fmt.Sprintf("%v&filter=sync_gateway/bychannel&channels=%s", params, strings.Join(p.channels, ","))
	}
	return params
}

type Sincer interface {
	Empty() bool
	String() string
	Equals(other Sincer) bool
}

type StringSincer struct {
	Since string
}

func (s StringSincer) Empty() bool {
	return s.Since == ""
}

func (s StringSincer) String() string {
	return s.Since
}

func (s StringSincer) Equals(otherSincer Sincer) bool {
	return s.String() == otherSincer.String()
}
