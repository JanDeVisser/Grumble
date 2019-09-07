package grumble

import "time"

type Trail struct {
	Owner         string
	CreatedBy     string
	Created       time.Time
	LastUpdatedBy string
	LastUpdated   time.Time
}
