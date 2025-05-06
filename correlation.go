package simplecon

type correlation string

const (
	// CorrelationID is the key for the correlation id
	CorrelationID correlation = "x-correlation-id"
)

func (c correlation) String() string {
	return string(c)
}
