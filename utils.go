package simplecon

import (
	"unsafe"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func MapHeaders(headers []kafka.Header) map[string]string {
	m := make(map[string]string)
	for _, h := range headers {
		m[h.Key] = BytesToString(h.Value)
	}
	return m
}

func PrintWelcomeTextGRPC() {
	log.Info().Msg("Welcome to SimpleCon GRPC")
}
