package ids

import "github.com/google/uuid"

func New(prefix string) string {
	return prefix + "_" + uuid.NewString()
}
