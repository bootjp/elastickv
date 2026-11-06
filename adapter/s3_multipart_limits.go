package adapter

import (
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
)

const (
	s3MaxObjectKeyBytes          = 1024
	s3MultipartReadKeyByteBudget = 24 << 20
)

func rejectOversizedS3ObjectKey(w http.ResponseWriter, key string, hasObject bool) bool {
	if !hasObject || len(key) <= s3MaxObjectKeyBytes {
		return false
	}
	writeS3Error(w, http.StatusBadRequest, "KeyTooLongError", "object key exceeds 1024 bytes", "", "")
	return true
}

func validateS3MultipartReadKeyBudget(completion *s3MultipartCompletion, count int) error {
	if len(completion.objectKey) > s3MaxObjectKeyBytes {
		return newS3ResponseError(http.StatusBadRequest, "KeyTooLongError", "object key exceeds 1024 bytes", "", "")
	}
	if count <= 0 || count > s3MaxPartsPerUpload {
		return s3MultipartReadKeyBudgetError()
	}
	// All part numbers and the generation have fixed-width encodings. Only
	// zero escaping increases segment lengths; account for it without copying
	// a caller-controlled segment or multiplying potentially large lengths.
	remaining := s3MultipartReadKeyByteBudget / count
	overhead := len(s3keys.UploadPartKey("", 0, "", "", 0))
	if overhead > remaining {
		return s3MultipartReadKeyBudgetError()
	}
	remaining -= overhead
	for _, segment := range []string{completion.bucket, completion.objectKey, completion.uploadID} {
		if len(segment) > remaining {
			return s3MultipartReadKeyBudgetError()
		}
		remaining -= len(segment)
		escapes := strings.Count(segment, "\x00")
		if escapes > remaining {
			return s3MultipartReadKeyBudgetError()
		}
		remaining -= escapes
	}
	return nil
}

func s3MultipartReadKeyBudgetError() error {
	return newS3ResponseError(http.StatusBadRequest, "InvalidArgument", "multipart read keys exceed the byte budget", "", "")
}
