package httputil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// WriteJSON writes the provided payload as JSON with the supplied status code.
func WriteJSON(w http.ResponseWriter, status int, payload any) {
	buffer := &bytes.Buffer{}
	if err := json.NewEncoder(buffer).Encode(payload); err != nil {
		http.Error(w, fmt.Sprintf("encode response: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buffer.Bytes())
}
