package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"

	"plastic-engine-core/internal/pkg/logger"
)

type WhoamiResponse struct {
	Role        string `json:"role"`
	Port        string `json:"port"`
	JoinAddress string `json:"join_address"`
}

func main() {
	log := logger.DefaultLogger()

	router := chi.NewRouter()
	role := os.Getenv("ROLE")
	if role == "" {
		role = "unknown"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	joinAddress := os.Getenv("JOIN_ADDRESS")
	router.Get("/whoami", func(w http.ResponseWriter, r *http.Request) {
		response := WhoamiResponse{
			Role:        role,
			Port:        fmt.Sprintf(":%s", port),
			JoinAddress: joinAddress,
		}
		err := json.NewEncoder(w).Encode(response)
		if err != nil {
			log.Error("error writing response", logger.Field{Key: "error", Value: err})
		}
	})

	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), router); err != nil {
		log.Error("server failed", logger.Field{Key: "error", Value: err})
		os.Exit(1)
	}
}
