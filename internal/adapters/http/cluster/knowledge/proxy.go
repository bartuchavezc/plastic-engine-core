package clusterknowledge

import (
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"plastic-engine-core/internal/core/cluster"
)

// Mount registers a reverse proxy that forwards /knowledge/* requests
// to an available search node in the cluster.
func Mount(r chi.Router, coord *cluster.Coordinator, httpClient *http.Client) {
	p := &proxy{coord: coord, client: httpClient}
	r.HandleFunc("/knowledge/*", p.handle)
}

type proxy struct {
	coord  *cluster.Coordinator
	client *http.Client
}

func (p *proxy) handle(w http.ResponseWriter, r *http.Request) {
	nodes, err := p.coord.ListNodes(r.Context())
	if err != nil {
		http.Error(w, "failed to list nodes", http.StatusInternalServerError)
		return
	}

	var targetAddr string
	for _, n := range nodes {
		if n.Role == "search" && n.Status == "ready" {
			targetAddr = n.AdvertiseAddr
			break
		}
	}
	if targetAddr == "" {
		http.Error(w, "no search nodes available", http.StatusServiceUnavailable)
		return
	}

	targetURL := fmt.Sprintf("http://%s%s", targetAddr, r.URL.RequestURI())
	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header.Clone()

	resp, err := p.client.Do(proxyReq)
	if err != nil {
		http.Error(w, "search node unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
