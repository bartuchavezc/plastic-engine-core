# Search Module Implementation Plan

## Resumen Ejecutivo

Motor de búsqueda con:
- **Term Registry** para plasticidad (término → term_id)
- **Postings por term_id** para updates O(1)
- **Edge N-grams** (prefix only) configurables por índice
- **BM25 scoring**
- **gRPC streaming** de resultados

---

## Arquitectura de Índice

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ESTRUCTURA DE ALMACENAMIENTO                         │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ TERM REGISTRY                                                          │ │
│  │                                                                        │ │
│  │ term:{field}:{term} → {term_id, df, created_at}                       │ │
│  │                                                                        │ │
│  │ Ejemplo:                                                               │ │
│  │ term:title:hello → {term_id: "t_7f3a1b2c", df: 5420}                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ INVERTED INDEX (postings por term_id)                                  │ │
│  │                                                                        │ │
│  │ inv:{term_id}:{docID} → {tf, positions}                               │ │
│  │                                                                        │ │
│  │ Ejemplo:                                                               │ │
│  │ inv:t_7f3a1b2c:doc_001 → {tf: 3, positions: [1, 5, 12]}              │ │
│  │ inv:t_7f3a1b2c:doc_002 → {tf: 1, positions: [8]}                     │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ EDGE N-GRAMS (prefix search)                                           │ │
│  │                                                                        │ │
│  │ ngram:{field}:{prefix}:{term_id} → (empty, key-only)                  │ │
│  │                                                                        │ │
│  │ Para "hello" con min_ngram=2, max_ngram=5:                            │ │
│  │ ngram:title:he:t_7f3a1b2c                                             │ │
│  │ ngram:title:hel:t_7f3a1b2c                                            │ │
│  │ ngram:title:hell:t_7f3a1b2c                                           │ │
│  │ ngram:title:hello:t_7f3a1b2c                                          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ FORWARD INDEX (existente)                                              │ │
│  │                                                                        │ │
│  │ fwd:{docID} → {fields: {title: {tokens: [...]}}}                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ METADATA                                                               │ │
│  │                                                                        │ │
│  │ meta:doc_count → N                                                    │ │
│  │ meta:avg_doc_len:{field} → float                                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuración de N-grams por Índice

```go
type NgramConfig struct {
    Enabled   bool `json:"enabled"`      // Default: true
    MinLength int  `json:"min_length"`   // Default: 2
    MaxLength int  `json:"max_length"`   // Default: 10
}
```

**API para crear índice:**

```json
POST /indexes
{
  "name": "products",
  "ngram_config": {
    "enabled": true,
    "min_length": 3,
    "max_length": 8
  }
}
```

---

## Term ID Generation

**Estrategia: Hash determinístico (SHA256 truncado)**

```go
func GenerateTermID(field, term string) string {
    input := field + "\x00" + term
    hash := sha256.Sum256([]byte(input))
    return "t_" + hex.EncodeToString(hash[:8])
}
```

---

## Flujos de Operación

### Indexación de Documento

1. **Tokenize** documento
2. **Get/Create Term Registry** para cada token
3. **Write Posting** (inv:{term_id}:{docID})
4. **Write Edge N-grams** (ngram:{field}:{prefix}:{term_id})
5. **Update Forward Index**
6. **Update Metadata** (doc_count, avg_doc_len)

### Búsqueda Exacta (Match Query)

1. **Tokenize** query
2. **Lookup Term Registry** → term_id + df
3. **Load Scoring Context** (doc_count, avg_doc_len)
4. **Scan Postings** + calculate BM25 score
5. **Return Top-K**

### Búsqueda Prefix

1. **Validate** prefix length >= min_ngram
2. **Scan N-gram Index** (ngram:{field}:{prefix}:*)
3. **Collect term_ids**
4. **For each term_id**: scan postings + score
5. **Return Top-K**

---

## BM25 Scoring

```go
type BM25Config struct {
    K1 float64  // Term frequency saturation, default: 1.2
    B  float64  // Length normalization, default: 0.75
}

// IDF = log(1 + (N - df + 0.5) / (df + 0.5))
// TF_norm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgdl)))
// Score = IDF * TF_norm
```

---

## Streaming Architecture

```
┌──────────┐    gRPC Server     ┌──────────────┐    gRPC Bidir    ┌──────────┐
│  Search  │◄──  Streaming  ───►│   Stream     │◄────(futuro)────►│   LLM    │
│  Nodes   │                    │   Proxy      │                  │  Client  │
└──────────┘                    └──────────────┘                  └──────────┘

Interno:                         Externo:
- Unidireccional                 - Bidireccional (futuro)
- Context cancellation           - Refinamiento mid-stream
```

---

## Fases de Implementación

| Fase | Tarea | Tiempo |
|------|-------|--------|
| 1 | Storage Layer (PrefixScan) | 1h |
| 2 | Key Codec | 30m |
| 3 | Term Registry | 45m |
| 4 | Edge N-gram Generator | 20m |
| 5 | Index Writer Updates | 1.5h |
| 6 | BM25 Scorer | 30m |
| 7 | Prefix Query Type | 15m |
| 8 | Query Executor | 2h |
| 9 | Search Service + gRPC | 1.5h |
| 10 | Result Merger | 45m |
| 11 | Index Definition Update | 20m |
| 12 | Tests | 2h |
| **Total** | | **~12h** |

---

## API de Búsqueda

### Match Query

```json
POST /search
{
  "index_id": "products",
  "query": {
    "match": {"field": "title", "value": "hello world"}
  },
  "limit": 20
}
```

### Prefix Query

```json
POST /search
{
  "index_id": "products",
  "query": {
    "prefix": {"field": "title", "value": "hel"}
  },
  "limit": 20
}
```

### Response

```json
{
  "hits": [
    {"doc_id": "doc1", "shard_id": "shard_001", "score": 4.23}
  ],
  "total": 1542,
  "cursor": "eyJ..."
}
```

