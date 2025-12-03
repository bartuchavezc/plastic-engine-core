# Stream Proxy Module

> 🔮 **Estado:** Planificado - No implementado

## Propósito

El módulo `stream_proxy` actúa como **multiplexor de streams gRPC** entre los nodos 
de búsqueda y los clientes. Permite que búsquedas distribuidas retornen resultados 
de forma incremental conforme cada shard responde, en lugar de esperar a que todos 
los nodos completen.

## Arquitectura

```
                    ┌─────────────────────────────────┐
                    │         Stream Proxy            │
                    │    (puede correr en coordinator │
                    │     o como servicio separado)   │
                    └───────────────┬─────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
    │  Search Node 1  │   │  Search Node 2  │   │  Search Node N  │
    │   (shards A,B)  │   │   (shards C,D)  │   │   (shards ...)  │
    └─────────────────┘   └─────────────────┘   └─────────────────┘
              │                     │                     │
              └─────────────────────┴─────────────────────┘
                                    │
                                    ▼
                              ┌───────────┐
                              │  Cliente  │
                              │  (gRPC)   │
                              └───────────┘
```

## Responsabilidades

- **Fan-out:** Distribuir queries a todos los shards relevantes en paralelo
- **Multiplexing:** Combinar streams de respuesta de múltiples nodos
- **Ordering:** Mantener orden por score/timestamp según el tipo de query
- **Backpressure:** Controlar flujo cuando el cliente consume lento
- **Cancellation:** Propagar cancelaciones a todos los nodos participantes
- **Partial Results:** Entregar resultados parciales si un nodo falla

## Dominios Planificados

```
stream_proxy/
├── proxy.go          # Orquestador principal del proxy
├── fanout.go         # Distribución de queries a nodos
├── merger.go         # Merge de streams ordenados (heap-based)
├── backpressure.go   # Control de flujo adaptativo
├── circuit.go        # Circuit breaker por nodo
└── proto/            # Definiciones gRPC
    └── search.proto
```

## Protocolo gRPC Conceptual

```protobuf
syntax = "proto3";
package plastic.search.v1;

service SearchService {
  // Búsqueda con streaming de resultados
  rpc Search(SearchRequest) returns (stream SearchHit);
  
  // Búsqueda con scroll para paginación profunda
  rpc Scroll(ScrollRequest) returns (stream SearchHit);
}

message SearchRequest {
  string index_id = 1;
  string query = 2;
  int32 limit = 3;
  repeated string shards = 4;  // opcional: shards específicos
}

message SearchHit {
  string document_id = 1;
  string shard_id = 2;
  float score = 3;
  bytes source = 4;  // documento serializado
  bool is_last = 5;  // marca fin del stream
}
```

## Flujo de Streaming

```
Cliente              Stream Proxy           Node 1           Node 2
   │                      │                   │                 │
   │──SearchRequest──────▶│                   │                 │
   │                      │──FanOut Query────▶│                 │
   │                      │──FanOut Query────────────────────▶│
   │                      │                   │                 │
   │                      │◀──Hit(score=0.9)──│                 │
   │◀──Hit(score=0.9)─────│                   │                 │
   │                      │◀──Hit(score=0.85)────────────────│
   │◀──Hit(score=0.85)────│                   │                 │
   │                      │◀──Hit(score=0.7)──│                 │
   │                      │◀──Hit(score=0.6)─────────────────│
   │◀──Hit(score=0.7)─────│  (merger ordena)  │                 │
   │◀──Hit(score=0.6)─────│                   │                 │
   │                      │◀──EOF─────────────│                 │
   │                      │◀──EOF────────────────────────────│
   │◀──EOF────────────────│                   │                 │
```

## Estrategias de Merge

| Estrategia | Uso | Descripción |
|------------|-----|-------------|
| **Score-ordered** | Relevance search | Min-heap por score descendente |
| **Time-ordered** | Log search | Min-heap por timestamp |
| **Round-robin** | Scan/export | Alterna entre nodos |
| **First-available** | Low latency | Emite apenas llega |

## Consideraciones

- **Timeouts:** Timeout global + timeout por nodo
- **Partial failure:** Retornar resultados parciales con metadata de errores
- **Load balancing:** Preferir nodos con menor latencia histórica
- **Compression:** gRPC soporta compresión nativa (gzip, snappy)

## Dependencias

- `cluster/shards`: Resolución de qué nodos tienen qué shards
- `search/query`: Ejecución de queries en nodos individuales
- `adapters/grpc`: Implementación del servidor gRPC

## Referencias

- [gRPC Bidirectional Streaming](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc)
- [Elasticsearch Scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html)
- [Merge-sort for distributed queries](https://en.wikipedia.org/wiki/External_sorting#External_merge_sort)

---

*Este módulo se implementará después de tener el query executor funcionando en los search nodes.*

