# Plastic Engine - TODO

Este documento lista las mejoras pendientes organizadas por prioridad.

## Rendimiento y Escalabilidad

### Alta Prioridad

- [ ] **Batching de documentos** (`internal/core/search/document/worker.go`)
  - Actualmente: 1 escritura por documento
  - Objetivo: Acumular N docs o T tiempo antes de flush
  - Impacto: ~4x mejora en throughput

- [ ] **Circuit breaker para nodos** (`internal/core/cluster/documents/router.go`)
  - Actualmente: Sin protección, un nodo lento bloquea todo
  - Objetivo: Implementar circuit breaker con exponential backoff
  - Impacto: Resiliencia ante fallos parciales

### Media Prioridad

- [ ] **enrichAssignments en batch** (`internal/core/cluster/nodes/service.go`)
  - Actualmente: O(N) queries para N indexes
  - Objetivo: Query batch `WHERE id IN (...)`
  - Impacto: Reduce latencia en joins con muchos shards

- [ ] **Timeouts configurables por operación**
  - Actualmente: Timeouts hardcodeados
  - Objetivo: Configuración via environment variables
  - Impacto: Mejor control operacional

### Baja Prioridad (para escala mayor)

- [ ] **gRPC streaming para ingesta**
  - Actualmente: HTTP síncrono
  - Objetivo: Streaming bidireccional con backpressure
  - Impacto: Mayor throughput, mejor manejo de carga

- [ ] **Control de compactación Pebble**
  - Actualmente: Configuración por defecto
  - Objetivo: Tunear según workload (write-heavy vs read-heavy)
  - Impacto: Mejor uso de recursos

## Funcionalidad

### En Progreso

- [ ] **Módulo de Mappings independiente**
  - Separar mappings de indexes
  - Dynamic mapping (inferencia de tipos)
  - Versionado de mappings para plasticidad

### Pendiente

- [ ] **Search Query Module**
  - Parser de queries
  - Ejecución distribuida
  - Agregaciones básicas

- [ ] **Stream Proxy Module**
  - Fan-out de queries a nodos
  - Merge de resultados
  - Streaming de resultados al cliente

- [ ] **Plasticity Module**
  - Cambios de mapping controlados
  - Resharding
  - Migraciones de datos

## Operacional

- [ ] **TLS entre nodos Raft**
- [ ] **Autenticación HTTP**
- [ ] **Rate limiting**
- [ ] **Backup/Restore automatizado**

## Documentación

- [x] `docs/OBSERVABILITY.md` - Trazas y métricas
- [x] `docs/COORDINATOR.md` - Arquitectura del coordinator
- [ ] `docs/SEARCH.md` - Arquitectura de nodos de búsqueda
- [ ] `docs/API.md` - Referencia completa de la API

