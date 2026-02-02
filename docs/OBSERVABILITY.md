# Observabilidad en Plastic Engine

Este documento describe la estrategia de observabilidad implementada en Plastic Engine, incluyendo trazas distribuidas, métricas y logging estructurado.

## Arquitectura de Observabilidad

```
┌─────────────────────────────────────────────────────────────────┐
│                        Plastic Engine                            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │ Coordinator │    │ Search Node │    │ Search Node │         │
│  │   :8080     │    │   :9090     │    │   :9091     │         │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘         │
│         │                  │                  │                 │
│         └──────────────────┼──────────────────┘                 │
│                            │                                    │
│         ┌──────────────────┼──────────────────┐                 │
│         │                  │                  │                 │
│         ▼                  ▼                  ▼                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Traces    │    │   Metrics   │    │    Logs     │         │
│  │  (OTLP)     │    │ (Prometheus)│    │ (Structured)│         │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘         │
└─────────┼──────────────────┼──────────────────┼─────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │   Jaeger/   │    │  Prometheus │    │    Loki     │
   │   Tempo     │    │  + Grafana  │    │  + Grafana  │
   └─────────────┘    └─────────────┘    └─────────────┘
```

## Variables de Entorno

### Tracing

| Variable | Descripción | Default |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint del collector OTLP | `localhost:4317` |
| `OTEL_EXPORTER_OTLP_INSECURE` | Deshabilitar TLS | `true` |
| `OTEL_TRACES_SAMPLER_ARG` | Ratio de sampling (0.0-1.0) | `1.0` |
| `OTEL_SDK_DISABLED` | Deshabilitar tracing | `false` |
| `SERVICE_VERSION` | Versión del servicio | `0.0.0` |
| `ENVIRONMENT` | Ambiente (production/staging/dev) | `development` |

### Métricas

| Variable | Descripción | Default |
|----------|-------------|---------|
| `ENABLE_PROMETHEUS` | Habilitar endpoint Prometheus | `true` |
| `METRICS_PORT` | Puerto para el endpoint /metrics | `9091` |

## Trazas Distribuidas

### Spans Automáticos

El middleware de observabilidad crea spans automáticos para cada request HTTP con los siguientes atributos:

- `http.method` - Método HTTP (GET, POST, etc.)
- `http.url` - URL completa del request
- `http.target` - Path del request
- `http.status_code` - Código de respuesta
- `http.response_size` - Tamaño de la respuesta en bytes
- `http.duration_ms` - Duración en milisegundos
- `user_agent.original` - User agent del cliente

### Spans de Negocio

| Operación | Span Name | Atributos |
|-----------|-----------|-----------|
| Join de nodo | `nodes.Service.Join` | `node.role`, `node.advertise_addr` |
| Heartbeat | `nodes.Service.Heartbeat` | `node.id`, `node.shards.count` |
| Crear índice | `Coordinator.CreateIndex` | `index_id` |
| Ingestar documento | `Router.Handle` | `index_id`, `shard_id`, `node_id` |
| Búsqueda | `cluster.search` | `search.index_id`, `search.limit` |

### Propagación de Contexto

Plastic Engine propaga el contexto de traza usando W3C TraceContext:
- Header `traceparent` para trace/span IDs
- Header `baggage` para metadata adicional

## Métricas

### HTTP Metrics

| Métrica | Tipo | Labels | Descripción |
|---------|------|--------|-------------|
| `http_requests_total` | Counter | method, path, status_code, status_class | Total de requests HTTP |
| `http_request_duration_seconds` | Histogram | method, path, status_code | Latencia de requests |
| `http_response_size_bytes_total` | Counter | method, path, status_code | Bytes enviados |
| `http_active_requests` | UpDownCounter | method, path | Requests en progreso |

### Business Metrics

| Métrica | Tipo | Labels | Descripción |
|---------|------|--------|-------------|
| `plastic_documents_ingested_total` | Counter | index_id | Documentos ingestados |
| `plastic_shards_assigned_total` | Counter | node_id, index_id | Shards asignados |
| `plastic_nodes_joined_total` | Counter | role | Nodos que hicieron join |
| `plastic_search_queries_total` | Counter | index_id | Búsquedas ejecutadas |

### System Metrics (Go Runtime)

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `go_memstats_alloc_bytes` | Gauge | Bytes de memoria heap en uso |
| `go_goroutines` | Gauge | Número de goroutines activas |
| `go_gc_duration_seconds` | Summary | Duración de pausas de GC |
| `process_cpu_seconds_total` | Counter | Segundos de CPU consumidos |
| `process_resident_memory_bytes` | Gauge | Memoria residente (RSS) |
| `process_open_fds` | Gauge | File descriptors abiertos |

#### Queries útiles para System Metrics

```promql
# Uso de CPU (ratio por minuto)
rate(process_cpu_seconds_total[1m])

# Memoria heap en uso (MB)
go_memstats_alloc_bytes / 1024 / 1024

# Número de goroutines (detectar leaks)
go_goroutines

# Tasa de GC por minuto
rate(go_gc_duration_seconds_count[1m])
```

### Buckets de Latencia

El histograma de latencias usa los siguientes buckets (en segundos):
```
0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
```

## Logging Estructurado

### Formato de Logs

```
[LEVEL] message key=value key2=value2 trace_id=xxx span_id=yyy
```

Ejemplo:
```
[INFO] HTTP request completed method=POST path=/indexes/idx-1/documents status=202 duration_ms=45 size=0 trace_id=abc123 span_id=def456
```

### Correlación con Traces

Los logs automáticamente incluyen `trace_id` y `span_id` cuando están disponibles, permitiendo correlación con las trazas en Jaeger/Tempo.

Para incluir trace context en logs custom:
```go
// Usando métodos con contexto
log.InfoCtx(ctx, "operación completada", logger.Field{Key: "result", Value: "ok"})

// O manualmente
log.Info("operación completada",
    logger.TraceField(ctx),
    logger.SpanField(ctx),
    logger.Field{Key: "result", Value: "ok"},
)
```

## Endpoints de Observabilidad

| Endpoint | Puerto | Descripción |
|----------|--------|-------------|
| `/health` | App port | Health check básico |
| `/ready` | App port | Readiness check |
| `/metrics` | 9091 (default) | Métricas Prometheus |
| `/debug/pprof/*` | App port | Profiling (solo en debug) |

## Alertas Recomendadas

### SLIs/SLOs

| SLI | SLO Target | Expresión PromQL |
|-----|------------|------------------|
| Disponibilidad | 99.9% | `sum(rate(http_requests_total{status_class!="5xx"}[5m])) / sum(rate(http_requests_total[5m]))` |
| Latencia P99 | < 500ms | `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))` |
| Error rate | < 0.1% | `sum(rate(http_requests_total{status_class="5xx"}[5m])) / sum(rate(http_requests_total[5m]))` |

### Alertas Críticas

```yaml
# Ejemplo para Prometheus Alertmanager
groups:
  - name: plastic-engine
    rules:
      - alert: HighErrorRate
        expr: |
          sum(rate(http_requests_total{status_class="5xx"}[5m])) 
          / sum(rate(http_requests_total[5m])) > 0.01
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate detected"
          
      - alert: HighLatency
        expr: |
          histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P99 latency above 1s"
          
      - alert: NoHeartbeats
        expr: |
          increase(plastic_nodes_joined_total[5m]) == 0
          and sum(plastic_shards_assigned_total) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "No new nodes joining but shards exist"

      - alert: HighGoroutineCount
        expr: go_goroutines > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High goroutine count - possible goroutine leak"

      - alert: HighMemoryUsage
        expr: |
          go_memstats_alloc_bytes / 1024 / 1024 > 500
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Heap memory usage above 500MB"

      - alert: HighCPUUsage
        expr: rate(process_cpu_seconds_total[1m]) > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "CPU usage above 80%"
```

## Dashboards Grafana

### Panel: Request Rate
```
sum(rate(http_requests_total[1m])) by (service, method, path)
```

### Panel: Error Rate
```
sum(rate(http_requests_total{status_class="5xx"}[5m])) by (service) 
/ sum(rate(http_requests_total[5m])) by (service)
```

### Panel: Latency Distribution
```
histogram_quantile(0.50, rate(http_request_duration_seconds_bucket[5m]))
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
```

### Panel: Active Shards per Node
```
sum(plastic_shards_assigned_total) by (node_id)
```

### Panel: Goroutines
```
go_goroutines
```

### Panel: Memory Usage (MB)
```
go_memstats_alloc_bytes / 1024 / 1024
```

### Panel: CPU Usage
```
rate(process_cpu_seconds_total[1m])
```

## Debugging con Traces

### Encontrar requests lentos
1. En Jaeger/Tempo, buscar traces con `http.duration_ms > 1000`
2. Examinar la cascada de spans para identificar el cuello de botella

### Correlacionar errores
1. Buscar logs con `status=5xx`
2. Usar el `trace_id` para encontrar la traza completa
3. Revisar spans con status `ERROR`

### Seguir un documento
1. Buscar trace con span `Router.Handle` y atributo `document_id=xxx`
2. Seguir la propagación hacia el nodo search
3. Verificar el span de indexación local

## Configuración de Producción

### Sampling Recomendado
```bash
# 10% sampling para alto volumen
export OTEL_TRACES_SAMPLER_ARG=0.1

# 100% para desarrollo/staging
export OTEL_TRACES_SAMPLER_ARG=1.0
```

### Stack Recomendado
- **Traces**: Jaeger o Grafana Tempo
- **Métricas**: Prometheus + Grafana
- **Logs**: Loki + Grafana (para correlación)

### Docker Compose de Desarrollo
```yaml
version: '3.8'
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"  # UI
      - "4317:4317"    # OTLP gRPC
      
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
```

