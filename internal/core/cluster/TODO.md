# TODO: Resolver estructura de paquetes en cluster

## Problema Detectado

Durante la reorganización, se identificó que los archivos del coordinator están muy acoplados entre sí:

- `coordinator.go` llama directamente a funciones de:
  - `shards/manager.go` → `planInitialShards()`, `assignPendingShardsToReadyNodes()`
  - `nodes/join.go` → `Join()`, `upsertNode()`
  - `nodes/heartbeat.go` → `Heartbeat()`, `updateNodeHeartbeat()`
  - `ingest/handler.go` → `Handler`, `Request`

Si están en paquetes separados (`cluster`, `nodes`, `shards`, `ingest`), hay riesgo de:
1. Imports circulares
2. Acoplamiento confuso entre paquetes

## Estado Actual

Los archivos fueron movidos físicamente pero todos tienen `package cluster` temporalmente para evitar errores de compilación:

```
cluster/
├── coordinator.go      # package cluster
├── db.go               # package cluster
├── admin.go            # package cluster
├── nodes/
│   ├── join.go         # package nodes (pero debería ser cluster?)
│   ├── heartbeat.go    # package nodes
│   ├── state.go        # package nodes
│   └── join_service.go # package nodes
├── shards/
│   ├── manager.go      # package shards (pero tiene funciones de coordinator)
│   ├── assignment.go   # package shards
│   └── lookup.go       # package shards
├── ingest/
│   ├── handler.go      # package ingest
│   └── normalizer.go   # package ingest
└── indexes/            # ✅ Este está bien separado
```

## Opciones a Evaluar

### Opción 1: Archivos planos en cluster/ (Pragmática)
Todos los archivos en `cluster/` con nombres descriptivos:
- `coordinator.go`
- `nodes_join.go`, `nodes_heartbeat.go`
- `shards_manager.go`, `shards_assignment.go`
- `ingest_handler.go`

**Pros:** Simple, un solo paquete, cero riesgo de imports circulares
**Cons:** Carpeta grande, menos organización visual

### Opción 2: Subdirectorios como organización visual (mismo paquete)
Archivos en subdirectorios pero todos con `package cluster`:
```
cluster/
├── nodes/join.go          # package cluster
├── shards/manager.go      # package cluster
```
**Pros:** Organizado visualmente
**Cons:** Confuso (Go no soporta esto nativamente), requiere build tags o tricks

### Opción 3: Refactorizar dependencias (Bounded Contexts puros)
Invertir dependencias usando interfaces:
- `Coordinator` define interfaces que `nodes`, `shards`, `ingest` implementan
- Inyección de dependencias en `NewCoordinator()`

**Pros:** Bounded contexts puros, testeable
**Cons:** Más trabajo, posible over-engineering para este caso

## Decisión Pendiente

Revisar después de completar la reorganización de `search/` para tener perspectiva completa.

## Problemas Adicionales Detectados

Durante la compilación se encontraron ciclos de imports:
- `cluster/ingest/handler.go` importa `cluster/shards`
- `cluster/shards/manager.go` importa `cluster/shards` (a sí mismo)

Los archivos tienen `package cluster` pero están en subdirectorios, lo cual Go no maneja bien.

## Acción Inmediata

**Opción recomendada:** Mover todos los archivos de subdirectorios a `cluster/` con nombres descriptivos:
```bash
mv cluster/nodes/join.go cluster/nodes_join.go
mv cluster/nodes/heartbeat.go cluster/nodes_heartbeat.go
mv cluster/shards/manager.go cluster/shards_manager.go
# etc.
```

Esto mantiene la organización visual en los nombres de archivo pero evita problemas de paquetes.

