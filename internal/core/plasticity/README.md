# Plasticity Module

> 🔮 **Estado:** Planificado - No implementado

## Propósito

El módulo `plasticity` gestiona la **evolución controlada de índices** sin downtime. 
Permite modificar mappings, analyzers y estrategias de sharding de forma incremental 
mientras el sistema continúa sirviendo búsquedas.

## Concepto

El nombre "plástico" viene de la capacidad del motor de **adaptarse y cambiar de forma** 
sin romperse, similar a la plasticidad neuronal donde las conexiones se reorganizan 
manteniendo la funcionalidad.

## Responsabilidades

- **Proposals:** Definir cambios pendientes en la estructura de un índice
- **Migrations:** Ejecutar transformaciones de datos de forma incremental
- **Versioning:** Mantener múltiples versiones de mappings durante transiciones
- **Rollback:** Revertir cambios fallidos sin pérdida de datos

## Dominios Planificados

```
plasticity/
├── proposal.go      # Definición de cambios (add field, change analyzer, reshard)
├── migration.go     # Orquestación de migraciones incrementales
├── versioning.go    # Control de versiones de mappings
├── validator.go     # Validación de compatibilidad entre versiones
└── rollback.go      # Estrategias de rollback
```

## Flujo Conceptual

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Proposal   │────▶│  Validation  │────▶│  Migration  │
│  (pending)  │     │  (approved)  │     │  (running)  │
└─────────────┘     └──────────────┘     └──────┬──────┘
                                                │
                    ┌──────────────┐            │
                    │   Rollback   │◀───────────┤ (on failure)
                    │  (reverted)  │            │
                    └──────────────┘            ▼
                                         ┌─────────────┐
                                         │  Completed  │
                                         │  (applied)  │
                                         └─────────────┘
```

## Casos de Uso

1. **Agregar campo nuevo:** Backfill opcional, índice sigue operativo
2. **Cambiar analyzer:** Re-indexación en background, queries usan versión anterior
3. **Modificar estrategia de sharding:** Redistribución gradual de documentos
4. **Eliminar campo:** Marcado como deprecated, limpieza diferida

## Dependencias

- `cluster/indexes`: Definiciones de índices actuales
- `cluster/shards`: Coordinación de re-sharding
- `search/document`: Re-indexación de documentos

## Referencias

- [Elasticsearch Index Lifecycle Management](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-lifecycle-management.html)
- [Zero-downtime reindexing patterns](https://www.elastic.co/blog/changing-mapping-with-zero-downtime)

---

*Este módulo se implementará después de estabilizar el pipeline de indexación básico.*

