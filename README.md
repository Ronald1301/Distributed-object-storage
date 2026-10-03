
# 🥈 P16 — Almacenamiento distribuido de objetos

Una arquitectura posible:

```text
             ┌──────────┐
             │ Cliente  │
             └────┬─────┘
                  │
             ┌────▼─────┐
             │  Server  │
             └────┬─────┘
                  │
        ┌─────────┼─────────┐
        │         │         │
   ┌────▼───┐ ┌───▼────┐ ┌──▼─────┐
   │ Nodo 1 │ │ Nodo 2 │ │ Nodo 3 │
   └────────┘ └────────┘ └────────┘
```

Metodos a implementar primero:

```text
PUT
GET
DELETE
LIST
```

Propiedades a cumplir:

- Replicación.
- Hashing/ubicación.
- Checksums.
- Fallo de nodos.
- Recuperación.
- Reconciliación.

Esto permite empezar con algo sencillo y añadir progresivamente los conceptos de distribución.

---
