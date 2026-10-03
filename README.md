
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

Pueden implementar primero:

```text
PUT
GET
DELETE
LIST
```

Y después añadir:

- Replicación.
- Hashing/ubicación.
- Checksums.
- Fallo de nodos.
- Recuperación.
- Reconciliación.

Esto permite empezar con algo sencillo y añadir progresivamente los conceptos de distribución.

---
