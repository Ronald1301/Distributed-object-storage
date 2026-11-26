# Contrato de comportamiento del servicio de objetos (P16)

Qué ve y qué puede esperar un cliente, **independientemente de cómo esté implementado**
el sistema. Es el contrato de la versión de la Fase 1 y la referencia para la versión
nativa final; los códigos HTTP de `native-v1` se dan como equivalencia. Cuando una cifra
es un parámetro, se indica su valor por defecto.

## 1. Modelo

- Un **objeto** es una secuencia de bytes con metadatos, identificada por su nombre
  `(bucket, clave)`. Si no se indica bucket, se usa `default`. Los buckets se crean
  implícitamente con el primer PUT.
- Cada objeto tiene un **identificador estable**, que no cambia al sobrescribirlo. Si se
  borra y se vuelve a crear con el mismo nombre, recibe un identificador nuevo.
- Cada PUT confirmado publica una **versión** nueva: un número que empieza en 1 y sube de
  uno en uno por objeto. Las versiones son inmutables, y solo la última es visible.
- El **ETag** de una versión es el SHA-256 de su contenido.
- Metadatos de un objeto: tamaño, tipo de contenido, ETag, versión, fecha de la última
  modificación y metadatos de usuario (pares clave-valor de texto).

### Nombres válidos

| Elemento | Regla |
|---|---|
| Bucket | 3–63 caracteres de `a-z`, `0-9`, `.` y `-`; empieza y termina con letra o dígito |
| Clave | 1–1024 bytes (UTF-8), sin caracteres de control; puede contener `/` (prefijos tipo carpeta) |

## 2. Parámetros del contrato

| Parámetro | Valor por defecto | Significado |
|---|---|---|
| RF (factor de replicación) | 2 | Copias que el sistema intenta mantener de cada objeto, siempre en nodos distintos |
| W (confirmaciones de escritura) | 2 | Copias persistidas y verificadas que hacen falta para confirmar un PUT |
| Tamaño máximo de objeto | 64 MB | Un PUT mayor se rechaza |
| Ventana de idempotencia | 10 000 peticiones | Cuántos identificadores de petición de PUT recuerda el sistema (`native-v1` no tiene identificador de petición) |
| Página de LIST | 100 por defecto, 1000 como máximo | Objetos por respuesta |

## 3. Operaciones

| Operación | Entrada | Resultado si va bien | Errores posibles |
|---|---|---|---|
| **PUT** | bucket, clave, bytes; opcionales: tipo de contenido, metadatos de usuario, `If-Match`, `If-None-Match`, identificador de petición | Identificador del objeto, versión, ETag, tamaño y si fue **creación** o **sobrescritura** | `ARGUMENTO_INVALIDO`, `DEMASIADO_GRANDE`, `PRECONDICION_FALLIDA`, `CONFLICTO`, `NO_DISPONIBLE` |
| **GET** | bucket, clave; opcional: `If-None-Match` | Bytes y metadatos de la última versión; o `NO_MODIFICADO` si el ETag coincide | `ARGUMENTO_INVALIDO`, `NO_ENCONTRADO`, `NO_DISPONIBLE` |
| **HEAD** | bucket, clave | Solo los metadatos de la última versión | `ARGUMENTO_INVALIDO`, `NO_ENCONTRADO`, `NO_DISPONIBLE` |
| **DELETE** | bucket, clave | Éxito siempre, indicando si el objeto existía | `ARGUMENTO_INVALIDO`, `NO_DISPONIBLE` |
| **LIST** | bucket; opcionales: prefijo, `after` (clave a partir de la cual seguir), límite | Objetos ordenados por clave, cada uno con clave, tamaño, tipo, ETag, versión y fecha; `truncated` y `next_after` para la página siguiente | `ARGUMENTO_INVALIDO`, `NO_DISPONIBLE` |
| **LIST de buckets** | — | Buckets con número de objetos y bytes totales | `NO_DISPONIBLE` |

Precondiciones del PUT (control optimista de concurrencia):

- `If-None-Match: *`: solo escribe si el objeto **no** existe.
- `If-Match: <etag>`: solo escribe si la versión actual tiene ese ETag.
- Se comprueban de forma atómica en el momento de publicar la versión: entre la
  comprobación y la publicación no puede colarse otro PUT.

## 4. Errores

| Código | Cuándo | ¿Reintentar? | HTTP en `native-v1` |
|---|---|---|---|
| `ARGUMENTO_INVALIDO` | Nombre de bucket o clave inválidos | No; corregir la petición | 400 |
| `NO_ENCONTRADO` | GET o HEAD de un objeto que no existe o ya se borró | No | 404 |
| `NO_MODIFICADO` | GET con `If-None-Match` que coincide con la versión actual (no es un fallo) | — | 304 |
| `PRECONDICION_FALLIDA` | Falla `If-Match` o `If-None-Match` | Solo tras releer el objeto | 412 |
| `CONFLICTO` | El objeto se borró mientras se escribía el PUT; o el identificador de petición ya se usó para otro objeto | Sí, el PUT completo (en el segundo caso, con un identificador nuevo) | 409 |
| `DEMASIADO_GRANDE` | El objeto supera el tamaño máximo | No | 413 |
| `NO_DISPONIBLE` | PUT: no hay W nodos que confirmen la escritura. GET: el objeto existe, pero ninguna copia válida es accesible. Cualquier operación: el servicio de metadatos no responde | Sí, más tarde | 503 |

Un error **nunca** deja una versión a medias: si un PUT falla, el objeto sigue con la
versión que tenía antes.

### Resultado desconocido

Si el cliente no recibe respuesta (se corta la conexión, vence un plazo o cae un componente
a mitad de la operación), **no sabe** si la operación se aplicó. No es un código de error,
es una situación que el cliente tiene que tratar:

- **PUT**: reintentar con el **mismo identificador de petición**. Si la primera vez llegó a
  publicarse, el reintento devuelve esa misma versión y no crea otra.
- **DELETE**: reintentar sin más; es idempotente.
- **GET, HEAD y LIST**: reintentar sin más; no cambian nada.

## 5. Garantías

### Durabilidad (W = 2)

Un PUT solo se confirma **después** de que:

1. los bytes están persistidos en **2 nodos distintos**, cada uno habiendo verificado el
   SHA-256 recibido antes de confirmar; y
2. la versión nueva está registrada de forma duradera en los metadatos.

Consecuencia: un objeto confirmado no se pierde mientras sobreviva al menos uno de sus 2
nodos y el almacenamiento de los metadatos. Tampoco se pierde si el servicio de metadatos
se reinicia: lo confirmado se reconstruye desde su registro en disco.

### Atomicidad del PUT

O se ve la versión nueva completa, o la anterior. Nunca un objeto a medias ni una mezcla
de dos versiones. Lo escrito por un PUT que no llegó a confirmarse es invisible.

### Lectura tras escritura

Después de que un PUT se confirma, cualquier GET, HEAD o LIST posterior, de cualquier
cliente, ve esa versión o una más reciente. Nunca devuelve una versión anterior ya
sustituida.

### Integridad

El cliente **nunca** recibe bytes cuyo SHA-256 no coincide con el ETag de la versión. Si
una copia está corrupta o falta, se sirve otra copia válida; si no queda ninguna
accesible, el resultado es `NO_DISPONIBLE`, nunca datos incorrectos.

### Versiones y concurrencia

- Cada PUT confirmado incrementa la versión en 1. La versión y el ETag identifican sin
  ambigüedad lo que se leyó.
- Varios PUT concurrentes al mismo nombre se ordenan por el momento en que se publican:
  **gana el último en publicarse**. Cada uno publica su versión completa y la siguiente la
  sustituye. Para evitar pisar a otro cliente se usa `If-Match`.
- Un GET que coincide con una sobrescritura devuelve la versión anterior completa o la
  nueva completa.

### Borrado

- DELETE es **idempotente**: borrar algo que no existe también es un éxito.
- En cuanto se confirma, GET y HEAD dan `NO_ENCONTRADO` y LIST deja de mostrarlo, para
  todos los clientes.
- El espacio de las copias se libera **después**, de forma asíncrona. Si un nodo estaba
  caído, sus copias se borran cuando vuelve.

### LIST

Refleja solo versiones **confirmadas**: no muestra PUT en curso ni objetos borrados. Está
ordenado por clave y se pagina con `after` y el límite.

### Disponibilidad ante la caída de un nodo de almacenamiento

Con RF = 2, la caída de **un** nodo no impide leer ningún objeto: queda la otra copia. Las
escrituras siguen funcionando mientras haya al menos W nodos disponibles: si un nodo
elegido para un PUT no responde, la copia se escribe en otro.

## 6. Lo que NO se garantiza

- **Exactly-once.** Una operación sin respuesta puede haberse aplicado o no (ver
  "Resultado desconocido").
- **Idempotencia del PUT sin identificador de petición.** Reintentar un PUT confirmado sin
  identificador crea **otra versión** con el mismo contenido (versión + 1).
- **Idempotencia ilimitada.** El sistema solo recuerda las últimas 10 000 peticiones de PUT;
  un reintento posterior crea una versión nueva.
- **Disponibilidad del servicio de metadatos.** Hay un único servicio de metadatos. Mientras
  no responde (por ejemplo, durante un reinicio), todas las operaciones dan `NO_DISPONIBLE`.
  Si se pierde su almacenamiento, se pierden los metadatos, aunque las copias de los datos
  sigan en los nodos. Es un punto único de fallo **declarado** de esta versión.
- **Escrituras con un solo nodo.** Con W = 2, si solo queda un nodo de almacenamiento
  disponible, los PUT fallan con `NO_DISPONIBLE`.
- **Lectura si caen los dos nodos de un objeto.** Ese objeto da `NO_DISPONIBLE` hasta que
  vuelva uno de ellos. No se pierde lo confirmado, salvo que la pérdida de ambos sea
  permanente.
- **Un plazo de reparación.** Si una copia se pierde o se corrompe, el objeto se queda con
  menos de RF copias válidas hasta que se repone, y mientras tanto perder el otro nodo lo
  perdería. El contrato no fija cuándo se repone.
- **Liberación inmediata de espacio.** Tras un DELETE, una sobrescritura o un PUT fallido,
  las copias que sobran ocupan disco hasta que se recolectan.
- **Acceso a versiones anteriores.** Solo se puede leer la última versión.
- **Transacciones entre objetos.** No hay orden ni atomicidad entre operaciones sobre
  claves distintas.
- **LIST como foto fija.** Cada página es consistente, pero si hay cambios entre una página
  y la siguiente, un objeto puede aparecer o desaparecer de las páginas posteriores.
- **Una regla concreta de orden alfabético.** LIST ordena por clave, pero el contrato no
  fija cómo se comparan mayúsculas, acentos o la `ñ`; dos versiones del sistema pueden
  ordenarlas distinto.
- **Autenticación y cifrado.** Fuera del alcance.

## 7. Resumen para el cliente

| Situación | Qué hacer |
|---|---|
| PUT confirmado | Los datos están en 2 nodos y se leerán tal cual desde cualquier cliente |
| PUT sin respuesta | Reintentar con el mismo identificador de petición |
| `NO_DISPONIBLE` | Reintentar más tarde; nada se ha publicado a medias |
| `PRECONDICION_FALLIDA` | Releer el objeto y decidir si volver a escribir |
| `CONFLICTO` en PUT | El objeto se borró mientras tanto: repetir el PUT si se quiere recrear |
| DELETE sin respuesta | Reintentar; es idempotente |
