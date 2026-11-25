
# P16 · PROYECTO FINAL

## Tema: Servicio distribuido de almacenamiento de objetos

### Descripción

El almacenamiento de objetos presenta una interfaz sencilla de clave, bytes y metadatos, pero oculta problemas de 
ubicación, replicación, integridad, reequilibrado y reparación. Es un proyecto adecuado para separar un contrato de 
aplicación simple de un plano de datos distribuido.

### Objetivo general

Construir un almacén de objetos con operaciones PUT, GET, DELETE, HEAD/metadatos y LIST o equivalentes. Los objetos 
deben tener identificadores estables y metadatos, utilizar de forma significativa varios nodos y conservar las garantías 
declaradas de durabilidad y disponibilidad. Las estrategias de ubicación, redundancia, integridad, metadatos y reparación 
quedan a decisión de los estudiantes.

### Funcionalidades y alcance

* Asignar identificadores estables a los objetos.
* Mantener metadatos asociados.
* Utilizar de forma significativa varios nodos de almacenamiento.
* Definir cuándo una escritura puede confirmarse como durable.
* Detectar o evitar corrupción silenciosa según la política de integridad elegida.
* Definir propiedad y consistencia de metadatos.
* Recuperar un servicio útil después del fallo de un nodo de almacenamiento.
* Reparar o reconciliar el estado distribuido según el contrato declarado.
* Adaptarse a cambios de membresía o capacidad de acuerdo con el diseño elegido.
Referencias para comparación arquitectónica
* Amazon S3: referencia de interfaz y terminología de almacenamiento de objetos.
* Ceph Object Gateway/RADOS: comparación entre API de objetos, ubicación distribuida y replicación.
* MinIO: comparación para almacenamiento de objetos de clúster pequeño con interfaz estilo S3.