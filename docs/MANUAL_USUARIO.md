# Manual de usuario: P16 almacenamiento distribuido de objetos

## 1. Que necesitas

- Docker Engine o Docker Desktop con Docker Compose v2.
- Python 3.10 o posterior en el host para la CLI, guiones y pruebas.
- Git para clonar el repositorio.
- Puerto 8080 libre.

No necesitas instalar PostgreSQL en el host. Docker Compose levanta PostgreSQL 17, el gateway y los Storage Nodes.

## 2. Descargar el proyecto

```bash
git clone https://github.com/Ronald1301/Distributed-object-storage.git
cd Distributed-object-storage
```

## 3. Iniciar y comprobar el sistema

```bash
docker compose up -d --build --wait
docker compose ps
curl http://localhost:8080/health
python3 scripts/p16.py status
```

Espera a que los servicios aparezcan como sanos. El gateway escucha en el puerto 8080.

Abrir la interfaz visual:

```bash
python3 scripts/p16.py watch
```

Tambien puedes abrir `http://localhost:8080/ui` en el navegador. La documentacion interactiva de la API esta en `http://localhost:8080/docs`.

## 4. Subir, descargar, listar y borrar objetos

La CLI es la forma mas sencilla de usar el proyecto. La forma general es:

```bash
python3 scripts/p16.py [--bucket NOMBRE] COMANDO [argumentos]
```

El bucket por defecto se llama `default`. Por ejemplo:

```bash
python3 scripts/p16.py put foto.jpg fotos/foto.jpg
python3 scripts/p16.py head fotos/foto.jpg
python3 scripts/p16.py get fotos/foto.jpg copia.jpg
python3 scripts/p16.py verify foto.jpg fotos/foto.jpg
python3 scripts/p16.py ls fotos/
python3 scripts/p16.py rm fotos/foto.jpg
```

Para usar otro bucket:

```bash
python3 scripts/p16.py --bucket trabajo put notas.txt notas.txt
python3 scripts/p16.py --bucket trabajo ls
```

Otros comandos utiles:

```bash
python3 scripts/p16.py status
python3 scripts/p16.py events 30
python3 scripts/p16.py locate fotos/foto.jpg
python3 scripts/p16.py seed 20
python3 scripts/p16.py -h
```

- `status`: salud de nodos, replicas deseadas y reales, y objetos degradados.
- `events`: fallos, reparaciones, corrupciones y limpieza.
- `locate`: nodos que HRW selecciona para la clave y replicas registradas.
- `seed`: crea objetos de texto para practicar.

Para cambiar la URL del gateway, define `P16_URL`:

```bash
P16_URL=http://localhost:8080 python3 scripts/p16.py status
```

## 5. Usar la API con curl

Las claves pueden incluir subrutas. `/objects/...` apunta al bucket `default`.

```bash
# Subir un objeto
curl -X PUT --data-binary @foto.jpg http://localhost:8080/objects/fotos/foto.jpg

# Descargarlo
curl -o copia.jpg http://localhost:8080/objects/fotos/foto.jpg

# Consultar sus metadatos
curl -I http://localhost:8080/objects/fotos/foto.jpg

# Listar objetos
curl "http://localhost:8080/objects?prefix=fotos/&limit=50"

# Listar buckets
curl http://localhost:8080/buckets

# Subir a un bucket distinto
curl -X PUT --data-binary @notas.txt   http://localhost:8080/buckets/trabajo/objects/notas.txt

# Borrar
curl -X DELETE http://localhost:8080/objects/fotos/foto.jpg
```

Para enviar tipo de contenido y metadatos personalizados:

```bash
curl -X PUT   -H "Content-Type: image/jpeg"   -H "X-Meta-Autor: ana"   --data-binary @foto.jpg   http://localhost:8080/objects/fotos/foto.jpg
```

Para crear una clave solo si no existe, usa `If-None-Match: *`. Para reemplazar solo una version concreta, puedes usar `If-Match` con el ETag devuelto por GET o HEAD.

## 6. Ver el cluster

```bash
curl http://localhost:8080/cluster/nodes
curl http://localhost:8080/cluster/distribution
curl "http://localhost:8080/cluster/locate?key=fotos/foto.jpg"
curl "http://localhost:8080/cluster/events?limit=20"
curl http://localhost:8080/cluster/config
```

La interfaz `/ui` muestra solicitudes y eventos en vivo, el gateway, PostgreSQL, el estado de los nodos y las replicas. Usa la CLI `watch --terminal` si prefieres observarlo desde la terminal:

```bash
python3 scripts/p16.py watch --terminal
```

## 7. Practicar fallos y recuperacion

Ejecuta estos comandos solo en el entorno de desarrollo:

```bash
# Detener un nodo y esperar a que se detecte
docker compose stop storage2
python3 scripts/p16.py wait storage2 DOWN

# Probar una lectura: puede servirse desde la otra replica valida
python3 scripts/p16.py get fotos/foto.jpg copia.jpg

# Volver a arrancar el nodo
docker compose start storage2
python3 scripts/p16.py wait storage2 HEALTHY
python3 scripts/p16.py wait converged
```

Simular corrupcion y comprobar la reparacion:

```bash
python3 scripts/p16.py seed 3 demo/
python3 scripts/p16.py corrupt demo/objeto-001.txt
python3 scripts/p16.py get demo/objeto-001.txt recuperado.txt
python3 scripts/p16.py events 30
```

Ejecutar reparacion o verificacion de todas las replicas bajo demanda:

```bash
python3 scripts/p16.py reconcile
python3 scripts/p16.py scrub
python3 scripts/p16.py wait converged
```

Los tiempos de reparacion y sondeo se pueden reducir para una demo. La guia de comandos contiene los valores y escenarios completos.

## 8. Agregar y retirar un nodo

El cuarto nodo esta bajo el perfil `scale`:

```bash
docker compose --profile scale up -d --wait storage4
python3 scripts/p16.py wait converged
python3 scripts/p16.py status
```

Para retirarlo ordenadamente, primero mueve sus replicas:

```bash
python3 scripts/p16.py drain storage4
python3 scripts/p16.py wait storage4 DECOMMISSIONED
docker compose --profile scale stop storage4
```

Para cambiar la capacidad relativa de un nodo:

```bash
python3 scripts/p16.py weight storage3 2
python3 scripts/p16.py wait converged
```

El peso 2 indica que el nodo tiene el doble de capacidad relativa que un nodo con peso 1.

## 9. Ejecutar guiones de demo

El repositorio incluye guiones en `escenarios/`. Por ejemplo:

```bash
python3 scripts/p16.py run escenarios/archivo.txt ~/Desktop/foto.jpg
python3 scripts/p16.py run escenarios/caida-nodo.txt ~/Desktop/foto.jpg
python3 scripts/p16.py run escenarios/membresia.txt
python3 scripts/p16.py run escenarios/demo-completa.txt
```

Para ejecutar sin pausas interactivas, añade `--yes`:

```bash
python3 scripts/p16.py run escenarios/demo-completa.txt --yes
```

Tambien hay una demo guiada:

```bash
FAST=1 ./scripts/demo.sh
```

## 10. Detener y conservar los datos

```bash
docker compose down
```

Esto detiene los contenedores y conserva los volumenes. Para empezar desde cero y borrar los datos:

```bash
docker compose --profile scale down -v
```

**Atencion:** `down -v` borra los volumenes de PostgreSQL y los Storage Nodes.

## 11. Problemas comunes

- **El puerto 8080 esta ocupado:** detén el otro servicio o cambia el mapeo en `docker-compose.yml`.
- **La CLI no conecta:** revisa `docker compose ps`, `docker compose logs gateway` y `curl http://localhost:8080/health`.
- **Un servicio no arranca:** consulta `docker compose logs NOMBRE_SERVICIO`.
- **Los objetos aparecen degradados:** espera el ciclo de reconciliacion y revisa `python3 scripts/p16.py events 30`.
- **Docker pide permisos:** en Linux usa `sudo docker ...` o configura el acceso del usuario siguiendo la documentacion de Docker.

## 12. Mas informacion

- `README.md`: resumen y garantia del sistema.
- `docs/INFORME_TECNICO.md`: arquitectura, consistencia, ubicacion, reparacion y middleware.
- `docs/COMANDOS.md`: referencia extensa de comandos y API.
- `docs/DISENO.md`: decisiones y flujos de arquitectura.
- `docs/DEMO.md`: guion de defensa.
