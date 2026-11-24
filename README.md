# Clúster Spark para el dataset Epsilon

Guía preparada para que el equipo reproduzca la misma instalación mañana. El Razer funciona como maestro y también como worker. La HP y el tercer equipo funcionan como workers. La tarea procesa el dataset Epsilon en formato LIBSVM y reporta filas, balance de clases, número de valores, suma de cuadrados, particiones y host que procesó cada partición. Sirve para demostrar el reparto y la comunicación entre máquinas.

## Direcciones y máquinas

| Rol | Equipo | IP LAN observada hoy | SSH |
|---|---|---:|---|
| Maestro + worker | Razer Blade 14 con Debian 13 | `192.168.1.111` por Ethernet | equipo actual |
| Worker | HP con Debian 13 | `192.168.1.109` | `hp` |
| Worker adicional | Tercer equipo del equipo | Por identificar mañana | Por configurar |

El Razer ya está conectado por Ethernet (`enx00e04c363ca1`, IP `192.168.1.111`) y también conserva Wi‑Fi (`wlp3s0`, IP `192.168.1.97`). La HP usa Ethernet `eno1` en `192.168.1.109`. `ip route get` confirmó que el tráfico del Razer a la HP toma la interfaz Ethernet. El archivo local `cluster.env` y la plantilla del repositorio ya apuntan a `192.168.1.111` y `192.168.1.109`.

Vuelve a confirmar las direcciones al iniciar mañana; DHCP puede cambiarlas:

```bash
# En el Razer (la ruta a HP debe mostrar dev enx00e04c363ca1 y src 192.168.1.111)
ip -brief -4 address
ip route get 192.168.1.109

# Desde el Razer, confirma que el alias SSH hp llega a la HP
ssh -F ~/.ssh/config hp 'hostname; hostname -I; ip -brief -4 address'
```

Las IP pueden variar por DHCP. Si mañana aparece otra dirección, reemplázala en `cluster.env` antes de iniciar Spark. Usa la dirección de la interfaz Ethernet del Razer. No configures IP fija ni cambies la red de la HP durante la preparación, porque el servidor puede prestar otros servicios.

## Versiones fijadas

- Debian 13 x86_64 en los nodos conocidos.
- Apache Spark **4.0.1**, compilado para Scala **2.13**. La versión está fijada en `conf/spark-version`; `bin/install` descarga la distribución oficial, comprueba SHA-512 y prepara las bibliotecas.
- Java **17 o 21** en todos los nodos. El maestro requiere JDK para compilar; los workers requieren Java Runtime. Spark 4.0.1 declara soporte para [Java 17/21 y Scala 2.13](https://spark.apache.org/docs/4.0.1/).
- El trabajo está escrito en Java y no requiere PySpark, Conda ni paquetes Python de ML.

En el Razer ya encontramos bibliotecas Spark 4.0.1 en la caché del proyecto RestMex y Java Temurin 17.0.20.1 en `/home/krvin/code/restmex-2025-spark-scala-main/.runtime/jdk17`; esas bibliotecas permitieron ejecutar la prueba con la HP. En la HP comprobamos OpenJDK 21.0.13.1. El proyecto fuente no incluye Java, los JAR de Spark ni el dataset: cada instalación obtiene las dependencias oficiales y el dataset se descarga en el maestro.

## Preparación inicial para mañana

### 1. Maestro: Razer

Abre una terminal en el Razer y ejecuta:

```bash
sudo apt update
sudo apt install -y git curl tar bzip2 rsync openssh-client python3 openjdk-17-jdk
git clone https://github.com/AloHerdz/spark-epsilon-cluster.git
cd spark-epsilon-cluster
cp cluster.env.example cluster.env
```

El `cluster.env` ya queda con las IP observadas. Si la dirección cambia después de conectar Ethernet, corrige `MASTER_HOST` y `HP_NODE_HOST`:

```bash
nano cluster.env
```

Instala el runtime fijado y compila el trabajo:

```bash
bin/install master
```

Este paso descarga la distribución Spark 4.0.1 de Apache (aprox. 524 MiB) y verifica su checksum. Requiere Internet en el maestro y varios cientos de MB disponibles. No descargues Spark aparte en la HP: el script de inicio sincroniza el runtime idéntico del maestro.

### 2. HP: worker

La HP debe aceptar `ssh hp` desde el Razer con clave SSH. Confirma Java y los requisitos de acceso:

```bash
ssh -F ~/.ssh/config hp 'java -version; command -v bash; command -v mkdir'
```

Si Java no está disponible, instala el runtime; `openssh-server` ya estaba activo durante la revisión:

```bash
ssh -F ~/.ssh/config hp 'sudo apt update && sudo apt install -y openjdk-17-jre-headless openssh-server rsync'
```

El Razer copia el proyecto y los JAR de Spark a `~/cluster-epsilon` en la HP al iniciar el clúster. La HP no necesita un clon separado del repositorio.

### 3. Muestra rápida en dos nodos

Genera y copia la muestra sintética para verificar el enlace antes de descargar epsilon:

```bash
bin/make-sample 512
bin/stage sample
bin/start-two-node
bin/observe
bin/submit sample | tee results/sample-run.log
bin/stop-two-node
```

La línea final debe mostrar `rows=512`, `invalid=0` y `hosts=hp,kriller`. Las líneas `PART` deben incluir los dos hosts. Esta muestra solo valida instalación, red y reparto; no sustituye el procesamiento del dataset real.

### 4. Descargar y procesar Epsilon en el maestro

En el Razer, ejecuta:

```bash
bin/fetch-epsilon
bin/stage epsilon
bin/start-two-node
bin/observe
bin/submit epsilon | tee results/epsilon-run.log
bin/stop-two-node
```

El maestro conserva la copia principal en `data/epsilon_normalized.bz2`; `bin/stage epsilon` la replica a la HP mediante SSH/rsync. Spark recibe la misma ruta local `/tmp/epsilon-cluster-data/epsilon_normalized.bz2` en cada nodo. Se requieren aproximadamente **3.6 GiB** en el maestro para la descarga y otros **3.6 GiB libres en cada worker** para su copia, además de espacio temporal. El programa falla si no encuentra 400 000 filas, si hay registros mal formados o si menos de dos hosts procesaron particiones. La fuente oficial de LIBSVM describe Epsilon como 400 000 filas de entrenamiento, 100 000 de prueba y 2 000 atributos: [dataset binario](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html).

## Añadir el tercer equipo

Identifica su IP, confirma Debian/Linux, Java 17/21 y SSH con clave desde el Razer. Después de añadir un alias al `~/.ssh/config` del Razer, inicia el worker así:

```bash
MASTER_HOST=192.168.1.111 bin/add-worker ALIAS_TERCER_NODO IP_TERCER_NODO
```

Si la IP del maestro cambia, usa el valor actualizado de `MASTER_HOST`. Asigna al tercer equipo una dirección disponible en `192.168.1.0/24`; una reserva DHCP `192.168.1.112` es una opción si confirmas que está libre en el router. No fijes una dirección al azar. `bin/add-worker` copia los JAR fijados y también la muestra o epsilon si ya están en `data/` del maestro. Para exigir que los tres hosts reciban tareas:

```bash
CLUSTER_CORES=6 REQUIRED_HOSTS=3 bin/submit epsilon
```

Ajusta `CLUSTER_CORES`, `WORKER_CORES` y `WORKER_MEMORY` según la RAM/CPU del tercer equipo; no excedas los recursos que queden libres en la HP.

## Mostrar la actividad al profesor

- Maestro: `http://192.168.1.111:8080` (lista de workers; sustituye la IP si cambió).
- Etapas y tareas mientras corre el trabajo: `http://192.168.1.111:4040`.
- HP: `http://192.168.1.109:8081`.
- `bin/observe` imprime estado de workers y conexiones activas; el log de `bin/submit` identifica cada partición y su host.
- Wireshark puede capturar el tráfico de red; filtro: `ip.addr == 192.168.1.111 && ip.addr == 192.168.1.109` (actualiza las IP si cambiaron). El shuffle de la muestra genera tráfico entre ejecutores. Spark UI muestra el reparto del trabajo y Wireshark los paquetes.

Los puertos usados son SSH `22`, Spark master `7077` y UI `8080`, worker `7078` y UI `8081`, driver `37071`, transferencia de bloques `37072` y Spark Jobs UI `4040`. La comunicación de bloques Spark puede usar puertos adicionales; los nodos deben poder comunicarse directamente por la LAN. La configuración es para una LAN de confianza: Spark Standalone no habilita autenticación por defecto. Al terminar, `bin/stop-two-node` detiene los workers y el maestro.

## Qué se comprobó antes de entregar

La muestra sintética se procesó en los dos hosts: 512 filas, 1 024 000 valores, cero registros inválidos y particiones ejecutadas tanto en `kriller` como en `hp`. También se comprobó la lectura del mismo formato comprimido en bzip2. Las pruebas y sus logs locales no se incluyen en Git; cada integrante puede repetir la muestra con los comandos de esta guía.
