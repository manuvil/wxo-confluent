# Agente de Disponibilidad de Inventario en Tiempo Real

## 📋 Caso de Uso

En este tutorial, desarrollarás un agente en **watsonx Orchestrate** para un caso de uso de retail que realiza lo siguiente:

1. **Consulta disponibilidad de stock en tiempo real** de productos en sucursales específicas mediante integración con Confluent Kafka
2. **Recomienda alternativas similares** usando RAG agéntico cuando el stock es cero, consultando documentos empresariales con descripciones de productos

### Escenario Real

Durante temporadas de alta demanda como las fiestas navideñas, los artículos populares se agotan rápidamente y la disponibilidad cambia minuto a minuto en diferentes sucursales. Los clientes (a través de los empleados de la tienda) hacen constantemente preguntas simples como:

> *"¿Tienen el Laptop Dell XPS en el Mall of Egypt? Si no, ¿cuál es el portátil alternativo?"*

## 🎯 Lo que Aprenderás

Al finalizar este tutorial, tendrás una comprensión clara de:

- Cómo el **streaming de eventos** se integra en arquitecturas de agentes
- Cómo usar **watsonx Orchestrate** para razonar sobre datos en tiempo real

**IBM Bob** será tu compañero de desarrollo de software con IA, acelerando el proceso ayudándote a:

- ✅ Crear topics de Kafka y un cluster de ksqlDB en Confluent Cloud
- ✅ Publicar eventos de ejemplo al topic
- ✅ Construir una herramienta MCP y agentes de IA en watsonx Orchestrate
- ✅ Usar RAG Agéntico para enriquecer eventos de Kafka con documentos empresariales

## 🚀 Guía de Instalación y Configuración

### 1. Crear Entorno Virtual de Python

```bash
# Crear entorno virtual con Python 3.12
python3.12 -m venv .venv

# Activar el entorno virtual
source .venv/bin/activate

# Comprobar la versión de Python
python --version
```

### 2. Instalar Dependencias

```bash
# Instalar todas las dependencias del proyecto
pip install --force-reinstall -r requirements.txt

# Verificar que las dependencias estén correctamente instaladas
pip show confluent-kafka
pip show fastmcp
pip show python-dotenv
```

### 3. Configurar Variables de Entorno

```bash
# Crear archivo .env basado en .env.example
cp .env.example .env

# Editar .env con tus credenciales de Confluent Cloud
# Asegúrate de actualizar:
# - STUDENT_ID
# - KAFKA_API_KEY y KAFKA_API_SECRET
# - KSQLDB_API_KEY y KSQLDB_API_SECRET
```

### 4. Configurar Kafka y ksqlDB

```bash
# Crear el topic de Kafka
python create_topic.py

# Configurar los streams de ksqlDB
python setup_ksqldb_streams.py
```

### 5. Producir Mensajes de Prueba

```bash
# Generar eventos de inventario de ejemplo
python produce_messages.py
```

### 6. Configurar watsonx Orchestrate

#### Requisito Previo
Asegúrate de tener watsonx Orchestrate configurado y activo:

```bash
pip install ibm-watsonx-orchestrate
```

#



#### Importar el Toolkit en Orchestrate

```bash
# Activar el entorno de Orchestrate (si es necesario)
orchestrate env activate {nombre_entorno}

Cambiar los nombres de las tablas y demas en get_sku_availability.py

# Agregar el toolkit MCP
orchestrate toolkits add \
  --kind mcp \
  --name "sku-availability-checker_test_0" \
  --description "Real-time inventory availability checker using Confluent Kafka and ksqlDB" \
  --package-root "./sku_availability_mcp" \
  --command "python get_sku_availability.py" \
  --tools "*"
```
#### Importar el agente en Orchestrate

cambiar el nombre del agente en ./agent/sku-availability-agent.yaml

```bash
orchestrate agents import --file ./agent/sku-availability-agent.yaml
```

## 🧪 Probar la Solución

### Consultar Disponibilidad

Una vez configurado el agente en watsonx Orchestrate, puedes hacer preguntas como:

- *"¿Cuántos Laptop Dell XPS 15 hay disponibles en Dubai Mall?"*
- *"Muéstrame el inventario de todos los productos en Mall of Egypt"*
- *"¿Hay stock del iPhone 15 Pro en alguna sucursal?"*




#### Comandos Útiles de Orchestrate

```bash
# Listar toolkits disponibles
orchestrate toolkits list

# Eliminar un toolkit (si necesitas recrearlo)
orchestrate toolkits remove --name sku-availability-checker_test_0

# Ver detalles de un toolkit
orchestrate toolkits get --name sku-availability-checker_test_0
```

## 📁 Estructura del Proyecto

```
.
├── README.md                          # Este archivo
├── requirements.txt                   # Dependencias del proyecto
├── .env.example                       # Plantilla de variables de entorno
├── .env                              # Variables de entorno (no subir a git)
├── create_topic.py                   # Script para crear topic de Kafka
├── setup_ksqldb_streams.py           # Script para configurar ksqlDB
├── produce_messages.py               # Script para generar eventos de prueba
├── sample-transactions.json          # Datos de ejemplo
├── get_sku_availability.py           # Servidor MCP principal
└── sku_availability_mcp/             # Paquete MCP para Orchestrate
    ├── __init__.py
    ├── get_sku_availability.py
    └── requirements.txt
```

## 🔧 Arquitectura de la Solución

```
┌─────────────────┐
│  Retail Store   │
│   Associates    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│   watsonx Orchestrate Agent         │
│   ┌─────────────────────────────┐   │
│   │  MCP Tool:                  │   │
│   │  get_sku_availability()     │   │
│   └─────────────┬───────────────┘   │
│                 │                    │
│   ┌─────────────▼───────────────┐   │
│   │  Agentic RAG                │   │
│   │  (Product Alternatives)     │   │
│   └─────────────────────────────┘   │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│      Confluent Cloud                │
│   ┌─────────────────────────────┐   │
│   │  Kafka Topic:               │   │
│   │  inventory.transactions     │   │
│   └─────────────┬───────────────┘   │
│                 │                    │
│   ┌─────────────▼───────────────┐   │
│   │  ksqlDB Stream Processing   │   │
│   │  INVENTORY_AVAILABILITY     │   │
│   └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

## 🧪 Probar la Solución

### Consultar Disponibilidad

Una vez configurado el agente en watsonx Orchestrate, puedes hacer preguntas como:

- *"¿Cuántos Laptop Dell XPS 15 hay disponibles en Dubai Mall?"*
- *"Muéstrame el inventario de todos los productos en Mall of Egypt"*
- *"¿Hay stock del iPhone 15 Pro en alguna sucursal?"*


**Desarrollado con ❤️ usando IBM Bob como asistente de desarrollo**
