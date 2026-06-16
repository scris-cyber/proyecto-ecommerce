# proyecto-ecommerce
# Secure E-commerce Analytics System

Proyecto Práctico - Administración Segura de Datos
Maestría en Ciberseguridad, Tecnológico de Costa Rica (TEC)

## Descripción del Proyecto
Este sistema automatiza la ingesta de datos de e-commerce, los procesa bajo un modelo ontológico, aplica técnicas de ofuscación/encriptación y expone los resultados mediante un dashboard seguro con autenticación OAuth 2.0 (PKCE).

## Arquitectura de Seguridad
1. **Ontología:** Mapeo de datos crudos a una estructura semántica definida.
2. **Procesamiento Distribuido:** Cluster Apache Spark sobre Docker, configurado con encriptación *at-transit* y *at-rest*.
3. **Ofuscación:** Los campos sensibles se ofuscan en el job de Spark antes de la persistencia.
4. **Base de Datos Protegida:** PostgreSQL con *Row-Level Security* (RLS) para filtrar acceso según el rol del usuario.
5. **Autenticación:** Flujo de OAuth 2.0 con **PKCE** integrado para asegurar el acceso al dashboard.

## Instrucciones de Despliegue (Guía para Pedro)

### 1. Configuración Inicial
Crea tu archivo `.env` basado en el `.env.example`. Asegúrate de definir las variables de conexión a la BD y las credenciales del proveedor de identidad (OAuth).

### 2. Ejecución del Cluster
Levanta la infraestructura distribuida:
```bash
docker-compose up -d --build

3. Procesamiento de Datos (Spark Job)
Para ejecutar la ingesta y transformación según la ontología:
# Entrar al contenedor de Spark
docker exec -it spark-master bash

# Ejecutar job de procesamiento (ofuscación y agregación)
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/jobs/process_ecommerce.py
. Dashboard de Consultas
Para iniciar la interfaz de visualización (Streamlit) con autenticación:
streamlit run app.py
