# Graph

Microservice for graph data

### Environment variables

```toml
APP_PREFIX=<app_prefix>
ARANGO_HOST=<arango_host>
ARANGO_PASSWORD=<arango_graph_password>
ARANGO_PORT=<arango_port>
ARANGO_PROTOCOL=<arango_protocol>
ARANGO_USERNAME=<arango_graph_username>
DOCS_CUSTOM_ENABLED=<True/False>
DOCS_REDOC_JS_URL=<redoc_js_url>
DOCS_SWAGGER_CSS_URL=<swagger_css_url>
DOCS_SWAGGER_JS_URL=<swagger_js_url>
INVENTORY_GRPC_HOST=<inventory_host>
INVENTORY_GRPC_PORT=<inventory_grpc_port>
KAFKA_GROUP_ID=Graph
KAFKA_INVENTORY_CHANGES_TOPIC=inventory.changes
KAFKA_KEYCLOAK_CLIENT_ID=<kafka_client>
KAFKA_KEYCLOAK_CLIENT_SECRET=<kafka_client_secret>
KAFKA_KEYCLOAK_SCOPES=profile
KAFKA_SASL_MECHANISM=<kafka_sasl_mechanism>
KAFKA_SECURITY_PROTOCOL=<kafka_security_protocol>
KAFKA_URL=<kafka_host>:<kafka_port>
KEYCLOAK_HOST=<keycloak_host>
KEYCLOAK_PORT=<keycloak_port>
KEYCLOAK_PROTOCOL=<keycloak_protocol>
KEYCLOAK_REALM=avataa
KEYCLOAK_REDIRECT_HOST=<keycloak_external_host>
KEYCLOAK_REDIRECT_PORT=<keycloak_external_port>
KEYCLOAK_REDIRECT_PROTOCOL=<keycloak_external_protocol>
SECURITY_TYPE=<security_type>
UVICORN_WORKERS=<uvicorn_workers_number>
```

### Explanation

#### General settings

`APP_PREFIX` Application prefix (default: _/api/graph_)  

#### Inventory gRPC

`INVENTORY_GRPC_HOST` Host of the gRPS server, which is raised in the inventory (default: _localhost_)  
`INVENTORY_GRPC_PORT` Port of the gRPS server, which is raised in the inventory (default: _50051_)  

#### Arango

`ARANGO_PROTOCOL` Communication protocol with Arango. Possible options: _http_, _https_ (default: _http_)  
`ARANGO_HOST` Arango host (default: _localhost_)  
`ARANGO_PORT` Arango port (default: _8529_)  
`ARANGO_USERNAME` User with admin rights in the database (default: _root_)  
`ARANGO_PASSWORD` Arango user password (default: _rootpassword_)  

#### Compose

- `REGISTRY_URL` - Docker regitry URL, e.g. `harbor.avataa.dev`
- `PLATFORM_PROJECT_NAME` - Docker regitry project Docker image can be downloaded from, e.g. `avataa`

### Requirements
```
$ pip install -r requirements.txt
```

### Running

```
$ cd app
$ uvicorn main:app --reload

INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [28720]
INFO:     Started server process [28722]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```