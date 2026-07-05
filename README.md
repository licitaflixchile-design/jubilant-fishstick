# licitaflix-ingesta

Pipelines de ingesta de datos públicos de Mercado Público (ChileCompra)
hacia la base de datos de LicitaFlix.

Fuentes:
- **API v1** (api.mercadopublico.cl): licitaciones (listado + detalle con adjudicaciones).
- **API v2** (api2.mercadopublico.cl): compra ágil (incremental + cotizaciones).
- **Datos abiertos** (transparenciachc blob): órdenes de compra nacionales (ZIP mensual),
  agregadas por proveedor/mes con desglose por modalidad.

Los datos son públicos; las credenciales van en GitHub Secrets:
`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `MP_TICKET_V1`.

Observabilidad: cada corrida se registra en la tabla `ingesta_runs`.
