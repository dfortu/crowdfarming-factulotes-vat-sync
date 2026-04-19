# Crowdfarming -> FactuLotes VAT Sync

Automatiza la carga trimestral de XLSX de payouts de Crowdfarming en FactuLotes y permite generar los resúmenes contables del trimestre.

## Qué hace

- Autentica mediante Farmeneur y obtiene los tokens necesarios para FarmerZone y FactuLotes.
- Consulta los payouts de un trimestre o de un rango de fechas.
- Descarga el XLSX de transacciones de cada payout.
- Sube cada XLSX a FactuLotes.
- Crea un lote por payout usando:
  - nombre derivado de `invoiceCode`
  - fecha derivada de la fecha real del payout
  - contador elegido por el usuario
- Añade un sufijo corto por ejecución al nombre del lote para evitar duplicados.
- Genera resúmenes contables finales en los formatos soportados por FactuLotes:
  - `ares`
  - `resumen_simple`
  - `hispatec`
  - `a3`
  - `resumen`
  - `gestion`
- Permite regenerar los resúmenes de un run anterior a partir del `run_log` sin repetir el proceso completo.

## Requisitos

- Python 3.11+ recomendado
- `requests`
- Acceso válido a Farmeneur con permisos sobre FarmerZone y FactuLotes

Instalación:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuración

Copia `.env.example` a `.env` si quieres guardar configuración local:

```bash
cp .env.example .env
```

Variables principales:

- `FARMENEUR_EMAIL`
- `CROWDFARMING_TOKEN`
- `FACTULOTES_TOKEN`
- `FARMER_ID`
- `TMP_DIR`
- `TIMEOUT_SECONDS`
- `MAX_RETRIES`

Si faltan tokens, el script puede pedir credenciales por terminal y resolverlos automáticamente. Con `--write-env` los guarda en `.env`.

## Uso

Modo interactivo:

```bash
python3 sync_quarterly_vat.py
```

El menú permite:

1. procesar un trimestre
2. procesar un rango personalizado
3. regenerar contabilidad desde un `run_log` existente

Ejecución directa por trimestre:

```bash
python3 sync_quarterly_vat.py --quarter 2026Q1
```

Dry run:

```bash
python3 sync_quarterly_vat.py --quarter 2026Q1 --dry-run
```

Generar solo contabilidad desde un run anterior:

```bash
python3 sync_quarterly_vat.py \
  --from-log tmp/payouts/run_log_2026Q1_B11985.json \
  --contabilidad-formato gestion \
  --contabilidad-formato hispatec
```

## Salidas

El script deja sus artefactos en `TMP_DIR`:

- `run_log_*.json`: log completo del run
- `contabilidad_log_*.json`: log de regeneración de contabilidad desde un log previo
- `contabilidad_*.(xlsx|xml)`: ficheros finales generados

## Estructura

- `sync_quarterly_vat.py`: punto de entrada
- `main.py`: CLI y orquestación
- `auth.py`: login en Farmeneur y resolución de tokens
- `crowdfarming_client.py`: cliente de payouts/XLSX
- `factulotes_client.py`: cliente de uploads, lotes, contadores y contabilidad
- `config.py`: carga de configuración
- `env_file.py`: lectura/escritura de `.env`
- `quarter.py`: cálculo de rangos trimestrales

## Notas

- `contador` en FactuLotes es el ID del contador configurado, no un número consecutivo manual.
- El nombre del lote debe ser único. El script añade un identificador corto por ejecución para evitar errores de lote duplicado.
- Los artefactos locales (`.env`, `tmp/`, respuestas de pruebas, etc.) están excluidos del repo.

## Licencia

Este repositorio está publicado bajo `PolyForm Noncommercial 1.0.0`.

- Se permite usar, copiar y modificar el código para fines no comerciales.
- El uso comercial no está permitido sin permiso previo del autor.
- Consulta [LICENSE](LICENSE) para el texto completo.
