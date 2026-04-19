from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

from auth import (
    authenticate_farmeneur,
    decode_jwt_payload,
    InvalidCredentialsError,
    prompt_farmeneur_credentials,
)
from config import Settings, load_settings
from crowdfarming_client import CrowdfarmingClient
from env_file import write_env_values
from factulotes_client import AccountingExport, FactulotesClient
from quarter import DateRange, SPAIN_TZ, quarter_to_date_range

CONTABILIDAD_FORMATS = ["ares", "resumen_simple", "hispatec", "a3", "resumen", "gestion"]


@dataclass
class PayoutProcessingResult:
    payout_id: str
    downloaded: bool
    uploaded: bool
    lote_created: bool
    filename: str | None
    local_path: str | None
    upload_response: dict[str, Any] | None
    lote_nombre: str | None
    lote_fecha: str | None
    lote_contador: int | None
    lote_response: dict[str, Any] | None
    error: str | None


@dataclass(frozen=True)
class GeneratedReport:
    formato: str
    path: str
    lote_ids: list[int]


@dataclass(frozen=True)
class LoggedLoteRef:
    lote_id: int | None
    lote_nombre: str | None
    lote_contador_id: int | None
    lote_fecha: str | None


@dataclass(frozen=True)
class LoggedRunReference:
    log_path: Path
    quarter_label: str
    source_execution_id: str | None
    lote_refs: list[LoggedLoteRef]


@dataclass(frozen=True)
class ContadorSelection:
    contador_id: int
    source: str
    description: str | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Automatiza la descarga de payouts de Crowdfarming y su carga en FactuLotes."
    )
    parser.add_argument("--quarter", help="Trimestre en formato YYYYQn, por ejemplo 2026Q1")
    parser.add_argument("--start-date", help="Fecha de inicio ISO 8601")
    parser.add_argument("--end-date", help="Fecha de fin ISO 8601")
    parser.add_argument(
        "--from-log",
        help="Ruta de un run_log JSON existente para regenerar solo la contabilidad de ese run.",
    )
    parser.add_argument(
        "--lote-contador",
        type=int,
        required=False,
        help="ID del contador de FactuLotes. Si no se indica, se mostraran todos para elegir.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Descarga pero no sube ni crea lote")
    parser.add_argument("--skip-upload", action="store_true", help="No sube archivos a FactuLotes")
    parser.add_argument(
        "--skip-create-lote",
        action="store_true",
        help="No crea lotes en FactuLotes tras cada upload",
    )
    parser.add_argument(
        "--output-log",
        default=None,
        help="Ruta del log JSON de salida. Por defecto se genera dentro de TMP_DIR.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Ruta del fichero .env para leer/escribir configuracion.",
    )
    parser.add_argument(
        "--write-env",
        action="store_true",
        help="Guarda en el .env los datos obtenidos o introducidos en runtime.",
    )
    parser.add_argument(
        "--contabilidad-formato",
        action="append",
        choices=CONTABILIDAD_FORMATS,
        help="Formato de resumen contable a generar al final. Puede repetirse.",
    )
    parser.add_argument(
        "--skip-contabilidad",
        action="store_true",
        help="No genera los ficheros finales de contabilidad.",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)

    if args.from_log:
        if args.quarter or args.start_date or args.end_date:
            raise SystemExit("--from-log no puede combinarse con --quarter ni con --start-date/--end-date")
        if args.dry_run:
            raise SystemExit("--dry-run no tiene sentido con --from-log")
        if args.skip_contabilidad:
            raise SystemExit("--skip-contabilidad no tiene sentido con --from-log")
        args.skip_upload = True
        args.skip_create_lote = True
        return args

    has_quarter = bool(args.quarter)
    has_explicit_dates = bool(args.start_date and args.end_date)
    if not has_quarter and not has_explicit_dates:
        args = prompt_interactive_run_options(args)
        if args.from_log:
            args.skip_upload = True
            args.skip_create_lote = True
            return args
        has_quarter = bool(args.quarter)
        has_explicit_dates = bool(args.start_date and args.end_date)
    elif has_quarter == has_explicit_dates:
        raise SystemExit("Debes indicar --quarter o bien --start-date y --end-date")

    if bool(args.start_date) != bool(args.end_date):
        raise SystemExit("--start-date y --end-date deben usarse juntos")

    if args.dry_run:
        args.skip_upload = True
        args.skip_create_lote = True
        args.skip_contabilidad = True

    return args


def prompt_interactive_run_options(args: argparse.Namespace) -> argparse.Namespace:
    print("Selecciona el modo de carga:")
    print("1. Cargar un trimestre")
    print("2. Cargar un rango de fechas personalizado")
    print("3. Regenerar contabilidad desde un run_log existente")

    while True:
        option = input("Opcion [1/2/3]: ").strip()
        if option in {"1", "2", "3"}:
            break
        print("Introduce 1, 2 o 3.")

    if option == "3":
        args.from_log = prompt_run_log_selection(args.env_file)
        args.quarter = None
        args.start_date = None
        args.end_date = None
        args.skip_upload = True
        args.skip_create_lote = True
        args.dry_run = False
        print("")
        print(f"Log seleccionado: {args.from_log}")
        return args

    if option == "1":
        while True:
            quarter = input("Trimestre (formato YYYYQn, por ejemplo 2026Q1): ").strip().upper()
            try:
                quarter_to_date_range(quarter)
            except Exception:
                print("Formato de trimestre no valido.")
                continue
            args.quarter = quarter
            args.start_date = None
            args.end_date = None
            break
    else:
        while True:
            start_date = input("Fecha inicio ISO 8601: ").strip()
            end_date = input("Fecha fin ISO 8601: ").strip()
            if not start_date or not end_date:
                print("Debes indicar ambas fechas.")
                continue
            args.start_date = start_date
            args.end_date = end_date
            args.quarter = None
            break

    print("")
    print("Selecciona el tipo de ejecucion:")
    print("1. Completa: descargar, subir XLSX y crear lotes")
    print("2. Dry-run: descargar XLSX sin subir ni crear lotes")

    while True:
        mode = input("Opcion [1/2]: ").strip()
        if mode in {"1", "2"}:
            break
        print("Introduce 1 o 2.")

    if mode == "2":
        args.dry_run = True
        args.skip_upload = True
        args.skip_create_lote = True
    else:
        args.dry_run = False

    return args


def prompt_run_log_selection(env_file: str) -> str:
    available_logs = discover_run_logs(env_file)

    print("")
    if available_logs:
        print("Run logs detectados:")
        for index, log_path in enumerate(available_logs, start=1):
            print(f"{index}. {log_path}")
        print("")
        print("Puedes elegir un numero de la lista o escribir una ruta manual.")
    else:
        print("No se han detectado run logs automaticamente.")
        print("Introduce la ruta manualmente.")

    while True:
        prompt = "Run log"
        if available_logs:
            prompt += f" [1-{len(available_logs)} o ruta]"
        prompt += ": "
        raw = input(prompt).strip()
        if not raw:
            print("Debes indicar un log.")
            continue
        if available_logs and raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(available_logs):
                return str(available_logs[index - 1])
            print("Numero fuera de rango.")
            continue

        candidate = Path(raw).expanduser()
        if candidate.exists() and candidate.is_file():
            return str(candidate)
        print("No existe ese fichero.")


def discover_run_logs(env_file: str) -> list[Path]:
    try:
        settings = load_settings(env_file)
        base_dir = settings.tmp_dir
    except Exception:
        base_dir = Path("./tmp/payouts")
    return sorted(base_dir.glob("run_log_*.json"))


def resolve_date_range(args: argparse.Namespace) -> DateRange:
    if args.quarter:
        return quarter_to_date_range(args.quarter)
    return DateRange(start_date=args.start_date, end_date=args.end_date)


def load_logged_run_reference(log_path: Path) -> LoggedRunReference:
    if not log_path.exists():
        raise ValueError(f"No existe el log indicado: {log_path}")

    payload = json.loads(log_path.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("El log no contiene una lista 'results' valida")

    lote_refs: list[LoggedLoteRef] = []
    for item in results:
        if not isinstance(item, dict):
            continue

        lote_response = item.get("loteResponse")
        lote_id = _coerce_int(lote_response.get("id")) if isinstance(lote_response, dict) else None
        lote_nombre = item.get("loteNombre") if isinstance(item.get("loteNombre"), str) else None
        lote_contador_id = _coerce_int(item.get("loteContadorId"))
        lote_fecha = item.get("loteFecha") if isinstance(item.get("loteFecha"), str) else None

        if lote_id is None and not lote_nombre:
            continue

        lote_refs.append(
            LoggedLoteRef(
                lote_id=lote_id,
                lote_nombre=lote_nombre,
                lote_contador_id=lote_contador_id,
                lote_fecha=lote_fecha,
            )
        )

    if not lote_refs:
        raise ValueError("El log no contiene lotes reutilizables para generar contabilidad")

    quarter = payload.get("quarter")
    quarter_label = quarter.strip() if isinstance(quarter, str) and quarter.strip() else "custom-range"
    source_execution_id = (
        payload.get("executionId")
        if isinstance(payload.get("executionId"), str) and payload.get("executionId").strip()
        else None
    )

    return LoggedRunReference(
        log_path=log_path,
        quarter_label=quarter_label,
        source_execution_id=source_execution_id,
        lote_refs=lote_refs,
    )


def extract_payout_id(payout: dict[str, Any]) -> str:
    candidate_keys = [
        "payoutId",
        "id",
        "_id",
        "uuid",
    ]
    for key in candidate_keys:
        value = payout.get(key)
        if value:
            return str(value)
    raise ValueError(f"No payout identifier found in payload keys: {sorted(payout.keys())}")


def ensure_valid_xlsx(path: Path) -> None:
    if path.suffix.lower() != ".xlsx":
        raise ValueError(f"Expected .xlsx file, got: {path.name}")
    if path.stat().st_size <= 0:
        raise ValueError(f"Downloaded XLSX is empty: {path.name}")


def save_binary_file(tmp_dir: Path, filename: str, content: bytes) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    destination = tmp_dir / filename
    destination.write_bytes(content)
    ensure_valid_xlsx(destination)
    return destination


def build_default_log_path(
    settings: Settings,
    args: argparse.Namespace,
    execution_id: str | None = None,
) -> Path:
    stamp = args.quarter or "custom-range"
    suffix = f"_{execution_id}" if execution_id else ""
    return settings.tmp_dir / f"run_log_{stamp}{suffix}.json"


def sanitize_lote_name(raw_value: str) -> str:
    sanitized = "".join(char if char.isalnum() else "_" for char in raw_value.strip())
    sanitized = sanitized.strip("_")
    if not sanitized:
        raise ValueError("Derived lote name is empty after sanitization")
    return sanitized


def derive_lote_name(payout: dict[str, Any], payout_id: str, execution_id: str | None = None) -> str:
    candidate_keys = [
        "invoiceCode",
        "nombreLote",
        "loteNombre",
        "batchName",
        "lotName",
        "nombre",
        "name",
        "codigo",
        "code",
        "referencia",
        "reference",
        "documentNumber",
        "document_number",
        "payoutNumber",
        "number",
    ]
    for key in candidate_keys:
        value = payout.get(key)
        if isinstance(value, str) and value.strip():
            base_name = sanitize_lote_name(value)
            return _append_execution_suffix(base_name, execution_id)

    nested_candidates = [
        payout.get("document"),
        payout.get("lote"),
        payout.get("batch"),
        payout.get("metadata"),
    ]
    for candidate in nested_candidates:
        if isinstance(candidate, dict):
            for key in candidate_keys:
                value = candidate.get(key)
                if isinstance(value, str) and value.strip():
                    base_name = sanitize_lote_name(value)
                    return _append_execution_suffix(base_name, execution_id)

    return _append_execution_suffix(f"payout_{payout_id}", execution_id)


def _append_execution_suffix(base_name: str, execution_id: str | None) -> str:
    if not execution_id:
        return base_name
    return f"{base_name}_{execution_id}"


def derive_lote_date(payout: dict[str, Any]) -> str:
    candidate_keys = ["date", "createdAt", "payoutDate", "invoiceDate"]
    for key in candidate_keys:
        value = payout.get(key)
        if isinstance(value, str) and value.strip():
            normalized = value.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=SPAIN_TZ)
            return parsed.astimezone(SPAIN_TZ).date().isoformat()
    raise ValueError("No payout date found to derive lote fecha")


def pick_default_contador_id(factulotes: FactulotesClient) -> ContadorSelection:
    errors: list[str] = []

    try:
        contadores = factulotes.get_contadores()
        if contadores:
            selected = max(contadores, key=_contador_sort_key)
            contador_id = _coerce_int(selected.get("id"))
            if contador_id is None:
                raise ValueError("El contador seleccionado no tiene un id valido")
            return ContadorSelection(
                contador_id=contador_id,
                source="contadores",
                description=_describe_contador(selected),
            )
        errors.append("No hay contadores disponibles en FactuLotes")
    except Exception as exc:
        errors.append(f"contadores: {exc}")

    try:
        lotes = factulotes.get_lotes()
        selected_lote = max(
            (
                lote
                for lote in lotes
                if _coerce_int(lote.get("contador_id")) is not None
            ),
            key=_lote_sort_key,
        )
        contador_id = _coerce_int(selected_lote.get("contador_id"))
        if contador_id is None:
            raise ValueError("El lote seleccionado no contiene contador_id valido")
        lote_nombre = selected_lote.get("nombre")
        description = f"ultimo lote encontrado: {lote_nombre}" if lote_nombre else None
        return ContadorSelection(
            contador_id=contador_id,
            source="lotes",
            description=description,
        )
    except ValueError:
        errors.append("lotes: No hay lotes con contador_id disponible")
    except Exception as exc:
        errors.append(f"lotes: {exc}")

    joined_errors = " | ".join(errors)
    raise ValueError(
        "No se pudo determinar automaticamente el contador de FactuLotes. "
        f"Detalles: {joined_errors}"
    )


def get_contadores_sorted(factulotes: FactulotesClient) -> list[dict[str, Any]]:
    return sorted(factulotes.get_contadores(), key=_contador_sort_key, reverse=True)


def prompt_contador_selection(factulotes: FactulotesClient) -> ContadorSelection:
    contadores = get_contadores_sorted(factulotes)
    if not contadores:
        raise ValueError("No hay contadores disponibles en FactuLotes")

    print("")
    print("Contadores disponibles en FactuLotes:")
    for contador in contadores:
        contador_id = _coerce_int(contador.get("id"))
        if contador_id is None:
            continue
        prefijo = contador.get("prefijo") or "-"
        numero_actual = contador.get("numero_actual")
        pais_prefijo = contador.get("pais_prefijo")
        pais_sufijo = contador.get("pais_sufijo")
        print(
            f"  {contador_id}: prefijo={prefijo} "
            f"numero_actual={numero_actual} pais_prefijo={pais_prefijo} pais_sufijo={pais_sufijo}"
        )

    print("")
    while True:
        raw = input("Selecciona el ID del contador a usar para esta ejecucion: ").strip()
        contador_id = _coerce_int(raw)
        if contador_id is None:
            print("Introduce un ID numerico valido.")
            continue
        selected = next((item for item in contadores if _coerce_int(item.get("id")) == contador_id), None)
        if selected is None:
            print("Ese contador no existe en la lista.")
            continue
        return ContadorSelection(
            contador_id=contador_id,
            source="interactive",
            description=_describe_contador(selected),
        )


def _contador_sort_key(item: dict[str, Any]) -> tuple[str, int]:
    created = item.get("fecha_creacion")
    return (str(created or ""), _coerce_int(item.get("id")) or 0)


def _lote_sort_key(item: dict[str, Any]) -> tuple[str, int]:
    created = item.get("fecha_creacion")
    return (str(created or ""), _coerce_int(item.get("id")) or 0)


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _describe_contador(contador: dict[str, Any]) -> str | None:
    parts: list[str] = []
    nombre = contador.get("nombre")
    prefijo = contador.get("prefijo")
    if isinstance(nombre, str) and nombre.strip():
        parts.append(nombre.strip())
    if isinstance(prefijo, str) and prefijo.strip():
        parts.append(f"prefijo {prefijo.strip()}")
    return ", ".join(parts) if parts else None


def print_info(message: str) -> None:
    print(f"[INFO] {message}")


def print_warning(message: str) -> None:
    print(f"[WARN] {message}")


def payout_display_name(payout: dict[str, Any], payout_id: str) -> str:
    invoice_code = payout.get("invoiceCode")
    if isinstance(invoice_code, str) and invoice_code.strip():
        return invoice_code.strip()
    return payout_id


def process_payouts(
    payouts: list[dict[str, Any]],
    settings: Settings,
    args: argparse.Namespace,
    crowdfarming: CrowdfarmingClient,
    factulotes: FactulotesClient | None,
    execution_id: str | None,
) -> list[PayoutProcessingResult]:
    results: list[PayoutProcessingResult] = []
    total = len(payouts)

    for index, payout in enumerate(payouts, start=1):
        payout_id = extract_payout_id(payout)
        display_name = payout_display_name(payout, payout_id)
        try:
            print_info(f"[{index}/{total}] Procesando payout {display_name} ({payout_id})")
            content, filename = crowdfarming.download_transactions_xls(payout_id)
            local_path = save_binary_file(settings.tmp_dir, filename, content)
            print_info(f"[{index}/{total}] XLSX descargado: {filename}")

            upload_response = None
            uploaded = False
            lote_created = False
            lote_nombre = None
            lote_fecha = None
            lote_contador = None
            lote_response = None
            if not args.skip_upload:
                if factulotes is None:
                    raise ValueError("Factulotes client is required when uploads are enabled")
                print_info(f"[{index}/{total}] Subiendo XLSX a FactuLotes...")
                upload_response = factulotes.upload_file(local_path)
                uploaded = True
                print_info(
                    f"[{index}/{total}] XLSX subido correctamente: "
                    f"{extract_upload_reference(upload_response)}"
                )
                if not args.skip_create_lote:
                    lote_nombre = derive_lote_name(payout, payout_id, execution_id=execution_id)
                    lote_fecha = derive_lote_date(payout)
                    lote_contador = args.lote_contador
                    print_info(
                        f"[{index}/{total}] Creando lote {lote_nombre} "
                        f"(fecha {lote_fecha}, contador {lote_contador})..."
                    )
                    lote_response = factulotes.create_lote(
                        nombre=lote_nombre,
                        contador=lote_contador,
                        fecha=lote_fecha,
                    )
                    lote_created = True
                    lote_id = lote_response.get("id") if isinstance(lote_response, dict) else None
                    if lote_id is not None:
                        print_info(f"[{index}/{total}] Lote creado con id {lote_id}")
                    else:
                        print_info(f"[{index}/{total}] Lote creado correctamente")

            results.append(
                PayoutProcessingResult(
                    payout_id=payout_id,
                    downloaded=True,
                    uploaded=uploaded,
                    lote_created=lote_created,
                    filename=filename,
                    local_path=str(local_path),
                    upload_response=upload_response,
                    lote_nombre=lote_nombre,
                    lote_fecha=lote_fecha,
                    lote_contador=lote_contador,
                    lote_response=lote_response,
                    error=None,
                )
            )
        except Exception as exc:
            print_warning(f"[{index}/{total}] Error en payout {display_name}: {exc}")
            results.append(
                PayoutProcessingResult(
                    payout_id=payout_id,
                    downloaded=False,
                    uploaded=False,
                    lote_created=False,
                    filename=None,
                    local_path=None,
                    upload_response=None,
                    lote_nombre=None,
                    lote_fecha=None,
                    lote_contador=None,
                    lote_response=None,
                    error=str(exc),
                )
            )

    return results


def write_log(
    log_path: Path,
    args: argparse.Namespace,
    date_range: DateRange,
    payouts: list[dict[str, Any]],
    results: list[PayoutProcessingResult],
    execution_id: str | None,
    generated_reports: list[GeneratedReport],
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "quarter": args.quarter,
        "executionId": execution_id,
        "startDate": date_range.start_date,
        "endDate": date_range.end_date,
        "lote": {
            "nombre": "derived-from-payout",
            "contadorId": args.lote_contador,
            "fecha": "derived-from-payout-date",
        },
        "summary": {
            "payoutsFound": len(payouts),
            "downloaded": sum(1 for result in results if result.downloaded),
            "uploaded": sum(1 for result in results if result.uploaded),
            "lotesCreated": sum(1 for result in results if result.lote_created),
            "errors": sum(1 for result in results if result.error),
            "lotesSkipped": args.skip_create_lote,
        },
        "generatedReports": [
            {
                "formato": report.formato,
                "path": report.path,
                "loteIds": report.lote_ids,
            }
            for report in generated_reports
        ],
        "results": [
            {
                "payoutId": result.payout_id,
                "downloaded": result.downloaded,
                "uploaded": result.uploaded,
                "loteCreated": result.lote_created,
                "filename": result.filename,
                "localPath": result.local_path,
                "uploadResponse": result.upload_response,
                "loteNombre": result.lote_nombre,
                "loteFecha": result.lote_fecha,
                "loteContadorId": result.lote_contador,
                "loteResponse": result.lote_response,
                "error": result.error,
            }
            for result in results
        ],
    }
    log_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def print_summary(
    args: argparse.Namespace,
    date_range: DateRange,
    payouts: list[dict[str, Any]],
    results: list[PayoutProcessingResult],
    log_path: Path,
    generated_reports: list[GeneratedReport],
) -> None:
    downloaded = sum(1 for result in results if result.downloaded)
    uploaded = sum(1 for result in results if result.uploaded)
    lotes_created = sum(1 for result in results if result.lote_created)
    errors = sum(1 for result in results if result.error)

    quarter_label = args.quarter or f"{date_range.start_date} -> {date_range.end_date}"

    print(f"Trimestre/rango: {quarter_label}")
    print(f"Payouts encontrados: {len(payouts)}")
    print(f"XLSX descargados: {downloaded}")
    print(f"XLSX subidos: {uploaded}")
    print(f"Lotes creados: {lotes_created}")
    if generated_reports:
        print(f"Resumenes generados: {len(generated_reports)}")
    print(f"Errores: {errors}")
    print(f"Log JSON: {log_path}")


def prompt_required_value(label: str, default: str | None = None) -> str:
    prompt = label
    if default:
        prompt += f" [{default}]"
    prompt += ": "
    value = input(prompt).strip()
    resolved = value or (default or "")
    if not resolved:
        raise SystemExit(f"{label} es obligatorio")
    return resolved


def build_validation_range() -> DateRange:
    today = datetime.now(SPAIN_TZ).date().isoformat()
    return DateRange(
        start_date=f"{today}T00:00:00+00:00",
        end_date=f"{today}T23:59:59+00:00",
    )


def try_extract_farmer_id_from_token(token: str | None) -> str | None:
    if not token:
        return None
    try:
        jwt_payload = decode_jwt_payload(token)
    except Exception:
        return None
    user = jwt_payload.get("user") if isinstance(jwt_payload, dict) else None
    farmer_id = user.get("_profile") if isinstance(user, dict) else None
    if isinstance(farmer_id, str) and farmer_id.strip():
        return farmer_id
    return None


def validate_saved_crowdfarming_token(settings: Settings) -> None:
    if not settings.crowdfarming_token or not settings.farmer_id:
        raise ValueError("Faltan CROWDFARMING_TOKEN o FARMER_ID")

    probe_range = build_validation_range()
    crowdfarming = CrowdfarmingClient(
        base_url=settings.crowdfarming_base_url,
        token=settings.crowdfarming_token,
        timeout_seconds=settings.timeout_seconds,
        max_retries=0,
    )
    crowdfarming.get_payouts(
        farmer_id=settings.farmer_id,
        start_date=probe_range.start_date,
        end_date=probe_range.end_date,
    )


def validate_saved_factulotes_token(settings: Settings) -> None:
    if not settings.factulotes_token:
        raise ValueError("Falta FACTULOTES_TOKEN")

    factulotes = FactulotesClient(
        base_url=settings.factulotes_base_url,
        token=settings.factulotes_token,
        timeout_seconds=settings.timeout_seconds,
        max_retries=0,
    )
    factulotes.get_contadores()


def validate_saved_runtime_settings(
    settings: Settings,
    needs_factulotes: bool,
) -> tuple[Settings, dict[str, str], list[str]]:
    updated_settings = settings
    env_updates: dict[str, str] = {}
    errors: list[str] = []

    if not updated_settings.farmer_id:
        farmer_id = try_extract_farmer_id_from_token(updated_settings.crowdfarming_token)
        if farmer_id:
            updated_settings = replace(updated_settings, farmer_id=farmer_id)
            env_updates["FARMER_ID"] = farmer_id

    if updated_settings.crowdfarming_token and updated_settings.farmer_id:
        try:
            validate_saved_crowdfarming_token(updated_settings)
            print_info("Token guardado de Crowdfarming valido.")
        except Exception as exc:
            errors.append(f"CROWDFARMING_TOKEN invalido: {exc}")
    else:
        errors.append("Falta CROWDFARMING_TOKEN o FARMER_ID")

    if needs_factulotes:
        if updated_settings.factulotes_token:
            try:
                validate_saved_factulotes_token(updated_settings)
                print_info("Token guardado de FactuLotes valido.")
            except Exception as exc:
                errors.append(f"FACTULOTES_TOKEN invalido: {exc}")
        else:
            errors.append("Falta FACTULOTES_TOKEN")

    return updated_settings, env_updates, errors


def ensure_runtime_config(args: argparse.Namespace, settings: Settings) -> Settings:
    updated_settings = settings
    env_updates: dict[str, str] = {}
    needs_factulotes = not args.skip_upload
    saved_tokens_available = bool(updated_settings.crowdfarming_token) and (
        bool(updated_settings.factulotes_token) or not needs_factulotes
    )

    saved_validation_errors: list[str] = []
    if saved_tokens_available:
        print_info("Comprobando tokens guardados...")
        updated_settings, derived_updates, saved_validation_errors = validate_saved_runtime_settings(
            updated_settings,
            needs_factulotes=needs_factulotes,
        )
        env_updates.update(derived_updates)
        if saved_validation_errors:
            print_warning("Los tokens guardados no sirven para esta ejecucion:")
            for error in saved_validation_errors:
                print_warning(f"  - {error}")
        else:
            print_info("Se reutilizaran los tokens guardados.")

    if saved_validation_errors or not saved_tokens_available:
        while True:
            email, password = prompt_farmeneur_credentials(settings.farmeneur_email)
            try:
                print_info("Autenticando en Farmeneur y resolviendo tokens de FarmerZone y FactuLotes...")
                auth_result = authenticate_farmeneur(
                    email=email,
                    password=password,
                    timeout_seconds=settings.timeout_seconds,
                )
                print_info(f"Autenticacion correcta. Farmer ID detectado: {auth_result.farmer_id}")
                break
            except InvalidCredentialsError:
                print("")
                print("Usuario o contrasena incorrectos en Farmeneur. Vuelve a intentarlo.")
                print("")
        updated_settings = replace(
            updated_settings,
            farmeneur_email=auth_result.email,
            crowdfarming_token=auth_result.farmerzone_token,
            factulotes_token=auth_result.factulotes_token,
            farmer_id=auth_result.farmer_id,
        )
        env_updates["FARMENEUR_EMAIL"] = auth_result.email
        env_updates["CROWDFARMING_TOKEN"] = auth_result.farmerzone_token
        env_updates["FACTULOTES_TOKEN"] = auth_result.factulotes_token
        env_updates["FARMER_ID"] = auth_result.farmer_id

    if args.write_env and env_updates:
        write_env_values(Path(args.env_file), env_updates)

    if not updated_settings.farmer_id:
        raise SystemExit("Falta FARMER_ID")
    if not updated_settings.crowdfarming_token:
        raise SystemExit("Falta CROWDFARMING_TOKEN")
    if not args.skip_upload and not updated_settings.factulotes_token:
        raise SystemExit("Falta FACTULOTES_TOKEN")

    return updated_settings


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings(args.env_file)
    if args.from_log:
        return run_contabilidad_from_log(args, settings)

    settings = ensure_runtime_config(args, settings)
    date_range = resolve_date_range(args)
    execution_label = "dry-run" if args.dry_run else "completa"
    execution_id = build_execution_id()

    print_info(
        f"Rango seleccionado: {date_range.start_date} -> {date_range.end_date} "
        f"(modo {execution_label}, ejecucion {execution_id})"
    )

    crowdfarming = CrowdfarmingClient(
        base_url=settings.crowdfarming_base_url,
        token=settings.crowdfarming_token,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    factulotes = None
    if not args.skip_upload:
        factulotes = FactulotesClient(
            base_url=settings.factulotes_base_url,
            token=settings.factulotes_token,
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
        )
        if not args.skip_create_lote and args.lote_contador is None:
            print_info("Cargando contadores de FactuLotes para que elijas uno...")
            contador_selection = prompt_contador_selection(factulotes)
            args.lote_contador = contador_selection.contador_id
            if contador_selection.description:
                print_info(f"Contador seleccionado: {contador_selection.contador_id} ({contador_selection.description})")
            else:
                print_info(f"Contador seleccionado: {contador_selection.contador_id}")
        elif not args.skip_create_lote:
            print_info(f"Usando contador indicado manualmente: {args.lote_contador}")

    print_info("Consultando payouts en Crowdfarming...")
    payouts = crowdfarming.get_payouts(
        farmer_id=settings.farmer_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
    )
    print_info(f"Payouts encontrados: {len(payouts)}")

    results = process_payouts(payouts, settings, args, crowdfarming, factulotes, execution_id)

    generated_reports: list[GeneratedReport] = []
    if factulotes is not None and not args.skip_contabilidad:
        lote_ids = [lote_id for lote_id in (extract_lote_id(result) for result in results) if lote_id is not None]
        if lote_ids:
            formatos = resolve_contabilidad_formats(args)
            generated_reports = generate_accounting_reports(
                factulotes,
                lote_ids=lote_ids,
                formatos=formatos,
                settings=settings,
                quarter_label=args.quarter or "custom-range",
                execution_id=execution_id,
            )
        else:
            print_warning("No se han creado lotes en esta ejecucion; no se genera resumen contable.")

    log_path = (
        Path(args.output_log)
        if args.output_log
        else build_default_log_path(settings, args, execution_id)
    )
    write_log(log_path, args, date_range, payouts, results, execution_id, generated_reports)
    print_summary(args, date_range, payouts, results, log_path, generated_reports)

    return 0 if all(result.error is None for result in results) else 1


def extract_upload_reference(upload_response: dict[str, Any] | None) -> str:
    if not isinstance(upload_response, dict):
        return "respuesta desconocida"
    for key in ("raw_text", "data"):
        value = upload_response.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().strip('"')
    return "respuesta sin nombre de archivo"


def extract_lote_id(result: PayoutProcessingResult) -> int | None:
    if not isinstance(result.lote_response, dict):
        return None
    return _coerce_int(result.lote_response.get("id"))


def resolve_logged_lote_ids(
    factulotes: FactulotesClient,
    logged_run: LoggedRunReference,
) -> list[int]:
    resolved: list[int] = []
    unresolved_refs = [ref for ref in logged_run.lote_refs if ref.lote_id is None]
    all_lotes = factulotes.get_lotes() if unresolved_refs else []

    for ref in logged_run.lote_refs:
        lote_id = ref.lote_id
        if lote_id is None:
            lote_id = find_lote_id_by_log_ref(all_lotes, ref)
        if lote_id is None:
            ref_name = ref.lote_nombre or "<sin nombre>"
            raise ValueError(
                f"No se pudo resolver el lote '{ref_name}' desde el log {logged_run.log_path}"
            )
        if lote_id not in resolved:
            resolved.append(lote_id)

    return resolved


def find_lote_id_by_log_ref(lotes: list[dict[str, Any]], ref: LoggedLoteRef) -> int | None:
    if not ref.lote_nombre:
        return None

    candidates = [
        lote
        for lote in lotes
        if isinstance(lote.get("nombre"), str) and lote.get("nombre") == ref.lote_nombre
    ]
    if ref.lote_contador_id is not None:
        candidates = [
            lote for lote in candidates if _coerce_int(lote.get("contador_id")) == ref.lote_contador_id
        ]
    if ref.lote_fecha:
        candidates = [
            lote
            for lote in candidates
            if isinstance(lote.get("fecha_factura"), str) and lote.get("fecha_factura", "")[:10] == ref.lote_fecha
        ]

    if not candidates:
        return None

    best = max(candidates, key=_lote_sort_key)
    return _coerce_int(best.get("id"))


def build_execution_id() -> str:
    return uuid.uuid4().hex[:6].upper()


def prompt_contabilidad_formats() -> list[str]:
    print("")
    print("Formatos de resumen contable disponibles:")
    for index, formato in enumerate(CONTABILIDAD_FORMATS, start=1):
        print(f"  {index}. {formato}")
    print("  0. No generar resumen")
    print("")

    while True:
        raw = input("Selecciona uno o varios formatos (ej. 1,3,6): ").strip()
        if raw == "0":
            return []
        choices = [part.strip() for part in raw.split(",") if part.strip()]
        selected: list[str] = []
        valid = True
        for choice in choices:
            if not choice.isdigit():
                valid = False
                break
            index = int(choice)
            if index < 1 or index > len(CONTABILIDAD_FORMATS):
                valid = False
                break
            formato = CONTABILIDAD_FORMATS[index - 1]
            if formato not in selected:
                selected.append(formato)
        if valid and selected:
            return selected
        print("Seleccion no valida.")


def resolve_contabilidad_formats(args: argparse.Namespace) -> list[str]:
    if args.skip_contabilidad:
        return []
    if args.contabilidad_formato:
        return list(dict.fromkeys(args.contabilidad_formato))
    return prompt_contabilidad_formats()


def save_accounting_export(
    export: AccountingExport,
    base_dir: Path,
    quarter_label: str,
    execution_id: str,
) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    destination = base_dir / f"contabilidad_{quarter_label}_{execution_id}_{export.formato}.{export.extension}"
    destination.write_bytes(export.content)
    return destination


def generate_accounting_reports(
    factulotes: FactulotesClient,
    lote_ids: list[int],
    formatos: list[str],
    settings: Settings,
    quarter_label: str,
    execution_id: str,
) -> list[GeneratedReport]:
    reports: list[GeneratedReport] = []
    if not lote_ids or not formatos:
        return reports

    output_dir = settings.tmp_dir / "contabilidad"
    for formato in formatos:
        print_info(f"Generando resumen contable en formato {formato} para lotes {lote_ids}...")
        export = factulotes.generate_contabilidad(lote_ids, formato)
        path = save_accounting_export(export, output_dir, quarter_label, execution_id)
        print_info(f"Resumen {formato} guardado en {path}")
        reports.append(GeneratedReport(formato=formato, path=str(path), lote_ids=lote_ids))

    return reports


def build_contabilidad_log_path(settings: Settings, quarter_label: str, execution_id: str) -> Path:
    return settings.tmp_dir / f"contabilidad_log_{quarter_label}_{execution_id}.json"


def write_contabilidad_only_log(
    log_path: Path,
    source_log_path: Path,
    source_execution_id: str | None,
    quarter_label: str,
    execution_id: str,
    lote_ids: list[int],
    generated_reports: list[GeneratedReport],
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": "contabilidad-from-log",
        "sourceLog": str(source_log_path),
        "sourceExecutionId": source_execution_id,
        "quarter": quarter_label,
        "executionId": execution_id,
        "summary": {
            "lotesReused": len(lote_ids),
            "reportsGenerated": len(generated_reports),
        },
        "loteIds": lote_ids,
        "generatedReports": [
            {
                "formato": report.formato,
                "path": report.path,
                "loteIds": report.lote_ids,
            }
            for report in generated_reports
        ],
    }
    log_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def print_contabilidad_only_summary(
    logged_run: LoggedRunReference,
    lote_ids: list[int],
    generated_reports: list[GeneratedReport],
    log_path: Path,
) -> None:
    print("Modo: contabilidad desde log")
    print(f"Log origen: {logged_run.log_path}")
    print(f"Run origen: {logged_run.source_execution_id or 'sin executionId'}")
    print(f"Lotes reutilizados: {len(lote_ids)}")
    print(f"Resumenes generados: {len(generated_reports)}")
    print(f"Log JSON: {log_path}")


def ensure_factulotes_runtime_config(args: argparse.Namespace, settings: Settings) -> Settings:
    updated_settings = settings
    env_updates: dict[str, str] = {}

    if updated_settings.factulotes_token:
        try:
            validate_saved_factulotes_token(updated_settings)
            print_info("Token guardado de FactuLotes valido.")
            return updated_settings
        except Exception as exc:
            print_warning(f"FACTULOTES_TOKEN invalido: {exc}")

    while True:
        email, password = prompt_farmeneur_credentials(settings.farmeneur_email)
        try:
            print_info("Autenticando en Farmeneur para resolver el token de FactuLotes...")
            auth_result = authenticate_farmeneur(
                email=email,
                password=password,
                timeout_seconds=settings.timeout_seconds,
            )
            break
        except InvalidCredentialsError:
            print("")
            print("Usuario o contrasena incorrectos en Farmeneur. Vuelve a intentarlo.")
            print("")

    updated_settings = replace(
        updated_settings,
        farmeneur_email=auth_result.email,
        crowdfarming_token=auth_result.farmerzone_token,
        factulotes_token=auth_result.factulotes_token,
        farmer_id=auth_result.farmer_id,
    )
    env_updates["FARMENEUR_EMAIL"] = auth_result.email
    env_updates["CROWDFARMING_TOKEN"] = auth_result.farmerzone_token
    env_updates["FACTULOTES_TOKEN"] = auth_result.factulotes_token
    env_updates["FARMER_ID"] = auth_result.farmer_id

    if args.write_env and env_updates:
        write_env_values(Path(args.env_file), env_updates)

    if not updated_settings.factulotes_token:
        raise SystemExit("Falta FACTULOTES_TOKEN")

    return updated_settings


def run_contabilidad_from_log(args: argparse.Namespace, settings: Settings) -> int:
    settings = ensure_factulotes_runtime_config(args, settings)
    logged_run = load_logged_run_reference(Path(args.from_log))
    execution_id = build_execution_id()

    print_info(
        f"Regenerando contabilidad desde {logged_run.log_path} "
        f"(run origen {logged_run.source_execution_id or 'sin executionId'}, ejecucion {execution_id})"
    )

    factulotes = FactulotesClient(
        base_url=settings.factulotes_base_url,
        token=settings.factulotes_token,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )

    lote_ids = resolve_logged_lote_ids(factulotes, logged_run)
    print_info(f"Lotes reutilizados para la contabilidad: {lote_ids}")

    formatos = resolve_contabilidad_formats(args)
    if not formatos:
        print_info("No se ha solicitado ningun formato de contabilidad.")
        return 0

    generated_reports = generate_accounting_reports(
        factulotes,
        lote_ids=lote_ids,
        formatos=formatos,
        settings=settings,
        quarter_label=logged_run.quarter_label,
        execution_id=execution_id,
    )

    log_path = (
        Path(args.output_log)
        if args.output_log
        else build_contabilidad_log_path(settings, logged_run.quarter_label, execution_id)
    )
    write_contabilidad_only_log(
        log_path=log_path,
        source_log_path=logged_run.log_path,
        source_execution_id=logged_run.source_execution_id,
        quarter_label=logged_run.quarter_label,
        execution_id=execution_id,
        lote_ids=lote_ids,
        generated_reports=generated_reports,
    )
    print_contabilidad_only_summary(logged_run, lote_ids, generated_reports, log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
