"""
Proyecto Tienda Virtual — visor CSV + base de datos Supabase + tablero (Streamlit).
Versión unificada para despliegue web (p. ej. Streamlit Community Cloud).
"""
from __future__ import annotations

import csv
import os
import re
import tempfile
import unicodedata
from urllib.parse import quote_plus
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
DEFAULT_TABLE = os.environ.get("TV_TABLE", "contrataciones_datos")
# Tabla de logística (OC + producto); no altera la tabla principal de la fuente.
COMPLEMENTARIOS_TABLE = "datos_complementarios_oc"
AGENDAMIENTOS_TABLE = "agendamientos_entregas"
CATALOGO_STOCK_TABLE = "catalogo_stock_critico"

POSTGRES_DEFAULTS = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "database": os.environ.get("POSTGRES_DB", "postgres"),
}

# ==========================================
# 2. PROCESAMIENTO CSV
# ==========================================
def detect_delimiter(filepath: str, encoding: str = "utf-8") -> str:
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        sample = f.read(65536)
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        return ";" if sample.count(";") > sample.count(",") else ","


def _ascii_key_header(name: object) -> str:
    """Clave comparable para encabezados (quita tildes, pasa a minúsculas)."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = unicodedata.normalize("NFKD", str(name).strip()).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def _map_columnas_por_clave(df: pd.DataFrame) -> dict[str, str]:
    """Primera columna por cada clave normalizada."""
    m: dict[str, str] = {}
    for c in df.columns:
        k = _ascii_key_header(c)
        if k and k not in m:
            m[k] = str(c)
    return m


def _leer_excel_formato_dmp_stock_critico(uploaded) -> pd.DataFrame:
    """
    Excel tipo 'Stock Crítico según DMP - MSPBS': título en fila 0,
    encabezados (Código, Producto, …) en la siguiente fila útil.
    """
    raw = pd.read_excel(uploaded, sheet_name=0, header=None, engine="openpyxl")
    header_ix: Optional[int] = None
    for i in range(min(35, len(raw))):
        cells = [_ascii_key_header(x) for x in raw.iloc[i].tolist()]
        cells = [c for c in cells if c]
        if not cells:
            continue
        if any(c == "codigo" or c.startswith("codigo_") for c in cells) and any(
            "producto" in c for c in cells
        ):
            header_ix = i
            break
    if header_ix is None:
        raise ValueError(
            "No se detectó la fila de encabezados (se esperan columnas **Código** y **Producto**)."
        )
    headers: list[str] = []
    for j in range(raw.shape[1]):
        v = raw.iloc[header_ix, j]
        headers.append(str(v).strip() if pd.notna(v) else f"col_{j}")
    body = raw.iloc[header_ix + 1 :].copy()
    body.columns = headers
    body = body.dropna(how="all")
    return body


def preparar_dataframe_catalogo_stock(uploaded) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas codigo_siciap y descripcion_oficial.
    Acepta CSV ya normalizado o Excel/CSV del formato DMP (Código + Producto + …).
    """
    name = (uploaded.name or "").lower()
    if name.endswith(".csv"):
        raw = pd.read_csv(uploaded, dtype=str, encoding="utf-8", on_bad_lines="skip")
    elif name.endswith(".xlsx"):
        uploaded.seek(0)
        raw = _leer_excel_formato_dmp_stock_critico(uploaded)
    else:
        raise ValueError("Formato no soportado. Usá .csv o .xlsx.")

    raw.columns = [str(c).strip() for c in raw.columns]
    cm = _map_columnas_por_clave(raw)

    cod_col = (
        cm.get("codigo_siciap")
        or cm.get("codigo")
        or cm.get("codigo_simese")
        or cm.get("codigo_dmp")
    )
    if not cod_col:
        raise ValueError(
            "No se encontró columna de código (Código / codigo_siciap). Columnas: "
            + ", ".join(raw.columns[:20].tolist())
        )

    if "descripcion_oficial" in cm:
        desc_col = cm["descripcion_oficial"]
        out = raw[[cod_col, desc_col]].copy()
        out.columns = ["codigo_siciap", "descripcion_oficial"]
    else:
        prod_col = cm.get("producto")
        if not prod_col:
            raise ValueError(
                "No se encontró **Producto** ni **descripcion_oficial**. Columnas: "
                + ", ".join(raw.columns[:20].tolist())
            )
        extra_keys = (
            "concentracion",
            "forma_farmaceutica",
            "presentacion",
            "clasificacion",
        )
        cols_desc = [prod_col]
        for ek in extra_keys:
            if ek in cm:
                cols_desc.append(cm[ek])
        def _armar_desc(row: pd.Series) -> str:
            partes: list[str] = []
            for c in cols_desc:
                v = row.get(c, "")
                if pd.isna(v):
                    continue
                t = str(v).strip()
                if t and t.lower() not in ("nan", "none"):
                    partes.append(t)
            return " — ".join(partes)

        out = pd.DataFrame(
            {
                "codigo_siciap": raw[cod_col].astype(str),
                "descripcion_oficial": raw.apply(_armar_desc, axis=1),
            }
        )

    out["codigo_siciap"] = out["codigo_siciap"].astype(str).str.strip()
    out["descripcion_oficial"] = out["descripcion_oficial"].astype(str).str.strip()
    out = out[out["codigo_siciap"] != ""]
    out = out[out["descripcion_oficial"] != ""]
    out = out.drop_duplicates(subset=["codigo_siciap"], keep="last")
    return out


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.lower()
    )
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("\x00", "", regex=False)
                .str.replace("\ufffd", "", regex=False)
            )
    return df


def read_csv_smart(filepath: str, delimiter: Optional[str] = None) -> pd.DataFrame:
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(filepath)

    delim = delimiter or detect_delimiter(str(path))
    configs = [
        dict(sep=delim, encoding="utf-8", on_bad_lines="skip", low_memory=False, dtype=str),
        dict(sep=delim, encoding="latin1", on_bad_lines="skip", low_memory=False, dtype=str),
        dict(sep=delim, encoding="cp1252", on_bad_lines="skip", low_memory=False, dtype=str),
    ]
    last_err: Optional[Exception] = None
    for cfg in configs:
        try:
            df = pd.read_csv(str(path), **cfg)
            if len(df.columns) > 1 and len(df) > 0:
                return normalize_columns(df)
        except Exception as e:
            last_err = e
            continue
    raise ValueError(f"No se pudo leer el CSV: {last_err}")


def dataframe_to_postgres(
    df: pd.DataFrame,
    engine: "Engine",
    table_name: str,
    schema: str = "public",
    if_exists: str = "replace",
    chunksize: int = 500,
) -> tuple[int, int]:
    """
    Carga el DataFrame en public.<tabla> con transacción explícita.
    Paquetes pequeños (500) + method='multi' para inserciones eficientes pero controladas.
    Devuelve (filas en el DataFrame, filas contadas en BD tras COMMIT).
    """
    # Siempre public: evita confusiones con otros esquemas.
    schema = "public"
    table_name = _validate_sql_identifier(table_name)
    expected = len(df)

    def _count_rows() -> int:
        with engine.connect() as conn:
            r = conn.execute(
                text(f"SELECT COUNT(*) FROM public.{table_name}")
            ).scalar()
        return int(r or 0)

    rows_before = 0
    if if_exists == "append" and table_exists(engine, table_name):
        rows_before = _count_rows()

    try:
        with engine.begin() as conn:
            # replace: limpiar antes de crear de nuevo (evita choques de esquema / restos)
            if if_exists == "replace":
                conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name} CASCADE"))
            # Tras DROP, siempre append (tabla nueva); en modo append, agregar a la existente
            df.to_sql(
                table_name,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                method="multi",
            )
    except Exception as e:
        raise RuntimeError(
            f"Falló la carga en la base de datos (public.{table_name}): {e}"
        ) from e

    rows_after = _count_rows()

    if if_exists == "replace":
        if rows_after != expected:
            raise RuntimeError(
                f"DISCREPANCIA: se intentaron subir {expected:,} filas pero en la nube "
                f"hay {rows_after:,} en public.{table_name}."
            )
    else:
        inserted = rows_after - rows_before
        if inserted != expected:
            raise RuntimeError(
                f"DISCREPANCIA: se esperaban {expected:,} filas nuevas; "
                f"incremento observado: {inserted:,} (antes {rows_before:,}, después {rows_after:,})."
            )

    return expected, rows_after


def subir_a_postgresql(
    df: pd.DataFrame,
    tabla_nombre: str,
    *,
    if_exists: str = "replace",
) -> tuple[int, int]:
    """
    Fuerza bruta controlada: mismo motor que el tablero, paquetes de 500, verificación COUNT(*).
    """
    engine = get_engine()
    return dataframe_to_postgres(df, engine, tabla_nombre, if_exists=if_exists)


# ==========================================
# 3. BASE DE DATOS
# ==========================================
def _postgres_params_from_streamlit() -> Optional[Dict[str, object]]:
    try:
        if hasattr(st, "secrets") and "postgres" in st.secrets:
            s = st.secrets["postgres"]
            return {
                "host": s.get("host", POSTGRES_DEFAULTS["host"]),
                "port": int(s.get("port", POSTGRES_DEFAULTS["port"])),
                "user": s.get("user", POSTGRES_DEFAULTS["user"]),
                "password": s.get("password", POSTGRES_DEFAULTS["password"]),
                "database": s.get("database", POSTGRES_DEFAULTS["database"]),
            }
    except Exception:
        pass
    return None


def get_postgres_params() -> Dict[str, object]:
    p = _postgres_params_from_streamlit()
    if p:
        return p
    return dict(POSTGRES_DEFAULTS)


def build_connection_url(params: Optional[Dict[str, object]] = None) -> str:
    p = params or get_postgres_params()
    pw = p.get("password") or ""
    user = str(p["user"])
    host = str(p["host"])
    port = int(p["port"])
    db = str(p["database"])
    # Caracteres especiales en usuario/clave no rompen la URL
    uq = quote_plus(user)
    pq = quote_plus(pw)
    url = f"postgresql://{uq}:{pq}@{host}:{port}/{db}"
    # Pooler / Supabase en la nube requieren TLS
    if "supabase" in host.lower():
        url = f"{url}?sslmode=require"
    return url


def get_engine() -> "Engine":
    params = get_postgres_params()
    engine = create_engine(
        build_connection_url(params),
        connect_args={"client_encoding": "utf8"},
        pool_pre_ping=True,
    )
    inicializar_base_de_datos(engine)
    return engine


def inicializar_base_de_datos(engine: "Engine") -> None:
    """Crea las tablas necesarias si no existen (logística + agendamientos parciales)."""
    _validate_sql_identifier(COMPLEMENTARIOS_TABLE)
    _validate_sql_identifier(AGENDAMIENTOS_TABLE)
    _validate_sql_identifier(CATALOGO_STOCK_TABLE)
    sql_complementaria = text(
        f"""
        CREATE TABLE IF NOT EXISTS public.{COMPLEMENTARIOS_TABLE} (
            nro_orden_compra VARCHAR(255) NOT NULL,
            descripcion_producto TEXT NOT NULL,
            codigo_siciap VARCHAR(100),
            lugar_entrega VARCHAR(255),
            cantidad_entregada NUMERIC DEFAULT 0,
            ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (nro_orden_compra, descripcion_producto)
        );
        """
    )
    sql_agendamientos = text(
        f"""
        CREATE TABLE IF NOT EXISTS public.{AGENDAMIENTOS_TABLE} (
            id SERIAL PRIMARY KEY,
            nro_orden_compra VARCHAR(255) NOT NULL,
            descripcion_producto TEXT NOT NULL,
            fecha_agendada DATE NOT NULL,
            cantidad_agendada NUMERIC NOT NULL,
            cantidad_pallets NUMERIC,
            lugar_entrega VARCHAR(255),
            estado VARCHAR(50) DEFAULT 'Pendiente',
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    sql_catalogo = text(
        f"""
        CREATE TABLE IF NOT EXISTS public.{CATALOGO_STOCK_TABLE} (
            codigo_siciap VARCHAR(100) NOT NULL PRIMARY KEY,
            descripcion_oficial TEXT NOT NULL
        );
        """
    )
    with engine.begin() as conn:
        conn.execute(sql_complementaria)
        conn.execute(sql_agendamientos)
        conn.execute(sql_catalogo)


def _validate_sql_identifier(name: str) -> str:
    if not name or not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError("Identificador SQL no permitido (use solo letras, números y _).")
    return name


def read_table_sql(engine: "Engine", table: str) -> pd.DataFrame:
    table = _validate_sql_identifier(table)
    sql = f'SELECT * FROM public."{table}"'
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def ensure_datos_complementarios_table(engine: "Engine") -> None:
    """Alias idempotente: misma lógica que inicializar_base_de_datos."""
    inicializar_base_de_datos(engine)


def _read_datos_complementarios(engine: "Engine") -> pd.DataFrame:
    cols = [
        "nro_orden_compra",
        "descripcion_producto",
        "codigo_siciap",
        "lugar_entrega",
        "cantidad_entregada",
        "ultima_actualizacion",
    ]
    if not table_exists(engine, COMPLEMENTARIOS_TABLE):
        return pd.DataFrame(columns=cols)
    sql = text(f"SELECT * FROM public.{COMPLEMENTARIOS_TABLE}")
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def merge_con_datos_complementarios(
    df_principal: pd.DataFrame, df_comp: pd.DataFrame
) -> pd.DataFrame:
    """Left join por (nro_orden_compra, n5) ↔ (nro_orden_compra, descripcion_producto)."""
    out = df_principal.copy()
    if "nro_orden_compra" not in out.columns or "n5" not in out.columns:
        return out

    if df_comp is None or df_comp.empty:
        if "codigo_siciap" not in out.columns:
            out["codigo_siciap"] = ""
        if "lugar_entrega" not in out.columns:
            out["lugar_entrega"] = ""
        if "cantidad_entregada" not in out.columns:
            out["cantidad_entregada"] = 0
        return out

    left = out.copy()
    left["_m_oc"] = left["nro_orden_compra"].astype(str).str.strip()
    left["_m_n5"] = left["n5"].astype(str).str.strip()

    right = df_comp.copy()
    right["_m_oc"] = right["nro_orden_compra"].astype(str).str.strip()
    right["_m_dp"] = right["descripcion_producto"].astype(str).str.strip()
    join_tmp = {"_m_oc", "_m_dp"}
    skip_src = join_tmp | {"nro_orden_compra", "descripcion_producto"}
    val_from_comp = [c for c in right.columns if c not in skip_src]
    right_sub = right[["_m_oc", "_m_dp"] + val_from_comp]

    merged = left.merge(
        right_sub,
        how="left",
        left_on=["_m_oc", "_m_n5"],
        right_on=["_m_oc", "_m_dp"],
        suffixes=("", "_log"),
    )
    merged = merged.drop(columns=[c for c in ("_m_oc", "_m_n5", "_m_dp") if c in merged.columns])

    for col in ("codigo_siciap", "lugar_entrega", "cantidad_entregada"):
        alt = f"{col}_log"
        if alt in merged.columns:
            if col in merged.columns:
                merged[col] = merged[alt].combine_first(merged[col])
            else:
                merged[col] = merged[alt]
            merged = merged.drop(columns=[alt], errors="ignore")

    for col, default in (("codigo_siciap", ""), ("lugar_entrega", ""), ("cantidad_entregada", 0)):
        if col not in merged.columns:
            merged[col] = default

    return merged


def obtener_datos_completos(engine: "Engine", tabla_principal: str) -> pd.DataFrame:
    """Lee la tabla principal y une avances de logística desde datos_complementarios_oc."""
    df_principal = read_table_sql(engine, tabla_principal)
    df_comp = _read_datos_complementarios(engine)
    return merge_con_datos_complementarios(df_principal, df_comp)


def table_exists(engine: "Engine", table: str, schema: str = "public") -> bool:
    q = text(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = :table
        """
    )
    with engine.connect() as conn:
        row = conn.execute(q, {"schema": schema, "table": table}).fetchone()
    return row is not None


def obtener_agendamientos(
    engine: "Engine", nro_oc: str, descripcion_producto: str
) -> pd.DataFrame:
    """Historial de agendamientos para una OC y descripción de producto."""
    if not table_exists(engine, AGENDAMIENTOS_TABLE):
        return pd.DataFrame(
            columns=[
                "id",
                "fecha_agendada",
                "cantidad_agendada",
                "cantidad_pallets",
                "lugar_entrega",
                "estado",
                "fecha_creacion",
            ]
        )
    sql = text(
        f"""
        SELECT id, fecha_agendada, cantidad_agendada, cantidad_pallets,
               lugar_entrega, estado, fecha_creacion
        FROM public.{AGENDAMIENTOS_TABLE}
        WHERE nro_orden_compra = :oc AND descripcion_producto = :dp
        ORDER BY fecha_creacion DESC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"oc": str(nro_oc), "dp": str(descripcion_producto)})


def guardar_agendamiento(
    engine: "Engine",
    nro_oc: str,
    descripcion_producto: str,
    fecha_agendada: date,
    cantidad_agendada: float,
    cantidad_pallets: Optional[float],
    lugar_entrega: str,
) -> None:
    """Inserta un agendamiento pendiente."""
    _validate_sql_identifier(AGENDAMIENTOS_TABLE)
    pallets_val = float(cantidad_pallets) if cantidad_pallets is not None else None
    ins = text(
        f"""
        INSERT INTO public.{AGENDAMIENTOS_TABLE} (
            nro_orden_compra, descripcion_producto, fecha_agendada,
            cantidad_agendada, cantidad_pallets, lugar_entrega, estado
        ) VALUES (
            :oc, :dp, :fecha, :cant, :pallets, :lugar, 'Pendiente'
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(
            ins,
            {
                "oc": str(nro_oc),
                "dp": str(descripcion_producto),
                "fecha": fecha_agendada,
                "cant": float(cantidad_agendada),
                "pallets": pallets_val,
                "lugar": (lugar_entrega or "")[:255],
            },
        )


def confirmar_recepcion_agendamiento(engine: "Engine", agendamiento_id: int) -> None:
    """
    Marca el agendamiento como Entregado y suma cantidad_agendada a datos_complementarios_oc
    en una sola transacción.
    """
    _validate_sql_identifier(AGENDAMIENTOS_TABLE)
    _validate_sql_identifier(COMPLEMENTARIOS_TABLE)
    tbl_c = COMPLEMENTARIOS_TABLE
    upsert_comp = text(
        f"""
        INSERT INTO public.{tbl_c} (
            nro_orden_compra, descripcion_producto, codigo_siciap, lugar_entrega, cantidad_entregada
        ) VALUES (:oc, :dp, '', '', :delta)
        ON CONFLICT (nro_orden_compra, descripcion_producto) DO UPDATE SET
            cantidad_entregada = COALESCE(
                datos_complementarios_oc.cantidad_entregada, 0
            ) + EXCLUDED.cantidad_entregada,
            ultima_actualizacion = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as conn:
        row = (
            conn.execute(
                text(
                    f"""
                    SELECT nro_orden_compra, descripcion_producto, cantidad_agendada, estado
                    FROM public.{AGENDAMIENTOS_TABLE}
                    WHERE id = :id
                    FOR UPDATE
                    """
                ),
                {"id": agendamiento_id},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ValueError("Agendamiento no encontrado.")
        if str(row["estado"]).strip() != "Pendiente":
            raise ValueError("Solo se puede confirmar un agendamiento en estado Pendiente.")
        delta = float(row["cantidad_agendada"])
        oc = str(row["nro_orden_compra"])
        dp = str(row["descripcion_producto"])
        conn.execute(
            text(
                f"UPDATE public.{AGENDAMIENTOS_TABLE} SET estado = 'Entregado' WHERE id = :id"
            ),
            {"id": agendamiento_id},
        )
        conn.execute(upsert_comp, {"oc": oc, "dp": dp, "delta": delta})


def actualizar_estado_agendamiento(
    engine: "Engine", agendamiento_id: int, nuevo_estado: str
) -> None:
    """Actualiza estado; si pasa a Entregado, sincroniza cantidad entregada en datos_complementarios_oc."""
    if nuevo_estado.strip() == "Entregado":
        confirmar_recepcion_agendamiento(engine, agendamiento_id)
        return
    _validate_sql_identifier(AGENDAMIENTOS_TABLE)
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE public.{AGENDAMIENTOS_TABLE} SET estado = :e WHERE id = :id"),
            {"e": nuevo_estado[:50], "id": agendamiento_id},
        )


def get_uoc_central_data(engine: "Engine") -> pd.DataFrame:
    """Consulta filtrada MSPBS – UOC Nivel Central (D.O.C) y convenios COVID indicados."""
    query = text(
        """
        SELECT
            fecha_orden_compra,
            nro_orden_compra,
            nombre_entidad,
            proveedor,
            ruc_completo,
            n5,
            cantidad,
            precio_unitario,
            precio_total,
            NOW() AS ultima_consulta
        FROM contrataciones_datos
        WHERE entidad = 'Ministerio de Salud Pública y Bienestar Social'
          AND nombre_entidad = 'Uoc Nro 1  Nivel Central (D.O.C) MSPBS / Ministerio de Salud Pública y Bienestar Social'
          AND (
              id LIKE '%382392%' OR id LIKE '%386038%'
              OR id LIKE '%395261%' OR id LIKE '%400275%'
          )
        ORDER BY fecha_orden_compra DESC, proveedor ASC
        """
    )
    with engine.connect() as conn:
        df_principal = pd.read_sql(query, conn)
    df_comp = _read_datos_complementarios(engine)
    return merge_con_datos_complementarios(df_principal, df_comp)


def refrescar_datos_tablero_en_sesion(engine: "Engine") -> None:
    """Recarga el DataFrame activo en session_state tras cambios en la BD."""
    active = st.session_state.get("ss_pg_active")
    if active == "uoc":
        st.session_state["ss_df_uoc"] = get_uoc_central_data(engine)
    elif active == "pg_full":
        st.session_state["ss_df_pg_full"] = obtener_datos_completos(
            engine, os.environ.get("TV_TABLE", DEFAULT_TABLE)
        )


# ==========================================
# 4. DASHBOARD
# ==========================================
def _pick(df: pd.DataFrame, candidates: tuple[str, ...]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    fc = _pick(out, ("fecha_orden_compra", "fecha_orden"))
    if fc:
        out[fc] = pd.to_datetime(out[fc], errors="coerce")
    for col in ("precio_total", "precio_unitario", "monto", "cantidad"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _aplicar_catalogo_stock_critico(d0: pd.DataFrame) -> pd.DataFrame:
    """LEFT JOIN con catalogo_stock_critico: si hay SICIAP, reemplaza n5 por descripcion_oficial."""
    if "n5" not in d0.columns or "codigo_siciap" not in d0.columns:
        return d0
    try:
        engine = get_engine()
    except Exception:
        return d0
    if not table_exists(engine, CATALOGO_STOCK_TABLE):
        return d0
    try:
        with engine.connect() as conn:
            cat = pd.read_sql(
                text(
                    f"SELECT codigo_siciap, descripcion_oficial FROM public.{CATALOGO_STOCK_TABLE}"
                ),
                conn,
            )
    except Exception:
        return d0
    if cat.empty or "descripcion_oficial" not in cat.columns:
        return d0
    out = d0.copy()
    out["_sic_join"] = out["codigo_siciap"].astype(str).str.strip()
    cat = cat.copy()
    cat["_sic_join"] = cat["codigo_siciap"].astype(str).str.strip()
    cat = cat.drop_duplicates(subset=["_sic_join"], keep="last")
    merged = out.merge(
        cat[["_sic_join", "descripcion_oficial"]],
        on="_sic_join",
        how="left",
    )
    merged = merged.drop(columns=["_sic_join"], errors="ignore")
    mask = (
        merged["descripcion_oficial"].notna()
        & merged["descripcion_oficial"].astype(str).str.strip().ne("")
        & merged["codigo_siciap"].astype(str).str.strip().ne("")
    )
    merged.loc[mask, "n5"] = merged.loc[mask, "descripcion_oficial"].astype(str).str.strip()
    return merged.drop(columns=["descripcion_oficial"], errors="ignore")


def render_tablero(
    df: pd.DataFrame,
    *,
    titulo: str = "Tablero",
    key_prefix: str = "tv",
    persist_complementarios_db: bool = False,
) -> None:
    if df is None or df.empty:
        st.warning("No hay datos para el tablero.")
        return

    def k(name: str) -> str:
        return f"{key_prefix}_{name}"

    st.subheader(f"📊 {titulo}")
    d0 = _coerce_types(df)

    if "codigo_siciap" not in d0.columns:
        d0["codigo_siciap"] = ""
    if "lugar_entrega" not in d0.columns:
        d0["lugar_entrega"] = ""

    d0 = _aplicar_catalogo_stock_critico(d0)

    col_monto = _pick(d0, ("precio_total", "monto"))
    col_prov = _pick(d0, ("proveedor",))
    col_fecha = _pick(d0, ("fecha_orden_compra", "fecha_orden"))
    col_ruc = _pick(d0, ("ruc_completo", "ruc"))
    col_nro = _pick(d0, ("nro_orden_compra",))
    col_ent = _pick(d0, ("entidad", "convocante", "nombre_entidad"))

    if col_prov:
        d0[col_prov] = (
            d0[col_prov]
            .astype(str)
            .apply(lambda x: re.sub(r"\s+\d+-\d+$", "", x).strip())
        )

    with st.expander("Filtros del tablero", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            q_texto = st.text_input("Buscar en toda la tabla (texto)", "", key=k("q_global"))
            q_siciap = st.text_input("Código Siciap contiene", "", key=k("q_siciap"))
        with c2:
            if col_nro:
                q_nro = st.text_input("Nº orden de compra contiene", "", key=k("q_nro"))
            else:
                q_nro = ""
            q_lugar = st.text_input("Lugar de entrega contiene", "", key=k("q_lugar"))
        with c3:
            if col_ent:
                ents = sorted(d0[col_ent].dropna().astype(str).unique().tolist())[:500]
                ent_sel = st.multiselect("Entidad / convocante", ents, default=[], key=k("ent"))
            else:
                ent_sel = []

        prov_col, ruc_col = st.columns(2)
        with prov_col:
            if col_prov:
                provs = ["(todos)"] + sorted(d0[col_prov].dropna().astype(str).unique().tolist())[:400]
                prov_f = st.selectbox("Proveedor", provs, key=k("prov"))
            else:
                prov_f = "(todos)"
        with ruc_col:
            if col_ruc:
                rucs = ["(todos)"] + sorted(d0[col_ruc].dropna().astype(str).unique().tolist())[:400]
                ruc_f = st.selectbox("RUC", rucs, key=k("ruc"))
            else:
                ruc_f = "(todos)"

        if col_fecha and d0[col_fecha].notna().any():
            rmin = d0[col_fecha].min().date()
            rmax = d0[col_fecha].max().date()
            d_ini, d_fin = st.date_input("Rango de fechas", (rmin, rmax), key=k("fechas"))
        else:
            d_ini = d_fin = None

    d = d0.copy()
    if q_texto.strip():
        mask = pd.Series(False, index=d.index)
        for c in d.columns:
            mask = mask | d[c].astype(str).str.contains(q_texto.strip(), case=False, na=False)
        d = d.loc[mask]
    if q_siciap.strip():
        d = d[d["codigo_siciap"].astype(str).str.contains(q_siciap.strip(), case=False, na=False)]
    if q_lugar.strip():
        d = d[d["lugar_entrega"].astype(str).str.contains(q_lugar.strip(), case=False, na=False)]
    if col_nro and q_nro.strip():
        d = d[d[col_nro].astype(str).str.contains(q_nro.strip(), case=False, na=False)]
    if col_ent and ent_sel:
        d = d[d[col_ent].astype(str).isin(ent_sel)]
    if col_prov and prov_f != "(todos)":
        d = d[d[col_prov].astype(str) == prov_f]
    if col_ruc and ruc_f != "(todos)":
        d = d[d[col_ruc].astype(str) == ruc_f]
    if col_fecha and d_ini is not None and d_fin is not None:
        dd = d[col_fecha].dt.date
        d = d[(dd >= d_ini) & (dd <= d_fin)]

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Registros (filtrados)", f"{len(d):,}")
    with m2:
        if col_monto and d[col_monto].notna().any():
            st.metric("Suma montos", f"₲ {d[col_monto].sum():,.0f}".replace(",", "."))
        else:
            st.metric("Suma montos", "—")
    with m3:
        if col_prov:
            st.metric("Proveedores distintos", f"{d[col_prov].nunique():,}")
        else:
            st.metric("Proveedores distintos", "—")
    with m4:
        if col_monto and d[col_monto].notna().any() and len(d) > 0:
            st.metric("Promedio monto", f"₲ {d[col_monto].mean():,.0f}".replace(",", "."))
        else:
            st.metric("Promedio monto", "—")

    if col_prov and col_monto and not d.empty:
        st.markdown("#### Montos de Emisiones a Proveedores")

        top = d.groupby(col_prov, dropna=True)[col_monto].sum().sort_values(ascending=False).head(15)
        chart_df = top.reset_index()
        chart_df.columns = ["proveedor", "monto"]
        chart_df["monto_formateado"] = chart_df["monto"].apply(
            lambda x: f"₲ {x:,.0f}".replace(",", ".")
        )

        bars = alt.Chart(chart_df).mark_bar(color="#F47A20").encode(
            x=alt.X("monto:Q", title="", axis=None),
            y=alt.Y("proveedor:N", sort="-x", title=""),
            tooltip=["proveedor", "monto_formateado"],
        )

        text_labels = bars.mark_text(
            align="left",
            baseline="middle",
            dx=6,
            color="white",
            fontWeight="bold",
        ).encode(text=alt.Text("monto_formateado:N"))

        chart = (
            (bars + text_labels)
            .properties(height=450)
            .configure_axis(labelFontSize=12, titleFontSize=14)
            .configure_view(strokeWidth=0)
        )

        st.altair_chart(chart, width="stretch")

    st.markdown("#### Tabla")
    work = d.copy()
    if "cantidad_entregada" not in work.columns:
        work["cantidad_entregada"] = 0
    if "cantidad" in work.columns:
        work["cantidad"] = pd.to_numeric(work["cantidad"], errors="coerce").fillna(0)
        work["cantidad_entregada"] = pd.to_numeric(work["cantidad_entregada"], errors="coerce").fillna(0)
        work["saldo_a_entregar"] = work["cantidad"] - work["cantidad_entregada"]

    display_df = work.copy()

    if col_fecha and col_fecha in display_df.columns:
        display_df[col_fecha] = display_df[col_fecha].apply(
            lambda x: x.strftime("%d / %m / %Y - %H:%M") if pd.notna(x) else ""
        )

    for col in ("cantidad", "cantidad_entregada", "saldo_a_entregar", "precio_unitario", col_monto):
        if col and col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce")
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,.0f}".replace(",", ".") if pd.notna(x) else ""
            )

    rename_map: dict[str, str] = {}
    if col_fecha:
        rename_map[col_fecha] = "Fecha / hora"
    if col_nro:
        rename_map[col_nro] = "N° OC"
    if col_ent:
        rename_map[col_ent] = "Nombre de UOC - Entidad"
    if col_prov:
        rename_map[col_prov] = "Proveedor"
    if "n5" in display_df.columns:
        rename_map["n5"] = "Descripcion del Producto"
    if "codigo_siciap" in display_df.columns:
        rename_map["codigo_siciap"] = "Codigo Siciap"
    if "cantidad" in display_df.columns:
        rename_map["cantidad"] = "Emitida"
    if "cantidad_entregada" in display_df.columns:
        rename_map["cantidad_entregada"] = "Entregada"
    if "saldo_a_entregar" in display_df.columns:
        rename_map["saldo_a_entregar"] = "Saldo"
    if "precio_unitario" in display_df.columns:
        rename_map["precio_unitario"] = "Precio Unitario"
    if col_monto:
        rename_map[col_monto] = "Precio Total"
    if "lugar_entrega" in display_df.columns:
        rename_map["lugar_entrega"] = "Lugar de Entrega"

    display_df = display_df.rename(columns=rename_map)

    cols_order = [
        "Fecha / hora",
        "N° OC",
        "Nombre de UOC - Entidad",
        "Proveedor",
        "Descripcion del Producto",
        "Codigo Siciap",
        "Emitida",
        "Entregada",
        "Saldo",
        "Precio Unitario",
        "Precio Total",
        "Lugar de Entrega",
    ]
    final_cols = [c for c in cols_order if c in display_df.columns]
    tabla_vista = display_df[final_cols].copy().reset_index(drop=True)

    st.markdown("#### Tabla de registros")
    st.caption(
        "Seleccioná la casilla a la izquierda de una fila para preparar la carga de datos complementarios."
    )

    cc: Dict[str, object] = {}
    if "Fecha / hora" in final_cols:
        cc["Fecha / hora"] = st.column_config.TextColumn(width="small")
    if "Nombre de UOC - Entidad" in final_cols:
        cc["Nombre de UOC - Entidad"] = st.column_config.TextColumn(width="small")
    if "Descripcion del Producto" in final_cols:
        cc["Descripcion del Producto"] = st.column_config.TextColumn(width="medium")
    if "Proveedor" in final_cols:
        cc["Proveedor"] = st.column_config.TextColumn(width="medium")
    if "Lugar de Entrega" in final_cols:
        cc["Lugar de Entrega"] = st.column_config.TextColumn(width="medium")

    eventos_tabla = st.dataframe(
        tabla_vista,
        width="stretch",
        height=450,
        hide_index=True,
        row_height=42,
        column_config=cc if cc else None,
        key=k("tabla_sel"),
        on_select="rerun",
        selection_mode="single-row",
    )

    rows_sel: list[int] = []
    if hasattr(eventos_tabla, "selection") and eventos_tabla.selection is not None:
        sel = eventos_tabla.selection
        if hasattr(sel, "rows"):
            rows_sel = list(sel.rows)
        elif isinstance(sel, dict):
            rows_sel = list(sel.get("rows", []) or [])

    # Formulario inline al seleccionar una fila (sin cambiar de pestaña)
    if rows_sel and col_nro and "N° OC" in display_df.columns:
        indice_real = int(rows_sel[0])
        if 0 <= indice_real < len(display_df):
            oc_seleccionada = display_df.iloc[indice_real]["N° OC"]
            oc_str = str(oc_seleccionada) if pd.notna(oc_seleccionada) else ""

            st.markdown("---")
            st.markdown(f"### 📝 Carga de datos complementarios — OC: **{oc_str}**")

            d_edit = d[d[col_nro].astype(str) == oc_str].copy()
            for col in ("codigo_siciap", "lugar_entrega"):
                if col not in d_edit.columns:
                    d_edit[col] = ""

            orden_key = re.sub(r"[^\w]", "_", oc_str)[:60]

            engine_ag = None
            if persist_complementarios_db:
                try:
                    engine_ag = get_engine()
                except Exception:
                    engine_ag = None

            for form_i, (_, row) in enumerate(d_edit.iterrows()):
                raw_n5 = row.get("n5")
                desc_db = "" if pd.isna(raw_n5) else str(raw_n5).strip()
                descripcion = desc_db if desc_db else "Sin descripción"
                cant_raw = row.get("cantidad", 0)
                try:
                    cant_num = float(cant_raw)
                    cant_txt = f"{cant_num:,.0f}".replace(",", ".")
                except (TypeError, ValueError):
                    cant_num = 0.0
                    cant_txt = str(cant_raw)

                with st.container():
                    prov_txt = str(row.get("proveedor", "") or "").strip() or "N/A"
                    fc_val = (
                        row.get(col_fecha)
                        if col_fecha and col_fecha in row
                        else row.get("fecha_orden_compra")
                    )
                    if pd.isna(fc_val) or fc_val is None:
                        fecha_txt = "N/A"
                    elif hasattr(fc_val, "strftime"):
                        try:
                            fecha_txt = fc_val.strftime("%d/%m/%Y %H:%M")
                        except Exception:
                            fecha_txt = str(fc_val)
                    else:
                        fecha_txt = str(fc_val)

                    st.markdown(f"**Ítem:** {descripcion}")
                    st.caption(
                        f"**Proveedor:** {prov_txt} | **Fecha emisión:** {fecha_txt} | "
                        f"**Cantidad emitida:** {cant_txt}"
                    )

                    row_key = f"{k('form')}_{orden_key}_{form_i}"
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.text_input(
                            "Código Siciap",
                            value=str(row["codigo_siciap"]) if pd.notna(row["codigo_siciap"]) else "",
                            key=f"{row_key}_siciap",
                        )
                    with c2:
                        st.text_input(
                            "Lugar de entrega",
                            value=str(row["lugar_entrega"]) if pd.notna(row["lugar_entrega"]) else "",
                            key=f"{row_key}_lugar",
                        )
                    with c3:
                        ce_default = row.get("cantidad_entregada", 0)
                        try:
                            ce_default = float(ce_default) if pd.notna(ce_default) else 0.0
                        except (TypeError, ValueError):
                            ce_default = 0.0
                        st.number_input(
                            "Cantidad entregada",
                            min_value=0.0,
                            value=float(ce_default),
                            step=1.0,
                            key=f"{row_key}_cant_ent",
                        )

                    with st.expander("📅 Agendamiento de Entregas Parciales", expanded=False):
                        if not engine_ag:
                            st.caption(
                                "Los agendamientos solo están disponibles cuando trabajás "
                                "con datos desde **Base de Datos (Supabase)**."
                            )
                        else:
                            emitida_f = float(cant_num)
                            df_ag = obtener_agendamientos(engine_ag, oc_str, desc_db)
                            total_agendado = (
                                float(df_ag["cantidad_agendada"].sum())
                                if not df_ag.empty and "cantidad_agendada" in df_ag.columns
                                else 0.0
                            )
                            try:
                                ent_bd = float(row.get("cantidad_entregada", 0) or 0)
                            except (TypeError, ValueError):
                                ent_bd = 0.0
                            saldo_agendar = max(0.0, emitida_f - total_agendado)
                            m1, m2, m3 = st.columns(3)
                            with m1:
                                st.metric("Cantidad emitida (ítem)", f"{emitida_f:,.0f}".replace(",", "."))
                            with m2:
                                st.metric(
                                    "Ya agendado (suma entregas planificadas)",
                                    f"{total_agendado:,.0f}".replace(",", "."),
                                )
                            with m3:
                                st.metric(
                                    "Saldo pendiente de agendar",
                                    f"{saldo_agendar:,.0f}".replace(",", "."),
                                )
                            st.caption(
                                f"Cantidad entregada acumulada en BD (complementarios / recepciones): "
                                f"{ent_bd:,.0f}".replace(",", ".")
                            )

                            col_ag1, col_ag2 = st.columns(2)
                            with col_ag1:
                                st.markdown("**Nueva entrega**")
                                ag_fecha = st.date_input(
                                    "Fecha agendada",
                                    value=date.today(),
                                    key=k(f"ag_fecha_{orden_key}_{form_i}"),
                                )
                                ag_cant = st.number_input(
                                    "Cantidad agendada",
                                    min_value=0.0,
                                    value=min(1000.0, saldo_agendar) if saldo_agendar > 0 else 0.0,
                                    step=1000.0,
                                    key=k(f"ag_cant_{orden_key}_{form_i}"),
                                )
                                ag_pallets = st.number_input(
                                    "Cantidad de pallets (opcional)",
                                    min_value=0.0,
                                    value=0.0,
                                    step=1.0,
                                    key=k(f"ag_pal_{orden_key}_{form_i}"),
                                )
                                ag_lugar = st.text_input(
                                    "Lugar de entrega (agendamiento)",
                                    value=str(row.get("lugar_entrega") or ""),
                                    key=k(f"ag_lugar_{orden_key}_{form_i}"),
                                )
                                if st.button(
                                    "Agendar entrega",
                                    key=k(f"ag_btn_{orden_key}_{form_i}"),
                                ):
                                    try:
                                        if ag_cant <= 0:
                                            st.error("La cantidad agendada debe ser mayor a cero.")
                                        elif total_agendado + ag_cant > emitida_f + 1e-9:
                                            st.error(
                                                "La suma de agendamientos no puede superar la cantidad emitida."
                                            )
                                        else:
                                            pallets_ins = (
                                                float(ag_pallets) if ag_pallets > 0 else None
                                            )
                                            guardar_agendamiento(
                                                engine_ag,
                                                oc_str,
                                                desc_db,
                                                ag_fecha,
                                                float(ag_cant),
                                                pallets_ins,
                                                ag_lugar.strip(),
                                            )
                                            st.success("Agendamiento registrado.")
                                            refrescar_datos_tablero_en_sesion(engine_ag)
                                            st.rerun()
                                    except Exception as ex_ag:
                                        st.error(str(ex_ag))

                            with col_ag2:
                                st.markdown("**Historial**")
                                if df_ag.empty:
                                    st.caption("Sin agendamientos para este ítem.")
                                else:
                                    show_ag = df_ag.copy()
                                    st.dataframe(
                                        show_ag,
                                        width="stretch",
                                        hide_index=True,
                                        key=k(f"ag_tbl_{orden_key}_{form_i}"),
                                    )
                                    for _, ag in df_ag.iterrows():
                                        aid = int(ag["id"])
                                        est = str(ag.get("estado", "")).strip()
                                        if est == "Pendiente":
                                            if st.button(
                                                "Confirmar recepción",
                                                key=k(f"ag_cfm_{aid}_{form_i}"),
                                            ):
                                                try:
                                                    actualizar_estado_agendamiento(
                                                        engine_ag, aid, "Entregado"
                                                    )
                                                    st.success("Recepción confirmada.")
                                                    refrescar_datos_tablero_en_sesion(engine_ag)
                                                    st.rerun()
                                                except Exception as ex_c:
                                                    st.error(str(ex_c))

                    st.markdown("---")

            if persist_complementarios_db:
                if st.button(
                    "💾 Guardar avances en base de datos",
                    type="primary",
                    key=k("btn_guardar_oc"),
                ):
                    upsert_sql = text(
                        f"""
                        INSERT INTO public.{COMPLEMENTARIOS_TABLE} (
                            nro_orden_compra, descripcion_producto, codigo_siciap,
                            lugar_entrega, cantidad_entregada
                        ) VALUES (
                            :nro_orden_compra, :descripcion_producto, :codigo_siciap,
                            :lugar_entrega, :cantidad_entregada
                        )
                        ON CONFLICT (nro_orden_compra, descripcion_producto)
                        DO UPDATE SET
                            codigo_siciap = EXCLUDED.codigo_siciap,
                            lugar_entrega = EXCLUDED.lugar_entrega,
                            cantidad_entregada = EXCLUDED.cantidad_entregada,
                            ultima_actualizacion = CURRENT_TIMESTAMP
                        """
                    )
                    try:
                        engine = get_engine()
                        with engine.begin() as conn:
                            for form_i, (_, row) in enumerate(d_edit.iterrows()):
                                raw_n5 = row.get("n5")
                                desc_str = "" if pd.isna(raw_n5) else str(raw_n5).strip()

                                row_key = f"{k('form')}_{orden_key}_{form_i}"
                                val_siciap = str(
                                    st.session_state.get(f"{row_key}_siciap", "") or ""
                                ).strip()
                                val_lugar = str(
                                    st.session_state.get(f"{row_key}_lugar", "") or ""
                                ).strip()
                                val_ent = st.session_state.get(f"{row_key}_cant_ent", 0)
                                try:
                                    val_ent_f = float(val_ent)
                                except (TypeError, ValueError):
                                    val_ent_f = 0.0

                                conn.execute(
                                    upsert_sql,
                                    {
                                        "nro_orden_compra": oc_str,
                                        "descripcion_producto": desc_str,
                                        "codigo_siciap": val_siciap,
                                        "lugar_entrega": val_lugar,
                                        "cantidad_entregada": val_ent_f,
                                    },
                                )

                        st.success(f"Avances guardados para la OC **{oc_str}**.")
                        refrescar_datos_tablero_en_sesion(engine)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al guardar en la base de datos: {e}")
            else:
                st.caption(
                    "Los avances en Siciap / entrega solo se guardan en la base de datos "
                    "cuando cargás datos desde la sección Base de Datos (no desde CSV)."
                )

    st.download_button(
        "Descargar CSV filtrado",
        work.to_csv(index=False, sep=";").encode("utf-8-sig"),
        f"tienda_virtual_filtrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv",
        key=k("dl"),
    )


# ==========================================
# 5. APLICACIÓN PRINCIPAL
# ==========================================
def check_password() -> bool:
    """Retorna True si el usuario ingresó las credenciales correctas.

    Roles: ``admin`` (carga CSV, catálogo stock) y ``operativo`` (solo tablero / BD).
    Podés sobrescribir usuario y contraseña de admin con TV_ADMIN_USER y TV_ADMIN_PASSWORD.

    Nota: credenciales fijas son adecuadas solo para equipos cerrados; para mayor
    seguridad usá variables de entorno o un proveedor de identidad.
    """
    admin_user = os.environ.get("TV_ADMIN_USER", "bruno_admin")
    admin_pass = os.environ.get("TV_ADMIN_PASSWORD", "admin_dggies_2026")

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if "role" not in st.session_state:
        st.session_state["role"] = None

    if st.session_state["authenticated"]:
        return True

    st.sidebar.title("Acceso restringido")
    user = st.sidebar.text_input("Usuario", key="auth_user")
    password = st.sidebar.text_input("Contraseña", type="password", key="auth_pass")

    if st.sidebar.button("Entrar", key="auth_btn"):
        if user == admin_user and password == admin_pass:
            st.session_state["authenticated"] = True
            st.session_state["role"] = "admin"
            st.rerun()
        elif user == "stock_dggies" and password == "stock_dggiesmspbs":
            st.session_state["authenticated"] = True
            st.session_state["role"] = "operativo"
            st.rerun()
        else:
            st.sidebar.error("Usuario o contraseña incorrectos")
    return False


st.set_page_config(page_title="Tienda Virtual", layout="wide", page_icon="🛒")

st.title("🛒 Tienda virtual — datos DNCP")

if not check_password():
    st.warning(
        "Iniciá sesión desde el panel lateral para acceder al sistema (Stock DGGIES)."
    )
    st.stop()

# Sesiones antiguas sin rol: tratar como operativo (sin funciones de admin).
if st.session_state.get("authenticated") and st.session_state.get("role") is None:
    st.session_state["role"] = "operativo"

IS_ADMIN = st.session_state.get("role") == "admin"

if IS_ADMIN:
    tab_admin, tab_instr, tab_enlaces, tab_app = st.tabs(
        [
            "⚙️ Admin (Carga CSV)",
            "📋 Instructivo",
            "🔗 Enlaces DNCP",
            "🛒 Aplicación",
        ]
    )
else:
    tab_instr, tab_enlaces, tab_app = st.tabs(
        ["📋 Instructivo", "🔗 Enlaces DNCP", "🛒 Aplicación"]
    )

with tab_instr:
    st.subheader("Instructivo de uso")
    if IS_ADMIN:
        st.markdown(
            """
1. Descargar datos: pestaña **Enlaces DNCP** (convenios prioritarios) o el enlace general:  
   [Descarga CSV DNCP](https://www.contrataciones.gov.py/t/download/SieDocumento/10)

2. Ir a la pestaña **Admin (Carga CSV)**, subir el archivo descargado y, si corresponde, usar **Cargar este DataFrame a la Base de Datos** (sobrescribe la tabla en la nube con los datos del CSV actual).

3. Para gestionar la logística: pestaña **Aplicación** → panel lateral **Ver Reporte (MSPBS - UOC)** o **Leer tabla completa**.

4. En cada ítem podés abrir **Agendamiento de Entregas Parciales** y **Confirmar recepción** cuando corresponda.

5. **Catálogo Stock Crítico** (sidebar en **Aplicación**, solo admin): subí un **.xlsx** (DMP/MSPBS) o **CSV** con `codigo_siciap` y `descripcion_oficial` para enriquecer descripciones en el tablero.
            """
        )
    else:
        st.markdown(
            """
1. Enlaces útiles: pestaña **Enlaces DNCP** (convenios prioritarios) o la [descarga general CSV DNCP](https://www.contrataciones.gov.py/t/download/SieDocumento/10).

2. Para trabajar con los datos cargados en el sistema: pestaña **Aplicación** → panel lateral **Ver Reporte (MSPBS - UOC)** o **Leer tabla completa**.

3. En cada ítem podés abrir **Agendamiento de Entregas Parciales** y **Confirmar recepción** cuando corresponda.

La carga masiva de archivos CSV y el catálogo «Stock Crítico» las realiza el **administrador** del sistema (perfil separado).
            """
        )

with tab_enlaces:
    st.subheader("Enlaces DNCP")
    st.markdown(
        """
### Convenios prioritarios (Nivel Central / COVID)

* [ID 400275 — Uso médico lucha COVID-19 Grupo 2](https://www.contrataciones.gov.py/convenios-marco/convenio/400275-adquisicion-productos-uso-medico-lucha-covid-19-grupo-2/compras.csv)
* [ID 395261 — Midazolam y atracurio besilato](https://www.contrataciones.gov.py/convenios-marco/convenio/395261-adquisicion-midazolam-atracurio-besilato-lucha-covid-19/compras.csv)
* [ID 386038 — Productos uso médico lucha COVID-19](https://www.contrataciones.gov.py/convenios-marco/convenio/386038-adquisicion-productos-uso-medico-lucha-covid-19/compras.csv)
* [ID 382392 — Productos contingencia COVID-19](https://www.contrataciones.gov.py/convenios-marco/convenio/382392-adquisicion-productos-contingencia-covid-19/compras.csv)

### Otros convenios

Acá podés sumar enlaces internos del equipo (portal interno, carpetas compartidas, etc.) según las necesidades operativas.
        """
    )

if IS_ADMIN:
    with tab_admin:
        st.caption(
            "Solo administración: vista previa del CSV y envío a Supabase "
            "(reemplaza la tabla configurada en `TV_TABLE`)."
        )
        tabla_pg = os.environ.get("TV_TABLE", DEFAULT_TABLE)

        for _k in ("ss_df_uoc", "ss_df_pg_full", "ss_pg_active"):
            st.session_state.pop(_k, None)

        st.subheader("Cargar CSV")
        uploaded = st.file_uploader(
            "Subir CSV (tienda órdenes, reporte compras, etc.)",
            type=["csv"],
            key="admin_csv_upload",
        )
        ruta = st.text_input(
            "O ruta absoluta a un .csv (solo útil en tu PC local)",
            value="",
            key="admin_csv_path",
        )

        df: pd.DataFrame | None = None
        err = None
        if uploaded is not None:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(uploaded.getvalue())
                    path = tmp.name
                df = read_csv_smart(path)
                try:
                    os.unlink(path)
                except OSError:
                    pass
            except Exception as e:
                err = str(e)
        elif ruta.strip():
            try:
                df = read_csv_smart(ruta.strip())
            except Exception as e:
                err = str(e)

        if err:
            st.error(err)
        elif df is None:
            st.info("Subí un archivo para visualizar los datos.")
        else:
            st.success(f"{len(df):,} filas × {len(df.columns)} columnas")
            render_tablero(df, titulo="Vista desde archivo CSV", key_prefix="tv_csv_admin")

            st.markdown("---")
            st.subheader("Enviar a la base de datos (Supabase)")
            st.caption("La carga **reemplaza** por completo la tabla con los datos del CSV actual.")
            if st.button("Cargar este DataFrame a la Base de Datos", key="admin_btn_cargar_bd"):
                try:
                    esperado, verificado = subir_a_postgresql(
                        df, tabla_pg, if_exists="replace"
                    )
                    st.success(
                        f"✅ CARGA EXITOSA: **{verificado:,}** filas verificadas en la nube "
                        f"(`public.{tabla_pg}`; CSV: {esperado:,})."
                    )
                except Exception as e:
                    st.error(f"❌ Error en la carga o verificación: {e}")

with tab_app:
    st.caption(
        "Conectate a la base de datos para el tablero y la logística; "
        "al seleccionar una fila podés cargar datos complementarios debajo."
    )

    st.sidebar.markdown("---")

    if IS_ADMIN:
        with st.sidebar.expander("⚙️ Cargar catálogo Stock Crítico", expanded=False):
            st.caption(
                "Archivo **.xlsx** (Stock Crítico DMP/MSPBS) o **CSV** con columnas "
                "**codigo_siciap** y **descripcion_oficial**."
            )
            cat_up = st.file_uploader(
                "Archivo catálogo",
                type=["csv", "xlsx"],
                key="catalogo_stock_upload",
            )
            if st.button("Guardar catálogo en la nube", key="catalogo_stock_btn"):
                if cat_up is None:
                    st.error("Seleccioná un archivo (.csv o .xlsx).")
                else:
                    try:
                        cat_up.seek(0)
                        df_out = preparar_dataframe_catalogo_stock(cat_up)
                        if df_out.empty:
                            raise ValueError(
                                "No quedaron filas válidas (código y descripción no vacíos)."
                            )
                        eng = get_engine()
                        with eng.begin() as conn:
                            df_out.to_sql(
                                CATALOGO_STOCK_TABLE,
                                con=conn,
                                schema="public",
                                if_exists="replace",
                                index=False,
                                chunksize=500,
                                method="multi",
                            )
                        st.success(
                            f"Catálogo actualizado en la nube: **{len(df_out):,}** ítems."
                        )
                        st.rerun()
                    except UnicodeDecodeError:
                        st.error(
                            "No se pudo leer el CSV como UTF-8. Guardalo en UTF-8 e intentá de nuevo."
                        )
                    except Exception as e:
                        st.error(str(e))

    # Nombre de tabla fijo por código / variable de entorno TV_TABLE (sin campo en la UI)
    tabla_pg = os.environ.get("TV_TABLE", DEFAULT_TABLE)

    st.subheader("Lectura desde Base de Datos (Supabase)")

    st.sidebar.markdown("#### Consultas")
    if st.sidebar.button("Ver Reporte (MSPBS - UOC)", key="btn_uoc_central"):
        try:
            engine = get_engine()
            if not table_exists(engine, "contrataciones_datos"):
                st.warning("No existe la tabla `contrataciones_datos` en public.")
            else:
                with st.spinner("Consultando datos del Nivel Central..."):
                    df_uoc = get_uoc_central_data(engine)
                if df_uoc.empty:
                    st.session_state.pop("ss_df_uoc", None)
                    st.session_state.pop("ss_pg_active", None)
                    st.warning(
                        "No se encontraron registros con los filtros indicados."
                    )
                else:
                    st.session_state["ss_df_uoc"] = df_uoc
                    st.session_state["ss_pg_active"] = "uoc"
        except Exception as e:
            st.error(f"Error en la conexión o consulta: {e}")

    st.sidebar.markdown("---")

    if st.sidebar.button("Leer tabla completa", key="btn_lectura_tabla"):
        try:
            engine = get_engine()
            if not table_exists(engine, tabla_pg):
                st.warning(f"No existe la tabla `{tabla_pg}` en public.")
            else:
                with st.spinner("Cargando todos los registros..."):
                    df_pg = obtener_datos_completos(engine, tabla_pg)
                st.session_state["ss_df_pg_full"] = df_pg
                st.session_state["ss_pg_active"] = "pg_full"
        except Exception as e:
            st.error(str(e))

    active = st.session_state.get("ss_pg_active")
    if active == "uoc" and st.session_state.get("ss_df_uoc") is not None:
        df_uoc = st.session_state["ss_df_uoc"]
        if not df_uoc.empty:
            st.success(f"Reporte generado con éxito: {len(df_uoc):,} registros.")
            render_tablero(
                df_uoc,
                titulo="MSPBS – UOC Nivel Central (D.O.C)",
                key_prefix="tv_uoc",
                persist_complementarios_db=True,
            )
    elif active == "pg_full" and st.session_state.get("ss_df_pg_full") is not None:
        df_pg = st.session_state["ss_df_pg_full"]
        st.success(f"{len(df_pg):,} filas cargadas en total")
        render_tablero(
            df_pg,
            titulo="Vista desde Base de Datos (Supabase) — tabla completa",
            key_prefix="tv_pg",
            persist_complementarios_db=True,
        )
