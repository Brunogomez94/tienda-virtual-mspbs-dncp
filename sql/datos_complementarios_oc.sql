-- Ejecutar en Supabase SQL Editor (o cualquier cliente PostgreSQL).
-- La app también puede crear esta tabla sola la primera vez que guardás o leés con merge.

CREATE TABLE IF NOT EXISTS public.datos_complementarios_oc (
    nro_orden_compra VARCHAR(255) NOT NULL,
    descripcion_producto TEXT NOT NULL,
    codigo_siciap VARCHAR(100),
    lugar_entrega VARCHAR(255),
    cantidad_entregada NUMERIC DEFAULT 0,
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (nro_orden_compra, descripcion_producto)
);
