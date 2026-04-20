import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
import os
import sys

def obtener_fechas():
    # Obtener el primer día del mes actual
    hoy = date.today()
    fecha_hasta = hoy.replace(day=1)
    
    # Obtener el primer día del mes anterior
    ultimo_dia_mes_pasado = fecha_hasta - timedelta(days=1)
    fecha_desde = ultimo_dia_mes_pasado.replace(day=1)
    
    # Formatear para SQL
    str_desde = f"{fecha_desde} 00:00:00"
    str_hasta = f"{fecha_hasta} 00:00:00"
    return str_desde, str_hasta

def ejecutar_proceso():
    # Configuración de conexión
    host = "179.27.99.0"
    puerto = "3306"
    usuario = "pablo"
    password = "Laura.1729"
    bd = "districronos"
    
    url_conexion = f"mysql+pymysql://{usuario}:{password}@{host}:{puerto}/{bd}"
    
    # Determinar ruta del archivo (misma carpeta que el ejecutable)
    if getattr(sys, 'frozen', False):
        ruta_base = os.path.dirname(sys.executable)
    else:
        ruta_base = os.path.dirname(os.path.abspath(__file__))
    
    ruta_excel = os.path.join(ruta_base, "DGI.xlsx")
    
    f_desde, f_hasta = obtener_fechas()
    
    # Consultas SQL con f-strings para insertar las fechas dinámicas
    query_ventas = f"""
    SELECT ventas.fechaventa AS Dia, (IF(ventas.tipoventa=1,11111,1121001)) AS Debe, 
    (IF(articulos.iva=1,4102,IF(articulos.iva=2,4103,4101))) AS Haber, ventas.documento as Concepto, 
    0 as Moneda, (IF(articulos.iva=1,round(SUM(CASE WHEN articulos.iva=1 THEN stockventa.subtotal ELSE 0 END),2),
    IF(articulos.iva=2,round(SUM(CASE WHEN articulos.iva=2 THEN stockventa.subtotal ELSE 0 END),2),
    round(SUM(CASE WHEN articulos.iva=3 THEN stockventa.subtotal ELSE 0 END),2)))) AS ImporteIvaIncl, 
    (IF(articulos.iva=1,15,IF(articulos.iva=2,14,0))) AS CodigoIva, 
    (IF(articulos.iva=1,round(SUM(CASE WHEN articulos.iva=1 THEN (stockventa.subtotal*22/122) ELSE 0 END),2),
    IF(articulos.iva=2,round(SUM(CASE WHEN articulos.iva=2 THEN (stockventa.subtotal*10/110) ELSE 0 END),2),0))) AS IVA, 
    0 as Cotizacion, (IF(ventas.tipoventa=1,'I','V')) AS Rubro, clientes.rut as RUTComprador 
    FROM ventas, stockventa, articulos, clientes 
    WHERE ventas.valida=1 AND ventas.fechaventa BETWEEN '{f_desde}' AND '{f_hasta}' 
    AND ventas.tipoventa < 3 AND ventas.idcliente=clientes.idcliente AND ventas.idventa=stockventa.idventa 
    AND stockventa.idarticulo=articulos.idarticulo GROUP BY ventas.idventa, articulos.iva
    """

    query_devoluciones = f"""
    SELECT ventas.fechaventa AS Dia, (IF(ventas.tipoventa=1,11111,1121001)) AS Debe, 
    (IF(articulos.iva=1,4102,IF(articulos.iva=2,4103,4101))) AS Haber, ventas.documento as Concepto, 
    0 as Moneda, (IF(articulos.iva=1,round(SUM(CASE WHEN articulos.iva=1 THEN stockventa.subtotal ELSE 0 END),2),
    IF(articulos.iva=2,round(SUM(CASE WHEN articulos.iva=2 THEN stockventa.subtotal ELSE 0 END),2),
    round(SUM(CASE WHEN articulos.iva=3 THEN stockventa.subtotal ELSE 0 END),2)))) AS ImporteIvaIncl, 
    (IF(articulos.iva=1,15,IF(articulos.iva=2,14,0))) AS CodigoIva, 
    (IF(articulos.iva=1,round(SUM(CASE WHEN articulos.iva=1 THEN (stockventa.subtotal*22/122) ELSE 0 END),2),
    IF(articulos.iva=2,round(SUM(CASE WHEN articulos.iva=2 THEN (stockventa.subtotal*10/110) ELSE 0 END),2),0))) AS IVA, 
    0 as Cotizacion, (IF(ventas.tipoventa=1,'I','V')) AS Rubro, clientes.rut as RUTComprador 
    FROM ventas, stockventa, articulos, clientes 
    WHERE ventas.valida=1 AND ventas.fechaventa BETWEEN '{f_desde}' AND '{f_hasta}' 
    AND ventas.tipoventa > 2 AND ventas.idcliente=clientes.idcliente AND ventas.idventa=stockventa.idventa 
    AND stockventa.idarticulo=articulos.idarticulo GROUP BY ventas.idventa, articulos.iva
    """

    try:
        print(f"Rango: {f_desde} hasta {f_hasta}")
        print("Conectando a MySQL...")
        engine = create_engine(url_conexion)
        
        print("Extrayendo datos de VENTAS...")
        df_ventas = pd.read_sql(query_ventas, engine)
        
        print("Extrayendo datos de DEVOLUCIONES...")
        df_devoluciones = pd.read_sql(query_devoluciones, engine)
        
        print(f"Generando archivo en: {ruta_excel}")
        with pd.ExcelWriter(ruta_excel, engine='openpyxl') as writer:
            df_ventas.to_excel(writer, sheet_name='VENTAS', index=False)
            df_devoluciones.to_excel(writer, sheet_name='DEVOLUCIONES', index=False)
            
        print("¡Archivo DGI.xlsx generado con éxito!")
    except Exception as e:
        print(f"\nERROR: {e}")
    
    input("\nPresiona Enter para cerrar...")

if __name__ == "__main__":
    ejecutar_proceso()
