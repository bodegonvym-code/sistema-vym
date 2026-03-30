import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta
import time
import json
import hashlib
import base64
from io import BytesIO
import threading  # <<< NUEVO: para verificar conexión en segundo plano
import requests  # <<< NUEVO

# ============================================
# CONFIGURACIÓN INICIAL
# ============================================
st.set_page_config(
    page_title="BODEGÓN Y LICORERÍA MEDITERRANEO EXPRESS",
    page_icon="🥂",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# SISTEMA DE TEMA (OSCURO/CLARO)
# ============================================
if 'tema' not in st.session_state:
    st.session_state.tema = 'claro'

def aplicar_tema():
    if st.session_state.tema == 'oscuro':
        return """
            <style>
            .stApp {
                background-color: #1e1e1e;
                color: #ffffff;
            }
            .main-header {
                color: #ffffff !important;
            }
            .stMarkdown, .stText, p, span, label, h1, h2, h3, h4 {
                color: #ffffff !important;
            }
            .stDataFrame {
                background-color: #2d2d2d;
            }
            </style>
        """
    else:
        return """
            <style>
            .stApp {
                background-color: #f8f9fa;
            }
            .main-header {
                color: #1e3c72 !important;
            }
            </style>
        """

st.markdown(aplicar_tema(), unsafe_allow_html=True)

# ============================================
# ESTILOS PERSONALIZADOS BASE
# ============================================
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        border-left: 5px solid #28a745;
    }
    .warning-box {
        background-color: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 8px;
        border-left: 5px solid #ffc107;
    }
    .error-box {
        background-color: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        border-left: 5px solid #dc3545;
    }
    .product-card {
        background-color: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .badge-stock-bajo {
        background-color: #dc3545;
        color: white;
        padding: 0.2rem 0.6rem;
        border-radius: 12px;
        font-size: 0.7rem;
        font-weight: 600;
        margin-left: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================
# SISTEMA OFFLINE (ALMACENAMIENTO LOCAL) - MEJORADO
# ============================================
class OfflineManager:
    """Gestiona el almacenamiento local para modo offline"""
    
    @staticmethod
    def guardar_datos_local(tabla, datos):
        """Guarda datos en session_state como caché local"""
        if 'offline_data' not in st.session_state:
            st.session_state.offline_data = {}
        st.session_state.offline_data[tabla] = {
            'datos': datos,
            'timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def obtener_datos_local(tabla):
        """Obtiene datos de la caché local"""
        if 'offline_data' in st.session_state and tabla in st.session_state.offline_data:
            return st.session_state.offline_data[tabla]['datos']
        return None
    
    @staticmethod
    def sincronizar_pendientes(db):
        """Sincroniza operaciones pendientes con Supabase"""
        if 'operaciones_pendientes' not in st.session_state:
            return
        
        pendientes = st.session_state.operaciones_pendientes.copy()
        exitosas = []
        for op in pendientes:
            try:
                if op['tipo'] == 'insert':
                    db.table(op['tabla']).insert(op['datos']).execute()
                elif op['tipo'] == 'update':
                    db.table(op['tabla']).update(op['datos']).eq(op['id_field'], op['id_value']).execute()
                elif op['tipo'] == 'delete':
                    db.table(op['tabla']).delete().eq(op['id_field'], op['id_value']).execute()
                elif op['tipo'] == 'update_stock':
                    # Caso especial para actualización de stock en punto de venta
                    db.table("inventario").update({"stock": db.raw(f"stock - {op['cantidad']}")}).eq("id", op['id_producto']).execute()
                elif op['tipo'] == 'insert_venta':
                    db.table("ventas").insert(op['datos']).execute()
                elif op['tipo'] == 'anular_venta':
                    # Primero restaurar stock
                    for item in op['items']:
                        db.table("inventario").update({"stock": db.raw(f"stock + {item['cantidad']}")}).eq("id", item['id']).execute()
                    # Luego anular venta
                    db.table("ventas").update({"estado": "Anulado"}).eq("id", op['id_venta']).execute()
                exitosas.append(op)
            except Exception as e:
                print(f"Error sincronizando operación {op}: {e}")
                continue
        
        # Eliminar las operaciones exitosas de la lista de pendientes
        for op in exitosas:
            st.session_state.operaciones_pendientes.remove(op)
        
        return len(exitosas) > 0

# ============================================
# CONEXIÓN A SUPABASE Y DETECCIÓN DE CONEXIÓN (MEJORADO)
# ============================================
URL = "https://orrfldqwpjkkooeuqnmp.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9ycmZsZHF3cGpra29vZXVxbm1wIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkzMDg5MDEsImV4cCI6MjA4NDg4NDkwMX0.va4XR7_lDF2QV9SBXTusmAa_bgqV9oKwiIhC23hsC7E"
CLAVE_ADMIN = "1234"

# Función para verificar conexión a internet (nueva)
def verificar_conexion():
    try:
        requests.get("https://www.google.com", timeout=3)
        return True
    except:
        return False

# Variable de sesión para control de conexión
if 'online_mode' not in st.session_state:
    st.session_state.online_mode = verificar_conexion()

if 'ultima_verificacion' not in st.session_state:
    st.session_state.ultima_verificacion = datetime.now()

# Verificar conexión cada 30 segundos o al recargar
ahora = datetime.now()
if (ahora - st.session_state.ultima_verificacion).seconds > 30:
    estaba_online = st.session_state.online_mode
    st.session_state.online_mode = verificar_conexion()
    st.session_state.ultima_verificacion = ahora
    
    # Si se recuperó la conexión, sincronizar pendientes
    if not estaba_online and st.session_state.online_mode:
        try:
            db = create_client(URL, KEY)
            if OfflineManager.sincronizar_pendientes(db):
                st.success("✅ Sincronización completada. Los datos pendientes se han subido.")
                time.sleep(2)
                st.rerun()
        except Exception as e:
            st.error(f"Error al sincronizar: {e}")

# Inicializar cliente de Supabase solo si hay conexión
if st.session_state.online_mode:
    try:
        db = create_client(URL, KEY)
        # Probar conexión
        db.table("inventario").select("*").limit(1).execute()
        st.session_state.db_connected = True
    except Exception as e:
        st.session_state.db_connected = False
        st.session_state.online_mode = False
        st.warning("⚠️ Modo offline activado. Los cambios se guardarán localmente.")
else:
    st.session_state.db_connected = False

# ============================================
# SISTEMA DE USUARIOS (RÁPIDO)
# ============================================
USUARIOS = {
    'admin': {'nombre': 'Administrador', 'clave': '1234', 'rol': 'admin'},
    'empleada': {'nombre': 'Empleada', 'clave': '5678', 'rol': 'empleado'}
}

if 'usuario_actual' not in st.session_state:
    st.session_state.usuario_actual = None

def login(usuario, clave):
    if usuario in USUARIOS and USUARIOS[usuario]['clave'] == clave:
        st.session_state.usuario_actual = USUARIOS[usuario]
        return True
    return False

def logout():
    st.session_state.usuario_actual = None

# ============================================
# VERIFICAR TURNO ACTIVO (MEJORADO)
# ============================================
if st.session_state.online_mode:
    try:
        response = db.table("cierres").select("*").eq("estado", "abierto").order("fecha_apertura", desc=True).limit(1).execute()
        turno_activo = response.data[0] if response.data else None
        if turno_activo:
            st.session_state.id_turno = turno_activo['id']
            st.session_state.tasa_dia = turno_activo.get('tasa_apertura', 1.0)
            st.session_state.fondo_bs = turno_activo.get('fondo_bs', 0)
            st.session_state.fondo_usd = turno_activo.get('fondo_usd', 0)
        else:
            st.session_state.id_turno = None
    except Exception as e:
        st.session_state.id_turno = None
else:
    # Modo offline: usar datos locales
    if 'id_turno' not in st.session_state:
        st.session_state.id_turno = None
    if 'tasa_dia' not in st.session_state:
        st.session_state.tasa_dia = 60.0

# ============================================
# MENÚ LATERAL (REDISEÑADO)
# ============================================
with st.sidebar:
    # Logo y nombre
    st.markdown("""
        <div style="background: linear-gradient(135deg, #0a1929 0%, #1a2b3c 100%); 
                    padding: 2rem 1rem; 
                    border-radius: 0 0 20px 20px; 
                    text-align: center; 
                    margin-top: -1rem;
                    margin-bottom: 1rem;">
            <h1 style="color: white; margin: 0; font-size: 2.2rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                🥂 BODEGÓN Y LICORERÍA
            </h1>
            <h2 style="color: #ffd700; margin: 0; font-size: 1.8rem; letter-spacing: 2px;">
                MEDITERRANEO EXPRESS
            </h2>
            <p style="color: rgba(255,255,255,0.9); margin-top: 0.5rem; font-style: italic;">
                Desde 2020 sirviendo con calidad
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    # Selector de tema
    col_tema1, col_tema2 = st.columns(2)
    with col_tema1:
        if st.button("☀️ Claro", use_container_width=True):
            st.session_state.tema = 'claro'
            st.rerun()
    with col_tema2:
        if st.button("🌙 Oscuro", use_container_width=True):
            st.session_state.tema = 'oscuro'
            st.rerun()
    
    st.divider()
    
    # Login rápido
    if not st.session_state.usuario_actual:
        with st.expander("🔐 Acceso al sistema", expanded=True):
            col_user1, col_user2 = st.columns(2)
            with col_user1:
                usuario_sel = st.selectbox("Usuario", ["admin", "empleada"])
            with col_user2:
                clave_input = st.text_input("Clave", type="password")
            
            if st.button("✅ Ingresar", use_container_width=True):
                if login(usuario_sel, clave_input):
                    st.success(f"Bienvenido {st.session_state.usuario_actual['nombre']}")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Clave incorrecta")
    else:
        st.success(f"👤 Usuario: {st.session_state.usuario_actual['nombre']}")
        if st.button("🚪 Cerrar sesión", use_container_width=True):
            logout()
            st.rerun()
    
    st.divider()
    
    # Tasa del día (EDITABLE)
    with st.container(border=True):
        st.markdown("**💱 TASA BCV**")
        nueva_tasa = st.number_input(
            "Bs/USD",
            min_value=1.0,
            max_value=999.0,
            value=float(st.session_state.get('tasa_dia', 60.0)),
            step=0.5,
            format="%.2f",
            key="tasa_input"
        )
        if st.button("Actualizar tasa", use_container_width=True):
            st.session_state.tasa_dia = nueva_tasa
            if st.session_state.online_mode and st.session_state.id_turno:
                try:
                    db.table("cierres").update({"tasa_apertura": nueva_tasa}).eq("id", st.session_state.id_turno).execute()
                except:
                    pass
            st.success("Tasa actualizada")
            time.sleep(1)
            st.rerun()
    
    # Botón manual de sincronización (nuevo)
    if not st.session_state.online_mode:
        if st.button("🔄 Intentar sincronizar", use_container_width=True):
            if verificar_conexion():
                try:
                    db = create_client(URL, KEY)
                    if OfflineManager.sincronizar_pendientes(db):
                        st.session_state.online_mode = True
                        st.success("✅ Conexión restaurada y datos sincronizados.")
                        time.sleep(2)
                        st.rerun()
                except:
                    st.error("No se pudo conectar a Supabase.")
            else:
                st.error("No hay conexión a internet.")
    
    st.divider()
    
    # Módulos
    opcion = st.radio(
        "MÓDULOS",
        ["📦 INVENTARIO", "🛒 PUNTO DE VENTA", "💸 GASTOS", "📜 HISTORIAL", "📊 CIERRE DE CAJA"],
        label_visibility="collapsed"
    )
    
    st.divider()
    
    # Estado del sistema
    if st.session_state.online_mode:
        st.success("✅ Conectado a Internet")
    else:
        st.warning("⚠️ Modo offline")
    
    if st.session_state.id_turno:
        st.info(f"📍 Turno activo: #{st.session_state.id_turno}")
    else:
        st.error("🔴 Caja cerrada")

# ============================================
# FUNCIONES AUXILIARES MEJORADAS
# ============================================
def requiere_turno():
    """Verifica si hay turno activo"""
    if not st.session_state.id_turno:
        st.warning("⚠️ No hay un turno activo. Debe abrir caja en el módulo 'Cierre de Caja'.")
        st.stop()

def requiere_usuario():
    """Verifica si hay usuario logueado"""
    if not st.session_state.usuario_actual:
        st.warning("⚠️ Debe iniciar sesión para acceder a este módulo.")
        st.stop()

def formatear_usd(valor):
    return f"${valor:,.2f}"

def formatear_bs(valor):
    return f"{valor:,.2f} Bs"

def exportar_excel(df, nombre_archivo):
    """Convierte DataFrame a Excel para descargar"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{nombre_archivo}.xlsx">📥 Descargar Excel</a>'
    return href

# ============================================
# MÓDULO 1: INVENTARIO MEJORADO (sin cambios)
# ============================================
if opcion == "📦 INVENTARIO":
    st.markdown("<h1 class='main-header'>📦 Gestión de Inventario</h1>", unsafe_allow_html=True)
    
    # Categorías predefinidas
    CATEGORIAS = [
        "Licores", "Cervezas", "Vinos", "Refrescos", "Aguas",
        "Víveres", "Confitería", "Snacks", "Lácteos", "Otros"
    ]
    
    try:
        # Cargar datos (con soporte offline)
        if st.session_state.online_mode:
            response = db.table("inventario").select("*").order("nombre").execute()
            df = pd.DataFrame(response.data) if response.data else pd.DataFrame()
            OfflineManager.guardar_datos_local('inventario', df.to_dict('records'))
        else:
            datos_local = OfflineManager.obtener_datos_local('inventario')
            df = pd.DataFrame(datos_local) if datos_local else pd.DataFrame()
        
        # Verificar si existe columna categoria, si no, agregarla
        if not df.empty:
            if 'categoria' not in df.columns:
                df['categoria'] = 'Otros'
            if 'codigo_barras' not in df.columns:
                df['codigo_barras'] = ''
        
        # Pestañas principales
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Ver Inventario", "➕ Agregar Producto", "📊 Estadísticas", "📥 Respaldos"])
        
        # ============================================
        # TAB 1: VER INVENTARIO (CORREGIDO)
        # ============================================
        with tab1:
            # Filtros avanzados
            col_f1, col_f2, col_f3, col_f4 = st.columns([2, 1, 1, 1])
            
            with col_f1:
                busqueda = st.text_input("🔍 Buscar producto", placeholder="Nombre o código...")
            
            with col_f2:
                categoria_filtro = st.selectbox("Categoría", ["Todas"] + CATEGORIAS)
            
            with col_f3:
                ver_bajo_stock = st.checkbox("⚠️ Solo stock bajo")
            
            with col_f4:
                if st.button("📤 Exportar a Excel", use_container_width=True):
                    if not df.empty:
                        export_df = df[['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'min_mayor']].copy()
                        export_df.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Precio Detal $', 'Precio Mayor $', 'Min. Mayor']
                        href = exportar_excel(export_df, f"inventario_{datetime.now().strftime('%Y%m%d')}")
                        st.markdown(href, unsafe_allow_html=True)
            
            if not df.empty:
                # Aplicar filtros
                df_filtrado = df.copy()
                
                # FILTRO CORREGIDO
                if busqueda:
                    mask_nombre = df_filtrado['nombre'].str.contains(busqueda, case=False, na=False)
                    if 'codigo_barras' in df_filtrado.columns:
                        codigos_str = df_filtrado['codigo_barras'].fillna('').astype(str)
                        mask_codigo = codigos_str.str.contains(busqueda, case=False, na=False)
                        df_filtrado = df_filtrado[mask_nombre | mask_codigo]
                    else:
                        df_filtrado = df_filtrado[mask_nombre]
                
                if categoria_filtro != "Todas" and 'categoria' in df_filtrado.columns:
                    df_filtrado = df_filtrado[df_filtrado['categoria'] == categoria_filtro]
                
                if ver_bajo_stock:
                    df_filtrado = df_filtrado[df_filtrado['stock'] < 5]
                    if len(df_filtrado) > 0:
                        st.warning(f"⚠️ Hay {len(df_filtrado)} productos con stock bajo")
                    else:
                        st.success("✅ No hay productos con stock bajo")
                
                # Mostrar tabla con colores según stock
                def colorear_stock(val):
                    if val < 5:
                        return 'color: red; font-weight: bold; background-color: #ffe6e6'
                    elif val < 10:
                        return 'color: orange; font-weight: bold;'
                    return 'color: green; font-weight: bold;'
                
                columnas_mostrar = ['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'min_mayor']
                columnas_mostrar = [col for col in columnas_mostrar if col in df_filtrado.columns]
                
                df_mostrar = df_filtrado[columnas_mostrar].copy()
                df_mostrar.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Detal $', 'Mayor $', 'Mín. Mayor']
                
                styled_df = df_mostrar.style.map(colorear_stock, subset=['Stock'])
                
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                st.caption(f"Mostrando {len(df_filtrado)} de {len(df)} productos")
                
                # EDITAR PRODUCTO
                st.divider()
                st.subheader("✏️ Editar producto")
                
                if not df_filtrado.empty:
                    producto_editar = st.selectbox("Seleccionar producto", df_filtrado['nombre'].tolist(), key="editar")
                    if producto_editar:
                        prod = df[df['nombre'] == producto_editar].iloc[0]
                        with st.form("form_editar"):
                            col_e1, col_e2 = st.columns(2)
                            with col_e1:
                                nuevo_nombre = st.text_input("Nombre", value=prod['nombre'])
                                nueva_categoria = st.selectbox("Categoría", CATEGORIAS, 
                                                              index=CATEGORIAS.index(prod.get('categoria', 'Otros')) if prod.get('categoria', 'Otros') in CATEGORIAS else 9)
                                nuevo_stock = st.number_input("Stock", value=float(prod['stock']), min_value=0.0, step=1.0)
                                nuevo_costo = st.number_input("Costo $", value=float(prod['costo']), min_value=0.0, step=0.01)
                                nuevo_codigo = st.text_input("Código de barras", value=prod.get('codigo_barras', ''))
                            with col_e2:
                                nuevo_detal = st.number_input("Precio Detal $", value=float(prod['precio_detal']), min_value=0.0, step=0.01)
                                nuevo_mayor = st.number_input("Precio Mayor $", value=float(prod['precio_mayor']), min_value=0.0, step=0.01)
                                nuevo_min = st.number_input("Mín. Mayor", value=int(prod['min_mayor']), min_value=1, step=1)
                            
                            if st.form_submit_button("💾 Guardar Cambios", use_container_width=True):
                                try:
                                    datos_actualizados = {
                                        "nombre": nuevo_nombre,
                                        "categoria": nueva_categoria,
                                        "stock": nuevo_stock,
                                        "costo": nuevo_costo,
                                        "precio_detal": nuevo_detal,
                                        "precio_mayor": nuevo_mayor,
                                        "min_mayor": nuevo_min
                                    }
                                    if nuevo_codigo:
                                        datos_actualizados["codigo_barras"] = nuevo_codigo
                                    
                                    if st.session_state.online_mode:
                                        db.table("inventario").update(datos_actualizados).eq("id", prod['id']).execute()
                                    else:
                                        if 'operaciones_pendientes' not in st.session_state:
                                            st.session_state.operaciones_pendientes = []
                                        st.session_state.operaciones_pendientes.append({
                                            'tipo': 'update',
                                            'tabla': 'inventario',
                                            'datos': datos_actualizados,
                                            'id_field': 'id',
                                            'id_value': prod['id']
                                        })
                                    
                                    st.success("✅ Producto actualizado")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                
                # ELIMINAR PRODUCTO
                st.divider()
                st.subheader("🗑️ Eliminar producto")
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    producto_eliminar = st.selectbox("Seleccionar producto", [""] + df['nombre'].tolist(), key="eliminar")
                with col_d2:
                    clave = st.text_input("Clave Admin", type="password", key="clave_eliminar")
                
                if producto_eliminar and st.button("❌ Eliminar", type="primary", use_container_width=True):
                    if clave == CLAVE_ADMIN:
                        try:
                            if st.session_state.online_mode:
                                db.table("inventario").delete().eq("nombre", producto_eliminar).execute()
                            else:
                                if 'operaciones_pendientes' not in st.session_state:
                                    st.session_state.operaciones_pendientes = []
                                st.session_state.operaciones_pendientes.append({
                                    'tipo': 'delete',
                                    'tabla': 'inventario',
                                    'id_field': 'nombre',
                                    'id_value': producto_eliminar
                                })
                            
                            st.success(f"Producto '{producto_eliminar}' eliminado")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        st.error("Clave incorrecta")
            else:
                st.info("No hay productos en el inventario")
        
        # ============================================
        # TAB 2: AGREGAR PRODUCTO (MEJORADO)
        # ============================================
        with tab2:
            with st.form("nuevo_producto", clear_on_submit=True):
                st.markdown("### 📝 Datos del nuevo producto")
                
                col_a1, col_a2 = st.columns(2)
                with col_a1:
                    nombre = st.text_input("Nombre del producto *").upper()
                    categoria = st.selectbox("Categoría", CATEGORIAS)
                    stock = st.number_input("Stock inicial *", min_value=0.0, step=1.0, format="%.2f")
                    costo = st.number_input("Costo $ *", min_value=0.0, step=0.01, format="%.2f")
                    codigo_barras = st.text_input("Código de barras (opcional)")
                
                with col_a2:
                    precio_detal = st.number_input("Precio Detal $ *", min_value=0.0, step=0.01, format="%.2f")
                    precio_mayor = st.number_input("Precio Mayor $ *", min_value=0.0, step=0.01, format="%.2f")
                    min_mayor = st.number_input("Mínimo para Mayor *", min_value=1, value=6, step=1)
                
                st.markdown("---")
                
                if st.form_submit_button("📦 Registrar Producto", use_container_width=True):
                    if not nombre:
                        st.error("El nombre es obligatorio")
                    elif stock < 0 or costo < 0 or precio_detal <= 0:
                        st.error("Verifique los valores ingresados")
                    else:
                        try:
                            if st.session_state.online_mode:
                                existe = db.table("inventario").select("*").eq("nombre", nombre).execute()
                                if existe.data:
                                    st.error(f"Ya existe un producto con el nombre '{nombre}'")
                                    st.stop()
                            
                            datos_nuevos = {
                                "nombre": nombre,
                                "categoria": categoria,
                                "stock": stock,
                                "costo": costo,
                                "precio_detal": precio_detal,
                                "precio_mayor": precio_mayor,
                                "min_mayor": min_mayor
                            }
                            if codigo_barras:
                                datos_nuevos["codigo_barras"] = codigo_barras
                            
                            if st.session_state.online_mode:
                                db.table("inventario").insert(datos_nuevos).execute()
                            else:
                                if 'operaciones_pendientes' not in st.session_state:
                                    st.session_state.operaciones_pendientes = []
                                st.session_state.operaciones_pendientes.append({
                                    'tipo': 'insert',
                                    'tabla': 'inventario',
                                    'datos': datos_nuevos
                                })
                            
                            st.success(f"✅ Producto '{nombre}' registrado exitosamente")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al registrar: {e}")
        
        # ============================================
        # TAB 3: ESTADÍSTICAS (MEJORADAS)
        # ============================================
        with tab3:
            if not df.empty:
                valor_inv = (df['stock'] * df['costo']).sum()
                valor_venta = (df['stock'] * df['precio_detal']).sum()
                bajo_stock = len(df[df['stock'] < 5])
                total_productos = len(df)
                
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                col_m1.metric("Total productos", total_productos)
                col_m2.metric("Valor inventario (costo)", formatear_usd(valor_inv))
                col_m3.metric("Valor venta potencial", formatear_usd(valor_venta))
                col_m4.metric("Stock bajo", bajo_stock, delta_color="inverse")
                
                ganancia_potencial = valor_venta - valor_inv
                st.metric("💰 Ganancia potencial total", formatear_usd(ganancia_potencial),
                         delta=f"{(ganancia_potencial/valor_inv*100):.1f}%" if valor_inv else "")
                
                st.subheader("📊 Productos por categoría")
                if 'categoria' in df.columns:
                    cat_stats = df.groupby('categoria').agg({
                        'nombre': 'count',
                        'stock': 'sum',
                        'costo': lambda x: (x * df.loc[x.index, 'stock']).sum()
                    }).round(2)
                    cat_stats.columns = ['Cantidad', 'Stock total', 'Valor total $']
                    st.dataframe(cat_stats, use_container_width=True)
                
                st.subheader("💰 Top 10 productos por valor en inventario")
                df_temp = df.copy()
                df_temp['valor_total'] = df_temp['stock'] * df_temp['costo']
                df_top = df_temp.nlargest(10, 'valor_total')[['nombre', 'categoria', 'stock', 'costo', 'valor_total']]
                df_top.columns = ['Producto', 'Categoría', 'Stock', 'Costo unitario', 'Valor total']
                st.dataframe(df_top, use_container_width=True, hide_index=True)
                
                st.subheader("⚠️ Productos con stock bajo (<5)")
                df_bajo = df[df['stock'] < 5][['nombre', 'categoria', 'stock', 'costo']]
                if not df_bajo.empty:
                    df_bajo.columns = ['Producto', 'Categoría', 'Stock', 'Costo unitario']
                    st.dataframe(df_bajo, use_container_width=True, hide_index=True)
                else:
                    st.success("No hay productos con stock bajo")
            else:
                st.info("No hay datos para mostrar estadísticas")
        
        # ============================================
        # TAB 4: RESPALDOS
        # ============================================
        with tab4:
            st.subheader("📥 Respaldo de inventario")
            st.markdown("""
                <div style='background-color: #e7f3ff; padding: 1rem; border-radius: 8px; margin-bottom: 1rem;'>
                    <p>Desde aquí puedes exportar todo tu inventario para tener un respaldo físico.</p>
                    <p>Recomendación: Haz un respaldo diario antes de cerrar.</p>
                </div>
            """, unsafe_allow_html=True)
            
            if not df.empty:
                col_r1, col_r2 = st.columns(2)
                
                with col_r1:
                    st.markdown("**📊 Respaldo completo**")
                    if st.button("📥 Exportar inventario completo", use_container_width=True):
                        export_df = df[['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'min_mayor']].copy()
                        export_df.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Precio Detal $', 'Precio Mayor $', 'Min. Mayor']
                        export_df = export_df.sort_values('Producto')
                        href = exportar_excel(export_df, f"inventario_completo_{datetime.now().strftime('%Y%m%d_%H%M')}")
                        st.markdown(href, unsafe_allow_html=True)
                
                with col_r2:
                    st.markdown("**📋 Lista de precios**")
                    if st.button("📥 Exportar lista de precios", use_container_width=True):
                        precio_df = df[['nombre', 'categoria', 'precio_detal', 'precio_mayor', 'min_mayor']].copy()
                        precio_df.columns = ['Producto', 'Categoría', 'Precio Detal $', 'Precio Mayor $', 'Mín. Mayor']
                        precio_df = precio_df.sort_values('Categoría')
                        href = exportar_excel(precio_df, f"lista_precios_{datetime.now().strftime('%Y%m%d')}")
                        st.markdown(href, unsafe_allow_html=True)
                
                st.divider()
                st.markdown(f"""
                    **📌 Última actualización:** {datetime.now().strftime('%d/%m/%Y %H:%M')}  
                    **📦 Total de productos:** {len(df)}  
                    **🏷️ Categorías:** {df['categoria'].nunique() if 'categoria' in df.columns else 0}
                """)
            else:
                st.info("No hay productos para respaldar")
                
    except Exception as e:
        st.error(f"Error en inventario: {e}")
        st.exception(e)

# ============================================
# MÓDULO 2: PUNTO DE VENTA CON SEPARACIÓN DE CUENTAS (MEJORADO Y CORREGIDO)
# ============================================
elif opcion == "🛒 PUNTO DE VENTA":
    requiere_turno()
    requiere_usuario()
    
    id_turno = st.session_state.id_turno
    tasa = st.session_state.tasa_dia
    
    st.markdown("<h1 class='main-header'>🛒 Punto de Venta</h1>", unsafe_allow_html=True)
    st.markdown(f"""
        <div style='background-color: #e7f3ff; padding: 0.8rem; border-radius: 8px; margin-bottom: 1rem;'>
            <span style='font-weight:600;'>📍 Turno #{id_turno}</span> | 
            <span>💱 Tasa: {tasa:.2f} Bs/$</span> |
            <span>👤 Cajero: {st.session_state.usuario_actual['nombre']}</span>
        </div>
    """, unsafe_allow_html=True)
    
    # SISTEMA DE MESAS / CUENTAS
    if 'mesas' not in st.session_state:
        st.session_state.mesas = {
            'mesa_1': {'nombre': 'Mesa 1', 'carrito': [], 'activa': True, 'cliente': ''},
            'mesa_2': {'nombre': 'Mesa 2', 'carrito': [], 'activa': True, 'cliente': ''},
            'mesa_3': {'nombre': 'Mesa 3', 'carrito': [], 'activa': True, 'cliente': ''},
            'mesa_4': {'nombre': 'Mesa 4', 'carrito': [], 'activa': True, 'cliente': ''},
            'barra': {'nombre': 'Barra', 'carrito': [], 'activa': True, 'cliente': 'Consumo en barra'},
            'llevar': {'nombre': 'Para llevar', 'carrito': [], 'activa': True, 'cliente': ''}
        }
    
    if 'mesa_actual' not in st.session_state:
        st.session_state.mesa_actual = 'mesa_1'
    
    # SELECTOR DE MESAS
    st.subheader("🍽️ Seleccionar Mesa / Cuenta")
    
    col_mesas = st.columns(6)
    idx_mesa = 0
    for mesa_id, mesa_data in st.session_state.mesas.items():
        with col_mesas[idx_mesa]:
            if mesa_id == st.session_state.mesa_actual:
                bg_color = "#28a745"
            elif len(mesa_data['carrito']) > 0:
                bg_color = "#ffc107"
            else:
                bg_color = "#6c757d"
            
            if st.button(
                f"{mesa_data['nombre']}\n({len(mesa_data['carrito'])} items)",
                key=f"mesa_{mesa_id}",
                use_container_width=True,
                type="primary" if mesa_id == st.session_state.mesa_actual else "secondary"
            ):
                st.session_state.mesa_actual = mesa_id
                st.rerun()
        idx_mesa += 1
    
    mesa_actual = st.session_state.mesas[st.session_state.mesa_actual]
    st.divider()
    
    # CABECERA DE LA MESA ACTUAL
    col_mesa_info1, col_mesa_info2, col_mesa_info3 = st.columns([2, 2, 1])
    with col_mesa_info1:
        st.markdown(f"### 🍽️ {mesa_actual['nombre']}")
    with col_mesa_info2:
        if mesa_actual['nombre'] not in ['Barra', 'Para llevar']:
            cliente = st.text_input(
                "Nombre del cliente (opcional)",
                value=mesa_actual.get('cliente', ''),
                key="cliente_mesa",
                placeholder="Ej: Juan Pérez"
            )
            if cliente != mesa_actual.get('cliente', ''):
                st.session_state.mesas[st.session_state.mesa_actual]['cliente'] = cliente
    with col_mesa_info3:
        if len(mesa_actual['carrito']) > 0:
            if st.button("🧹 Limpiar mesa", use_container_width=True):
                st.session_state.mesas[st.session_state.mesa_actual]['carrito'] = []
                st.rerun()
    
    col_busqueda, col_carrito = st.columns([1.2, 1.8])
    
    # COLUMNA IZQUIERDA: BÚSQUEDA DE PRODUCTOS
    with col_busqueda:
        st.subheader("🔍 Buscar productos")
        es_tasca = st.checkbox("🍷 Venta en tasca (+10%)", help="Los precios aumentan un 10% para consumo en el local")
        busqueda = st.text_input("", placeholder="Escribe nombre del producto...", key="buscar_venta")
        
        if busqueda:
            try:
                if st.session_state.online_mode:
                    response = db.table("inventario")\
                        .select("*")\
                        .ilike("nombre", f"%{busqueda}%")\
                        .gt("stock", 0)\
                        .order("nombre")\
                        .limit(20)\
                        .execute()
                    productos = response.data
                else:
                    datos_local = OfflineManager.obtener_datos_local('inventario')
                    if datos_local:
                        df_local = pd.DataFrame(datos_local)
                        df_local = df_local[df_local['stock'] > 0]
                        df_local = df_local[df_local['nombre'].str.contains(busqueda, case=False, na=False)]
                        productos = df_local.to_dict('records')
                    else:
                        productos = []
                
                if productos:
                    for i in range(0, len(productos), 2):
                        cols = st.columns(2)
                        for j in range(2):
                            if i + j < len(productos):
                                prod = productos[i + j]
                                precio_base = float(prod['precio_detal'])
                                precio_unitario = precio_base * 1.10 if es_tasca else precio_base
                                
                                with cols[j]:
                                    with st.container(border=True):
                                        st.markdown(f"**{prod['nombre']}**", help=prod['nombre'])
                                        st.caption(f"Stock: {prod['stock']:.0f}")
                                        st.markdown(f"<h3 style='color:#2a9d8f;'>${precio_unitario:.2f}</h3>", unsafe_allow_html=True)
                                        
                                        if st.button("➕ Agregar", key=f"add_{prod['id']}", use_container_width=True):
                                            carrito_actual = mesa_actual['carrito']
                                            cantidad_existente = 0
                                            for item in carrito_actual:
                                                if item['id'] == prod['id']:
                                                    cantidad_existente += item['cantidad']
                                            
                                            nueva_cantidad = cantidad_existente + 1
                                            
                                            # Determinar precio final
                                            if nueva_cantidad >= prod['min_mayor'] and not es_tasca:
                                                precio_final = float(prod['precio_mayor'])
                                                tipo_precio = " (Mayor)"
                                            else:
                                                precio_final = precio_base
                                                tipo_precio = ""
                                            
                                            if es_tasca:
                                                precio_final = precio_base * 1.10
                                                tipo_precio = " (Tasca)"
                                            
                                            encontrado = False
                                            for item in st.session_state.mesas[st.session_state.mesa_actual]['carrito']:
                                                if item['id'] == prod['id']:
                                                    item['cantidad'] += 1
                                                    item['precio'] = precio_final
                                                    item['subtotal'] = item['cantidad'] * item['precio']
                                                    encontrado = True
                                                    break
                                            
                                            if not encontrado:
                                                st.session_state.mesas[st.session_state.mesa_actual]['carrito'].append({
                                                    "id": prod['id'],
                                                    "nombre": prod['nombre'],
                                                    "cantidad": 1,
                                                    "precio": precio_final,
                                                    "costo": float(prod['costo']),
                                                    "subtotal": precio_final,
                                                    "tipo_precio": tipo_precio
                                                })
                                            
                                            st.rerun()
                else:
                    st.info("No se encontraron productos")
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.info("Escribe algo para buscar productos")
    
    # COLUMNA DERECHA: CARRITO DE LA MESA ACTUAL (CORREGIDO para precio mayor)
    with col_carrito:
        st.subheader(f"🛒 Carrito - {mesa_actual['nombre']}")
        carrito = mesa_actual['carrito']
        
        if not carrito:
            st.info("Carrito vacío")
        else:
            total_venta_usd = 0
            total_costo = 0
            
            for idx, item in enumerate(carrito):
                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns([2.5, 1, 1, 0.5])
                    
                    with col1:
                        st.markdown(f"**{item['nombre']}**")
                        if 'tipo_precio' in item:
                            st.caption(f"Precio: ${item['precio']:.2f}{item['tipo_precio']}")
                    
                    with col2:
                        nueva_cant = st.number_input(
                            "Cant.",
                            min_value=0.0,
                            max_value=1000.0,
                            value=float(item['cantidad']),
                            step=1.0,
                            key=f"cant_mesa_{idx}",
                            label_visibility="collapsed"
                        )
                        
                        if nueva_cant != item['cantidad']:
                            if nueva_cant == 0:
                                st.session_state.mesas[st.session_state.mesa_actual]['carrito'].pop(idx)
                                st.rerun()
                            else:
                                # <<< NUEVO: Recalcular precio mayor al cambiar cantidad
                                # Obtener datos del producto desde el inventario en caché o Supabase
                                prod_data = None
                                if st.session_state.online_mode:
                                    try:
                                        prod_resp = db.table("inventario").select("precio_detal, precio_mayor, min_mayor").eq("id", item['id']).execute()
                                        if prod_resp.data:
                                            prod_data = prod_resp.data[0]
                                    except:
                                        pass
                                if not prod_data:
                                    # Fallback: usar datos locales (inventario en caché)
                                    inventario_local = OfflineManager.obtener_datos_local('inventario')
                                    if inventario_local:
                                        for p in inventario_local:
                                            if p['id'] == item['id']:
                                                prod_data = p
                                                break
                                
                                if prod_data:
                                    # Determinar nuevo precio según cantidad y umbral
                                    if nueva_cant >= prod_data['min_mayor'] and not es_tasca:
                                        nuevo_precio = float(prod_data['precio_mayor'])
                                        tipo_precio = " (Mayor)"
                                    else:
                                        nuevo_precio = float(prod_data['precio_detal'])
                                        tipo_precio = ""
                                    
                                    if es_tasca:
                                        nuevo_precio = nuevo_precio * 1.10
                                        tipo_precio = " (Tasca)"
                                    
                                    item['precio'] = nuevo_precio
                                    item['tipo_precio'] = tipo_precio
                                
                                item['cantidad'] = nueva_cant
                                item['subtotal'] = item['cantidad'] * item['precio']
                                st.rerun()
                    
                    with col3:
                        st.markdown(f"**${item['subtotal']:.2f}**")
                    
                    with col4:
                        if st.button("❌", key=f"del_mesa_{idx}"):
                            st.session_state.mesas[st.session_state.mesa_actual]['carrito'].pop(idx)
                            st.rerun()
                    
                    total_venta_usd += item['subtotal']
                    total_costo += item['cantidad'] * item['costo']
            
            total_venta_bs = total_venta_usd * tasa
            st.divider()
            
            col_t1, col_t2 = st.columns(2)
            with col_t1:
                st.markdown(f"### Total calculado USD: ${total_venta_usd:,.2f}")
            with col_t2:
                st.markdown(f"### Total calculado Bs: {total_venta_bs:,.2f}")
            
            # AJUSTE MANUAL DEL MONTO (REDONDEO)
            total_final_usd = total_venta_usd
            total_final_bs = total_venta_bs
            
            with st.expander("🔧 Ajustar monto final (redondeo)", expanded=False):
                st.markdown("Si deseas redondear el total a cobrar, selecciona una opción e ingresa el monto:")
                opcion_ajuste = st.radio(
                    "Ajustar en:",
                    ["No ajustar (usar calculado)", "Bolívares (Bs)", "Dólares (USD)"],
                    horizontal=True,
                    key="opcion_ajuste"
                )
                
                if opcion_ajuste == "Bolívares (Bs)":
                    monto_ajustado_bs = st.number_input(
                        "Monto final en Bs",
                        min_value=0.0,
                        value=float(total_venta_bs),
                        step=10.0,
                        format="%.2f",
                        key="monto_ajustado_bs"
                    )
                    total_final_bs = monto_ajustado_bs
                    total_final_usd = monto_ajustado_bs / tasa if tasa > 0 else 0
                    st.info(f"Equivalente en USD: ${total_final_usd:,.2f}")
                elif opcion_ajuste == "Dólares (USD)":
                    monto_ajustado_usd = st.number_input(
                        "Monto final en USD",
                        min_value=0.0,
                        value=float(total_venta_usd),
                        step=1.0,
                        format="%.2f",
                        key="monto_ajustado_usd"
                    )
                    total_final_usd = monto_ajustado_usd
                    total_final_bs = monto_ajustado_usd * tasa
                    st.info(f"Equivalente en Bs: {total_final_bs:,.2f} Bs")
            
            st.divider()
            
            # SECCIÓN DE PAGOS
            with st.expander("💳 Detalle de pagos", expanded=True):
                st.markdown("**Ingresa los montos recibidos:**")
                col_p1, col_p2 = st.columns(2)
                with col_p1:
                    st.markdown("**💵 Pagos en USD**")
                    pago_usd_efectivo = st.number_input("Efectivo USD", min_value=0.0, step=5.0, format="%.2f", key="p_usd_efectivo")
                    pago_zelle = st.number_input("Zelle USD", min_value=0.0, step=5.0, format="%.2f", key="p_zelle")
                    pago_otros_usd = st.number_input("Otros USD (Binance/Transfer)", min_value=0.0, step=5.0, format="%.2f", key="p_otros_usd")
                with col_p2:
                    st.markdown("**💵 Pagos en Bs**")
                    pago_bs_efectivo = st.number_input("Efectivo Bs", min_value=0.0, step=100.0, format="%.2f", key="p_bs_efectivo")
                    pago_movil = st.number_input("Pago Móvil Bs", min_value=0.0, step=100.0, format="%.2f", key="p_movil")
                    pago_punto = st.number_input("Punto de Venta Bs", min_value=0.0, step=100.0, format="%.2f", key="p_punto")
                
                total_usd_recibido = pago_usd_efectivo + pago_zelle + pago_otros_usd
                total_bs_recibido = pago_bs_efectivo + pago_movil + pago_punto
                total_usd_equivalente = total_usd_recibido + (total_bs_recibido / tasa if tasa > 0 else 0)
                esperado_usd = total_final_bs / tasa if tasa > 0 else 0
                vuelto_usd = total_usd_equivalente - esperado_usd
                
                st.divider()
                col_r1, col_r2, col_r3 = st.columns(3)
                with col_r1:
                    st.metric("Pagado USD eq.", f"${total_usd_equivalente:,.2f}")
                with col_r2:
                    st.metric("Esperado USD", f"${esperado_usd:,.2f}")
                with col_r3:
                    if vuelto_usd >= 0:
                        st.metric("Vuelto USD", f"${vuelto_usd:,.2f}")
                    else:
                        st.metric("Faltante USD", f"${abs(vuelto_usd):,.2f}", delta_color="inverse")
                
                if vuelto_usd >= -0.01:
                    st.success(f"✅ Pago suficiente. Vuelto: ${vuelto_usd:.2f} / {(vuelto_usd * tasa):,.2f} Bs")
                else:
                    st.error(f"❌ Faltante: ${abs(vuelto_usd):,.2f} / {(abs(vuelto_usd) * tasa):,.2f} Bs")
            
            # BOTONES DE ACCIÓN
            col_btn1, col_btn2, col_btn3 = st.columns(3)
            
            with col_btn1:
                if st.button("🔄 Limpiar carrito", use_container_width=True):
                    st.session_state.mesas[st.session_state.mesa_actual]['carrito'] = []
                    st.rerun()
            
            with col_btn2:
                venta_valida = vuelto_usd >= -0.01 and len(carrito) > 0
                
                if st.button("✅ Cobrar y cerrar cuenta", type="primary", use_container_width=True, disabled=not venta_valida):
                    try:
                        items_resumen = []
                        for item in carrito:
                            items_resumen.append(f"{item['cantidad']:.0f}x {item['nombre']}")
                            
                            if st.session_state.online_mode:
                                try:
                                    stock_actual = db.table("inventario").select("stock").eq("id", item['id']).execute().data[0]['stock']
                                    db.table("inventario").update({
                                        "stock": stock_actual - item['cantidad']
                                    }).eq("id", item['id']).execute()
                                except:
                                    if 'operaciones_pendientes' not in st.session_state:
                                        st.session_state.operaciones_pendientes = []
                                    st.session_state.operaciones_pendientes.append({
                                        'tipo': 'update_stock',
                                        'id_producto': item['id'],
                                        'cantidad': item['cantidad']
                                    })
                            else:
                                if 'operaciones_pendientes' not in st.session_state:
                                    st.session_state.operaciones_pendientes = []
                                st.session_state.operaciones_pendientes.append({
                                    'tipo': 'update_stock',
                                    'id_producto': item['id'],
                                    'cantidad': item['cantidad']
                                })
                        
                        info_cliente = mesa_actual.get('cliente', '')
                        if info_cliente:
                            info_cliente = f" - Cliente: {info_cliente}"
                        
                        venta_data = {
                            "id_cierre": id_turno,
                            "producto": ", ".join(items_resumen),
                            "cantidad": len(carrito),
                            "total_usd": round(total_final_usd, 2),
                            "monto_cobrado_bs": round(total_final_bs, 2),
                            "tasa_cambio": tasa,
                            "pago_divisas": round(pago_usd_efectivo, 2),
                            "pago_zelle": round(pago_zelle, 2),
                            "pago_otros": round(pago_otros_usd, 2),
                            "pago_efectivo": round(pago_bs_efectivo, 2),
                            "pago_movil": round(pago_movil, 2),
                            "pago_punto": round(pago_punto, 2),
                            "costo_venta": round(total_costo, 2),
                            "estado": "Finalizado",
                            "items": json.dumps(carrito),
                            "id_transaccion": str(int(datetime.now().timestamp())),
                            "fecha": datetime.now().isoformat(),
                            "cliente": mesa_actual.get('cliente', '') or f"Mesa: {mesa_actual['nombre']}"
                        }
                        
                        if st.session_state.online_mode:
                            db.table("ventas").insert(venta_data).execute()
                        else:
                            if 'operaciones_pendientes' not in st.session_state:
                                st.session_state.operaciones_pendientes = []
                            st.session_state.operaciones_pendientes.append({
                                'tipo': 'insert_venta',
                                'datos': venta_data
                            })
                        
                        st.balloons()
                        st.success(f"✅ Venta registrada - {mesa_actual['nombre']}{info_cliente}")
                        
                        with st.expander("🧾 Ver Ticket", expanded=True):
                            items_ticket = ""
                            for item in carrito:
                                items_ticket += f"""
                                    <tr>
                                        <td style='padding: 4px 8px;'>{item['cantidad']:.0f}</td>
                                        <td style='padding: 4px 8px;'>{item['nombre']}</td>
                                        <td style='padding: 4px 8px; text-align: right;'>${item['precio']:.2f}</td>
                                        <td style='padding: 4px 8px; text-align: right;'>${item['subtotal']:.2f}</td>
                                    </tr>
                                """
                            
                            st.markdown(f"""
                            <div style="background:white; padding:20px; border-radius:10px; border:2px solid #1e3c72; max-width:800px; margin:0 auto;">
                                <h3 style="text-align:center;">BODEGÓN Y LICORERÍA MEDITERRANEO</h3>
                                <p style="text-align:center;">{datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
                                <p style="text-align:center;">Turno #{id_turno} | {mesa_actual['nombre']}{info_cliente}</p>
                                <p style="text-align:center;">Cajero: {st.session_state.usuario_actual['nombre']}</p>
                                <hr>
                                <table style="width:100%; border-collapse: collapse;">
                                    <thead>
                                        <tr style="border-bottom:1px solid #ccc;">
                                            <th style="text-align:left;">Cant</th>
                                            <th style="text-align:left;">Producto</th>
                                            <th style="text-align:right;">Precio</th>
                                            <th style="text-align:right;">Subtotal</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {items_ticket}
                                    </tbody>
                                </table>
                                <hr>
                                <table style="width:100%;">
                                    <tr>
                                        <td style="text-align:right;"><b>Total USD:</b></td>
                                        <td style="text-align:right;">${total_final_usd:,.2f}</td>
                                    </tr>
                                    <tr>
                                        <td style="text-align:right;"><b>Total Bs:</b></td>
                                        <td style="text-align:right;">{total_final_bs:,.2f} Bs</td>
                                    </tr>
                                    <tr>
                                        <td style="text-align:right;"><b>Vuelto:</b></td>
                                        <td style="text-align:right;">${vuelto_usd:.2f} / {(vuelto_usd * tasa):,.2f} Bs</td>
                                    </tr>
                                </table>
                                <p style="text-align:center; margin-top:20px;">¡Gracias por su compra!</p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.session_state.mesas[st.session_state.mesa_actual]['carrito'] = []
                        if mesa_actual['nombre'] not in ['Barra', 'Para llevar']:
                            st.session_state.mesas[st.session_state.mesa_actual]['cliente'] = ''
                        
                        if st.button("🔄 Cerrar y continuar"):
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"Error al procesar venta: {e}")
            
            with col_btn3:
                if len(carrito) > 0 and mesa_actual['nombre'] not in ['Barra', 'Para llevar']:
                    if st.button("⏸️ Dejar pendiente", use_container_width=True):
                        st.session_state.mesa_actual = 'mesa_1'
                        st.rerun()

# ============================================
# MÓDULO 3: GASTOS
# ============================================
elif opcion == "💸 GASTOS":
    requiere_turno()
    requiere_usuario()
    
    id_turno = st.session_state.id_turno
    st.markdown("<h1 class='main-header'>💸 Gestión de Gastos</h1>", unsafe_allow_html=True)
    
    try:
        if st.session_state.online_mode:
            response = db.table("gastos").select("*").eq("id_cierre", id_turno).order("fecha", desc=True).execute()
            df_gastos = pd.DataFrame(response.data) if response.data else pd.DataFrame()
            OfflineManager.guardar_datos_local(f'gastos_{id_turno}', df_gastos.to_dict('records'))
        else:
            datos_local = OfflineManager.obtener_datos_local(f'gastos_{id_turno}')
            df_gastos = pd.DataFrame(datos_local) if datos_local else pd.DataFrame()
        
        if not df_gastos.empty:
            st.subheader("📋 Gastos del turno")
            if 'fecha' in df_gastos.columns:
                df_gastos['fecha'] = pd.to_datetime(df_gastos['fecha']).dt.strftime('%d/%m/%Y %H:%M')
            
            columnas_mostrar = ['fecha', 'descripcion', 'monto_usd']
            if 'categoria' in df_gastos.columns:
                columnas_mostrar.append('categoria')
            if 'estado' in df_gastos.columns:
                columnas_mostrar.append('estado')
            
            st.dataframe(
                df_gastos[columnas_mostrar],
                use_container_width=True,
                hide_index=True
            )
            
            total_gastos = df_gastos['monto_usd'].sum()
            st.metric("💰 Total gastos USD", f"${total_gastos:,.2f}")
            
            if st.button("📥 Exportar gastos a Excel", use_container_width=True):
                export_df = df_gastos[['fecha', 'descripcion', 'monto_usd', 'categoria']].copy()
                export_df.columns = ['Fecha', 'Descripción', 'Monto USD', 'Categoría']
                href = exportar_excel(export_df, f"gastos_turno_{id_turno}_{datetime.now().strftime('%Y%m%d')}")
                st.markdown(href, unsafe_allow_html=True)
        else:
            st.info("No hay gastos registrados en este turno")
    
    except Exception as e:
        st.error(f"Error cargando gastos: {e}")
    
    st.divider()
    
    with st.form("nuevo_gasto"):
        st.subheader("➕ Registrar nuevo gasto")
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            descripcion = st.text_input("Descripción *", placeholder="Ej: Agua, café, cena empleada...")
            monto_usd = st.number_input("Monto USD *", min_value=0.01, step=0.01, format="%.2f")
        with col_g2:
            categoria = st.selectbox("Categoría", ["", "Servicios", "Insumos", "Personal", "Alimentación", "Otros"])
            monto_bs_extra = st.number_input("Monto extra Bs (opcional)", min_value=0.0, step=10.0, format="%.2f")
        
        submitted = st.form_submit_button("✅ Registrar gasto", use_container_width=True)
        
        if submitted:
            if descripcion and monto_usd > 0:
                try:
                    gasto_data = {
                        "id_cierre": id_turno,
                        "descripcion": descripcion,
                        "monto_usd": monto_usd,
                        "estado": "activo",
                        "fecha": datetime.now().isoformat()
                    }
                    
                    if categoria:
                        gasto_data["categoria"] = categoria
                    if monto_bs_extra > 0:
                        gasto_data["monto_bs_extra"] = monto_bs_extra
                    
                    if st.session_state.online_mode:
                        db.table("gastos").insert(gasto_data).execute()
                    else:
                        if 'operaciones_pendientes' not in st.session_state:
                            st.session_state.operaciones_pendientes = []
                        st.session_state.operaciones_pendientes.append({
                            'tipo': 'insert',
                            'tabla': 'gastos',
                            'datos': gasto_data
                        })
                    
                    st.success("✅ Gasto registrado correctamente")
                    time.sleep(1)
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error al registrar gasto: {e}")
            else:
                st.warning("⚠️ Complete los campos obligatorios (*)")

# ============================================
# MÓDULO 4: HISTORIAL DE VENTAS (TODOS LOS TURNOS)
# ============================================
elif opcion == "📜 HISTORIAL":
    requiere_usuario()
    
    st.markdown("<h1 class='main-header'>📜 Historial de Ventas</h1>", unsafe_allow_html=True)
    st.markdown(f"""
        <div style='background-color: #e7f3ff; padding: 0.8rem; border-radius: 8px; margin-bottom: 1.5rem;'>
            <span style='font-weight:600;'>👤 Usuario: {st.session_state.usuario_actual['nombre']}</span>
        </div>
    """, unsafe_allow_html=True)
    
    try:
        if st.session_state.online_mode:
            response = db.table("ventas").select("*").order("fecha", desc=True).execute()
            df = pd.DataFrame(response.data) if response.data else pd.DataFrame()
            OfflineManager.guardar_datos_local('ventas_historial', df.to_dict('records'))
        else:
            # <<< NUEVO: Modo offline usar datos locales
            df = pd.DataFrame(OfflineManager.obtener_datos_local('ventas_historial') or [])
        
        if not df.empty:
            df['fecha_dt'] = pd.to_datetime(df['fecha'])
            df['hora'] = df['fecha_dt'].dt.strftime('%H:%M')
            df['fecha_corta'] = df['fecha_dt'].dt.strftime('%d/%m/%Y')
            df['fecha_display'] = df['fecha_dt'].dt.strftime('%d/%m/%Y %H:%M')
            
            st.subheader("🔍 Filtrar ventas")
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                fecha_desde = st.date_input("📅 Desde", value=None, key="hist_desde")
            with col_f2:
                fecha_hasta = st.date_input("📅 Hasta", value=None, key="hist_hasta")
            with col_f3:
                turno_filtro = st.number_input("🔢 Número de turno", min_value=0, value=0, step=1, key="filtro_turno")
            with col_f4:
                estado_filtro = st.selectbox(
                    "Estado",
                    ["Todos", "Finalizado", "Anulado"],
                    key="filtro_estado"
                )
            
            buscar_texto = st.text_input("🔍 Buscar producto", placeholder="Ej: Ron...", key="filtro_buscar")
            
            df_filtrado = df.copy()
            if fecha_desde:
                df_filtrado = df_filtrado[df_filtrado['fecha_dt'].dt.date >= fecha_desde]
            if fecha_hasta:
                df_filtrado = df_filtrado[df_filtrado['fecha_dt'].dt.date <= fecha_hasta]
            if turno_filtro > 0:
                df_filtrado = df_filtrado[df_filtrado['id_cierre'] == turno_filtro]
            if estado_filtro != "Todos":
                df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
            if buscar_texto:
                df_filtrado = df_filtrado[df_filtrado['producto'].str.contains(buscar_texto, case=False, na=False)]
            
            if not df_filtrado.empty:
                df_activas = df_filtrado[df_filtrado['estado'] != 'Anulado']
                total_usd = df_activas['total_usd'].sum() if not df_activas.empty else 0
                total_bs = df_activas['monto_cobrado_bs'].sum() if not df_activas.empty else 0
                cantidad_ventas = len(df_activas)
                promedio_usd = total_usd / cantidad_ventas if cantidad_ventas > 0 else 0
                
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                with col_m1:
                    st.markdown(f"""
                        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                padding: 1rem; border-radius: 10px; color: white; text-align: center;'>
                            <span style='font-size: 0.9rem; opacity: 0.9;'>💰 TOTAL USD</span><br>
                            <span style='font-size: 1.8rem; font-weight: 700;'>${total_usd:,.2f}</span>
                        </div>
                    """, unsafe_allow_html=True)
                
                with col_m2:
                    st.markdown(f"""
                        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                                padding: 1rem; border-radius: 10px; color: white; text-align: center;'>
                            <span style='font-size: 0.9rem; opacity: 0.9;'>💵 TOTAL BS</span><br>
                            <span style='font-size: 1.8rem; font-weight: 700;'>{total_bs:,.0f}</span>
                        </div>
                    """, unsafe_allow_html=True)
                
                with col_m3:
                    st.markdown(f"""
                        <div style='background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                                padding: 1rem; border-radius: 10px; color: white; text-align: center;'>
                            <span style='font-size: 0.9rem; opacity: 0.9;'>📊 VENTAS</span><br>
                            <span style='font-size: 1.8rem; font-weight: 700;'>{cantidad_ventas}</span>
                        </div>
                    """, unsafe_allow_html=True)
                
                with col_m4:
                    st.markdown(f"""
                        <div style='background: linear-gradient(135deg, #5f2c82 0%, #49a09d 100%); 
                                padding: 1rem; border-radius: 10px; color: white; text-align: center;'>
                            <span style='font-size: 0.9rem; opacity: 0.9;'>📈 PROMEDIO</span><br>
                            <span style='font-size: 1.8rem; font-weight: 700;'>${promedio_usd:,.2f}</span>
                        </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                st.markdown("""
                    <style>
                    .venta-row {
                        display: flex;
                        align-items: center;
                        padding: 0.8rem;
                        margin: 0.2rem 0;
                        border-radius: 8px;
                        transition: all 0.2s;
                    }
                    .venta-row:hover {
                        transform: translateX(5px);
                        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                    }
                    .venta-finalizada {
                        background-color: #ffffff;
                        border-left: 4px solid #28a745;
                    }
                    .venta-anulada {
                        background-color: #f8f9fa;
                        border-left: 4px solid #dc3545;
                        opacity: 0.7;
                    }
                    .badge-finalizada {
                        background-color: #28a745;
                        color: white;
                        padding: 0.2rem 0.6rem;
                        border-radius: 12px;
                        font-size: 0.7rem;
                        font-weight: 600;
                    }
                    .badge-anulada {
                        background-color: #dc3545;
                        color: white;
                        padding: 0.2rem 0.6rem;
                        border-radius: 12px;
                        font-size: 0.7rem;
                        font-weight: 600;
                    }
                    </style>
                """, unsafe_allow_html=True)
                
                col_h1, col_h2, col_h3, col_h4, col_h5, col_h6, col_h7, col_h8 = st.columns([0.6, 0.8, 0.8, 2.2, 1.0, 1.0, 0.8, 0.8])
                with col_h1:
                    st.markdown("**Turno**")
                with col_h2:
                    st.markdown("**ID**")
                with col_h3:
                    st.markdown("**Hora**")
                with col_h4:
                    st.markdown("**Productos**")
                with col_h5:
                    st.markdown("**USD**")
                with col_h6:
                    st.markdown("**Bs**")
                with col_h7:
                    st.markdown("**Estado**")
                with col_h8:
                    st.markdown("**Acción**")
                
                st.markdown("<hr style='margin:0; margin-bottom:0.5rem;'>", unsafe_allow_html=True)
                
                for idx, venta in df_filtrado.iterrows():
                    es_anulado = venta['estado'] == 'Anulado'
                    badge = '<span class="badge-anulada">ANULADA</span>' if es_anulado else '<span class="badge-finalizada">FINALIZADA</span>'
                    
                    productos = venta['producto']
                    if len(productos) > 35:
                        productos = productos[:35] + "..."
                    
                    cols = st.columns([0.6, 0.8, 0.8, 2.2, 1.0, 1.0, 0.8, 0.8])
                    with cols[0]:
                        st.markdown(f"<span style='font-weight:500;'>#{venta['id_cierre']}</span>", unsafe_allow_html=True)
                    with cols[1]:
                        st.markdown(f"<span style='font-weight:500;'>#{venta['id']}</span>", unsafe_allow_html=True)
                    with cols[2]:
                        st.markdown(f"<span>{venta['hora']}</span>", unsafe_allow_html=True)
                    with cols[3]:
                        st.markdown(f"<span title='{venta['producto']}'>{productos}</span>", unsafe_allow_html=True)
                    with cols[4]:
                        st.markdown(f"<span style='font-weight:600;'>${venta['total_usd']:,.2f}</span>", unsafe_allow_html=True)
                    with cols[5]:
                        st.markdown(f"<span>{venta['monto_cobrado_bs']:,.0f}</span>", unsafe_allow_html=True)
                    with cols[6]:
                        st.markdown(badge, unsafe_allow_html=True)
                    with cols[7]:
                        if not es_anulado:
                            if st.button("🚫", key=f"btn_anular_{venta['id']}", help="Anular venta"):
                                try:
                                    items = venta.get('items')
                                    if isinstance(items, str):
                                        items = json.loads(items)
                                    
                                    if items and isinstance(items, list):
                                        for item in items:
                                            if 'id' in item and 'cantidad' in item:
                                                if st.session_state.online_mode:
                                                    stock_res = db.table("inventario").select("stock").eq("id", item['id']).execute()
                                                    if stock_res.data:
                                                        stock_actual = stock_res.data[0]['stock']
                                                        db.table("inventario").update({
                                                            "stock": stock_actual + item['cantidad']
                                                        }).eq("id", item['id']).execute()
                                                else:
                                                    if 'operaciones_pendientes' not in st.session_state:
                                                        st.session_state.operaciones_pendientes = []
                                                    st.session_state.operaciones_pendientes.append({
                                                        'tipo': 'anular_venta',
                                                        'id_venta': venta['id'],
                                                        'items': items
                                                    })
                                    
                                    if st.session_state.online_mode:
                                        db.table("ventas").update({"estado": "Anulado"}).eq("id", venta['id']).execute()
                                    
                                    st.success(f"✅ Venta #{venta['id']} anulada")
                                    time.sleep(1)
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"Error al anular: {e}")
                        else:
                            st.markdown("—")
                    
                    if idx < len(df_filtrado) - 1:
                        st.markdown("<hr style='margin:0.2rem 0; opacity:0.3;'>", unsafe_allow_html=True)
                
                if not df_activas.empty:
                    st.markdown(f"""
                        <div style='background-color: #f0f2f6; padding: 1rem; border-radius: 8px; margin-top: 1rem;'>
                            <div style='display: flex; justify-content: space-between; align-items: center;'>
                                <span style='font-weight:600;'>📊 TOTALES EN PANTALLA (ventas activas):</span>
                                <span>
                                    <span style='color: #28a745; font-weight:600;'>${total_usd:,.2f}</span> | 
                                    <span style='color: #007bff; font-weight:600;'>{total_bs:,.0f} Bs</span>
                                </span>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("📭 No hay ventas que coincidan con los filtros")
        else:
            st.info("📭 No hay ventas registradas en el sistema")
            
    except Exception as e:
        st.error(f"Error cargando historial: {e}")
        st.exception(e)

# ============================================
# MÓDULO 5: CIERRE DE CAJA (COMPLETO)
# ============================================
elif opcion == "📊 CIERRE DE CAJA":
    st.markdown("<h1 class='main-header'>📊 Cierre de Caja</h1>", unsafe_allow_html=True)

    tab_c1, tab_c2 = st.tabs(["🔓 Cierre del turno actual", "📋 Historial de cierres"])

    with tab_c1:
        if not st.session_state.id_turno:
            st.warning("🔓 No hay turno activo. Complete para abrir caja:")

            with st.form("form_apertura"):
                st.subheader("📝 Datos de apertura")
                col1, col2 = st.columns(2)
                with col1:
                    tasa_apertura = st.number_input("💱 Tasa BCV (Bs/$)", min_value=1.0, value=60.0, step=0.5, format="%.2f")
                    fondo_bs = st.number_input("💰 Fondo inicial Bs", min_value=0.0, value=0.0, step=10.0, format="%.2f")
                with col2:
                    fondo_usd = st.number_input("💰 Fondo inicial USD", min_value=0.0, value=0.0, step=5.0, format="%.2f")
                    st.info(f"👤 Abre: {st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo'}")

                if st.form_submit_button("🚀 ABRIR CAJA", type="primary", use_container_width=True):
                    try:
                        data = {
                            "tasa_apertura": tasa_apertura,
                            "fondo_bs": fondo_bs,
                            "fondo_usd": fondo_usd,
                            "monto_apertura": fondo_usd,
                            "estado": "abierto",
                            "fecha_apertura": datetime.now().isoformat(),
                            "usuario_apertura": st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo'
                        }
                        res = db.table("cierres").insert(data).execute()
                        if res.data:
                            st.session_state.id_turno = res.data[0]['id']
                            st.session_state.tasa_dia = tasa_apertura
                            st.session_state.fondo_bs = fondo_bs
                            st.session_state.fondo_usd = fondo_usd
                            st.success(f"✅ Turno #{res.data[0]['id']} abierto")
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            st.stop()

        id_turno = st.session_state.id_turno
        tasa = st.session_state.tasa_dia
        fondo_bs_ini = st.session_state.get('fondo_bs', 0)
        fondo_usd_ini = st.session_state.get('fondo_usd', 0)

        turno_info = db.table("cierres").select("*").eq("id", id_turno).execute()
        usuario_apertura = turno_info.data[0].get('usuario_apertura', 'N/A') if turno_info.data else 'N/A'

        col_info1, col_info2, col_info3 = st.columns(3)
        col_info1.success(f"📍 Turno activo: #{id_turno}")
        col_info2.info(f"👤 Abrió: {usuario_apertura}")
        col_info3.info(f"💱 Tasa: {tasa:.2f} Bs/$")

        ventas = db.table("ventas").select("*").eq("id_cierre", id_turno).eq("estado", "Finalizado").execute().data or []
        gastos = db.table("gastos").select("*").eq("id_cierre", id_turno).execute().data or []

        total_ventas_usd = sum(float(v.get('total_usd', 0)) for v in ventas)
        total_costos = sum(float(v.get('costo_venta', 0)) for v in ventas)
        total_gastos = sum(float(g.get('monto_usd', 0)) for g in gastos)

        total_pagos_usd = sum(
            float(v.get('pago_divisas', 0)) +
            float(v.get('pago_zelle', 0)) +
            float(v.get('pago_otros', 0)) for v in ventas
        )
        total_pagos_bs = sum(
            float(v.get('pago_efectivo', 0)) +
            float(v.get('pago_movil', 0)) +
            float(v.get('pago_punto', 0)) for v in ventas
        )

        ganancia_bruta = total_ventas_usd - total_costos
        ganancia_neta = ganancia_bruta - total_gastos
        reposicion = total_costos

        total_efectivo_usd = sum(float(v.get('pago_divisas', 0)) for v in ventas)
        total_zelle = sum(float(v.get('pago_zelle', 0)) for v in ventas)
        total_otros_usd = sum(float(v.get('pago_otros', 0)) for v in ventas)
        total_efectivo_bs = sum(float(v.get('pago_efectivo', 0)) for v in ventas)
        total_movil = sum(float(v.get('pago_movil', 0)) for v in ventas)
        total_punto = sum(float(v.get('pago_punto', 0)) for v in ventas)

        st.subheader("📈 Resumen del turno")
        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
        col_r1.metric("💰 Ventas totales", f"${total_ventas_usd:,.2f}")
        col_r2.metric("📦 Reposición", f"${reposicion:,.2f}")
        col_r3.metric("💸 Gastos", f"${total_gastos:,.2f}")
        col_r4.metric("📊 Ganancia neta", f"${ganancia_neta:,.2f}")

        with st.expander("💰 Ver desglose por método de pago", expanded=True):
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.markdown("**💵 Pagos en USD**")
                st.metric("Efectivo USD", f"${total_efectivo_usd:,.2f}")
                st.metric("Zelle USD", f"${total_zelle:,.2f}")
                st.metric("Otros USD", f"${total_otros_usd:,.2f}")
            with col_d2:
                st.markdown("**💵 Pagos en Bs**")
                st.metric("Efectivo Bs", f"{total_efectivo_bs:,.2f} Bs")
                st.metric("Pago Móvil Bs", f"{total_movil:,.2f} Bs")
                st.metric("Punto Venta Bs", f"{total_punto:,.2f} Bs")

        st.divider()
        st.subheader("🧮 Ingreso de montos físicos")

        with st.form("form_ingreso_montos"):
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                st.markdown("**💰 Bolívares (Bs)**")
                efec_bs = st.number_input("Efectivo Bs", min_value=0.0, value=0.0, step=100.0, format="%.2f", key="bs_efectivo")
                pmovil_bs = st.number_input("Pago Móvil Bs", min_value=0.0, value=0.0, step=100.0, format="%.2f", key="bs_pmovil")
                punto_bs = st.number_input("Punto Venta Bs", min_value=0.0, value=0.0, step=100.0, format="%.2f", key="bs_punto")
            with col_f2:
                st.markdown("**💰 Dólares (USD)**")
                efec_usd = st.number_input("Efectivo USD", min_value=0.0, value=0.0, step=5.0, format="%.2f", key="usd_efectivo")
                zelle_usd = st.number_input("Zelle USD", min_value=0.0, value=0.0, step=5.0, format="%.2f", key="usd_zelle")
                otros_usd = st.number_input("Otros USD", min_value=0.0, value=0.0, step=5.0, format="%.2f", key="usd_otros")

            observaciones = st.text_area("📝 Observaciones (opcional)", placeholder="Ej: Todo en orden...")
            st.markdown("---")
            previsualizar = st.form_submit_button("👁️ PREVISUALIZAR CIERRE", use_container_width=True)

            if previsualizar:
                st.session_state.montos_fisicos = {
                    'efec_bs': efec_bs, 'pmovil_bs': pmovil_bs, 'punto_bs': punto_bs,
                    'efec_usd': efec_usd, 'zelle_usd': zelle_usd, 'otros_usd': otros_usd,
                    'observaciones': observaciones
                }
                st.session_state.montos_calculados = True
                st.rerun()

        if st.session_state.get('montos_calculados', False):
            montos = st.session_state.montos_fisicos
            total_bs_fisico = montos['efec_bs'] + montos['pmovil_bs'] + montos['punto_bs']
            total_usd_fisico = montos['efec_usd'] + montos['zelle_usd'] + montos['otros_usd']

            esperado_bs = fondo_bs_ini + total_pagos_bs - (total_gastos * tasa)
            esperado_usd = fondo_usd_ini + total_pagos_usd - total_gastos

            diff_bs = total_bs_fisico - esperado_bs
            diff_usd = total_usd_fisico - esperado_usd
            diff_total = diff_usd + (diff_bs / tasa if tasa > 0 else 0)

            st.subheader("📊 Comparación Caja vs Sistema")
            col_x1, col_x2 = st.columns(2)
            with col_x1:
                st.markdown("**🇻🇪 Bolívares**")
                st.metric("Esperado", f"{esperado_bs:,.2f} Bs")
                st.metric("Físico", f"{total_bs_fisico:,.2f} Bs")
                st.metric("Diferencia", f"{diff_bs:+,.2f} Bs")
            with col_x2:
                st.markdown("**🇺🇸 Dólares**")
                st.metric("Esperado", f"${esperado_usd:,.2f}")
                st.metric("Físico", f"${total_usd_fisico:,.2f}")
                st.metric("Diferencia", f"${diff_usd:+,.2f}")

            st.metric("DIFERENCIA TOTAL", f"${diff_total:+,.2f}")

            if abs(diff_total) < 0.1:
                st.success("✅ **¡CAJA CUADRADA!** Todo coincide.")
            elif diff_total > 0:
                st.warning(f"🟡 **SOBRANTE:** +${diff_total:,.2f} USD a favor de la caja")
            else:
                st.error(f"🔴 **FALTANTE:** -${abs(diff_total):,.2f} USD en caja")

            st.warning("⚠️ Una vez cerrado, no podrá modificar este turno.")
            confirmar = st.checkbox("✅ Confirmo que los datos del conteo son correctos")

            if st.button("🔒 CONFIRMAR Y CERRAR TURNO", type="primary", use_container_width=True, disabled=not confirmar):
                try:
                    datos_cierre = {
                        "fecha_cierre": datetime.now().isoformat(),
                        "total_ventas": total_ventas_usd,
                        "total_costos": total_costos,
                        "total_ganancias": ganancia_neta,
                        "diferencia": diff_total,
                        "tasa_cierre": tasa,
                        "estado": "cerrado",
                        "usuario_cierre": st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo',
                        "observaciones": montos['observaciones'],
                        "fondo_bs_final": total_bs_fisico,
                        "fondo_usd_final": total_usd_fisico,
                        "efectivo_bs_fisico": montos['efec_bs'],
                        "pmovil_fisico": montos['pmovil_bs'],
                        "punto_fisico": montos['punto_bs'],
                        "efectivo_usd_fisico": montos['efec_usd'],
                        "zelle_fisico": montos['zelle_usd'],
                        "otros_fisico": montos['otros_usd']
                    }

                    db.table("cierres").update(datos_cierre).eq("id", id_turno).execute()
                    db.table("gastos").update({"estado": "cerrado"}).eq("id_cierre", id_turno).execute()

                    st.session_state.id_turno = None
                    st.session_state.montos_calculados = False
                    st.balloons()
                    st.success("✅ Turno cerrado exitosamente!")

                    st.markdown("---")
                    st.subheader("📄 REPORTE DE CIERRE")
                    col_y1, col_y2 = st.columns(2)
                    with col_y1:
                        st.markdown(f"**Turno:** #{id_turno}")
                        st.markdown(f"**Abrió:** {usuario_apertura}")
                        st.markdown(f"**Cerró:** {st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo'}")
                        st.markdown(f"**Fecha:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                    with col_y2:
                        st.markdown(f"**Ventas:** ${total_ventas_usd:,.2f}")
                        st.markdown(f"**Reposición:** ${reposicion:,.2f}")
                        st.markdown(f"**Gastos:** ${total_gastos:,.2f}")
                        st.markdown(f"**Ganancia neta:** ${ganancia_neta:,.2f}")
                    st.markdown(f"**Diferencia total:** ${diff_total:+,.2f}")

                    if st.button("🔄 Volver al inicio"):
                        st.rerun()

                except Exception as e:
                    st.error(f"Error al cerrar: {e}")

            if st.button("✏️ CORREGIR MONTOS", use_container_width=True):
                st.session_state.montos_calculados = False
                st.rerun()

    with tab_c2:
        st.subheader("📋 Historial de turnos cerrados")
        try:
            cierres = db.table("cierres").select("*").eq("estado", "cerrado").order("fecha_cierre", desc=True).execute()
            df_cierres = pd.DataFrame(cierres.data) if cierres.data else pd.DataFrame()

            if not df_cierres.empty:
                df_cierres['fecha_apertura'] = pd.to_datetime(df_cierres['fecha_apertura']).dt.strftime('%d/%m/%Y %H:%M')
                df_cierres['fecha_cierre'] = pd.to_datetime(df_cierres['fecha_cierre']).dt.strftime('%d/%m/%Y %H:%M')

                st.dataframe(
                    df_cierres[['id', 'fecha_apertura', 'fecha_cierre', 'usuario_apertura', 'usuario_cierre',
                                'total_ventas', 'total_ganancias', 'diferencia']],
                    column_config={
                        "id": "Turno",
                        "fecha_apertura": "Apertura",
                        "fecha_cierre": "Cierre",
                        "usuario_apertura": "Abrió",
                        "usuario_cierre": "Cerró",
                        "total_ventas": st.column_config.NumberColumn("Ventas USD", format="$%.2f"),
                        "total_ganancias": st.column_config.NumberColumn("Ganancias USD", format="$%.2f"),
                        "diferencia": st.column_config.NumberColumn("Diferencia USD", format="$%.2f")
                    },
                    use_container_width=True,
                    hide_index=True
                )

                if st.button("📥 Exportar historial a Excel", use_container_width=True):
                    export_df = df_cierres[['id', 'fecha_apertura', 'fecha_cierre', 'usuario_apertura', 'usuario_cierre',
                                            'total_ventas', 'total_ganancias', 'diferencia']].copy()
                    export_df.columns = ['Turno', 'Apertura', 'Cierre', 'Abrió', 'Cerró',
                                         'Ventas USD', 'Ganancias USD', 'Diferencia USD']
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        export_df.to_excel(writer, index=False, sheet_name='Cierres')
                    excel_data = output.getvalue()
                    b64 = base64.b64encode(excel_data).decode()
                    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="historial_cierres.xlsx">📥 Descargar Excel</a>'
                    st.markdown(href, unsafe_allow_html=True)
            else:
                st.info("No hay turnos cerrados registrados.")
        except Exception as e:
            st.error(f"Error cargando historial de cierres: {e}")
