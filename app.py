import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta
import time
import json
import hashlib
import base64
from io import BytesIO
import math

# ============================================
# CONFIGURACIÓN DE CREDENCIALES
# ============================================
URL = "https://phcnjozdhhyvrcbyzahs.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBoY25qb3pkaGh5dnJjYnl6YWhzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ4ODY5NzksImV4cCI6MjA5MDQ2Mjk3OX0.pmFqG1qjuOiEK_SmNXpoimLcT-muLPRtfmUN62h7OYM"

db = create_client(URL, KEY)

def hash_clave(clave):
    return hashlib.sha256(clave.encode()).hexdigest()

# ============================================
# CONFIGURACIÓN INICIAL
# ============================================
st.set_page_config(page_title="BODEGÓN VYM", page_icon="🛒", layout="wide", initial_sidebar_state="expanded")

if 'tema' not in st.session_state:
    st.session_state.tema = 'claro'

def aplicar_tema():
    if st.session_state.tema == 'oscuro':
        return """
            <style>
            .stApp { background-color: #1e1e1e; color: #ffffff; }
            .main-header { color: #FF8C00 !important; }
            .stMarkdown, .stText, p, span, label, h1, h2, h3, h4 { color: #ffffff !important; }
            .stButton > button { background-color: #FF8C00 !important; color: #000000 !important; }
            .stButton > button:hover { background-color: #E67E00 !important; }
            .stDataFrame { background-color: #2d2d2d; }
            </style>
        """
    else:
        return """
            <style>
            .stApp { background-color: #ffffff; color: #000000; }
            .main-header { color: #1E88E5 !important; }
            .stButton > button { background-color: #FF8C00 !important; color: #ffffff !important; border: none; }
            .stButton > button:hover { background-color: #E67E00 !important; }
            .stMarkdown, .stText, p, span, label, h1, h2, h3, h4 { color: #000000 !important; }
            </style>
        """

st.markdown(aplicar_tema(), unsafe_allow_html=True)

st.markdown("""
    <style>
    .main-header { font-size: 2.5rem; font-weight: 700; text-align: center; margin-bottom: 2rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.1); color: #1E88E5 !important; }
    .stButton > button { background-color: #FF8C00; color: white; border-radius: 8px; font-weight: 600; transition: all 0.3s ease; border: none; }
    .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(0,0,0,0.2); background-color: #E67E00; }
    .success-box { background-color: #d4edda; color: #155724; padding: 1rem; border-radius: 8px; border-left: 5px solid #28a745; }
    .warning-box { background-color: #fff3cd; color: #856404; padding: 1rem; border-radius: 8px; border-left: 5px solid #ffc107; }
    .error-box { background-color: #f8d7da; color: #721c24; padding: 1rem; border-radius: 8px; border-left: 5px solid #dc3545; }
    </style>
""", unsafe_allow_html=True)

# ============================================
# INICIALIZAR VARIABLES DE SESIÓN
# ============================================
if 'id_turno' not in st.session_state: st.session_state.id_turno = None
if 'tasa_dia' not in st.session_state: st.session_state.tasa_dia = 60.0
if 'tasa_divisas' not in st.session_state: st.session_state.tasa_divisas = 70.0
if 'fondo_bs' not in st.session_state: st.session_state.fondo_bs = 0
if 'fondo_usd' not in st.session_state: st.session_state.fondo_usd = 0
if 'usuario_cargado' not in st.session_state: st.session_state.usuario_cargado = False
if 'usuario_actual' not in st.session_state: st.session_state.usuario_actual = None

# ============================================
# LOGIN Y PERSISTENCIA
# ============================================
def login(usuario, clave):
    try:
        response = db.table("usuarios").select("*").eq("usuario", usuario).eq("activo", True).execute()
        if response.data:
            user = response.data[0]
            if user['clave'] == hash_clave(clave):
                st.session_state.usuario_actual = {'id': user['id'], 'usuario': user['usuario'], 'nombre': user['nombre'], 'rol': user['rol'], 'activo': user['activo']}
                st.markdown(f"<script>localStorage.setItem('usuario_actual', JSON.stringify({json.dumps(st.session_state.usuario_actual)}));</script>", unsafe_allow_html=True)
                return True
    except Exception as e:
        st.toast(f"Error en login: {e}", icon="❌")
    return False

def logout():
    st.session_state.usuario_actual = None
    st.session_state.id_turno = None
    st.markdown("<script>localStorage.removeItem('usuario_actual');</script>", unsafe_allow_html=True)
    if 'usuario' in st.query_params: del st.query_params['usuario']
    if 'turno' in st.query_params: del st.query_params['turno']
    st.rerun()

if not st.session_state.usuario_cargado:
    if 'usuario_local' not in st.query_params:
        st.markdown("""
            <script>
            const user = localStorage.getItem('usuario_actual');
            if (user) { window.location.href = window.location.pathname + '?usuario_local=' + encodeURIComponent(user); }
            else { window.location.href = window.location.pathname + '?usuario_local=null'; }
            </script>
        """, unsafe_allow_html=True)
        st.session_state.usuario_cargado = True
        st.rerun()
    else:
        usuario_json = st.query_params.get('usuario_local')
        if usuario_json and usuario_json != 'null':
            try: st.session_state.usuario_actual = json.loads(usuario_json)
            except: st.session_state.usuario_actual = None
        else:
            st.session_state.usuario_actual = None
        st.query_params.clear()
        st.session_state.usuario_cargado = True

def es_admin():
    user = st.session_state.get('usuario_actual')
    return user is not None and user.get('rol') == 'admin'

def tiene_permiso(modulo):
    user = st.session_state.get('usuario_actual')
    if not user: return False
    if user.get('rol') == 'admin': return True
    modulos_empleado = ["🛒 PUNTO DE VENTA", "💸 GASTOS", "📜 HISTORIAL", "📊 CIERRE DE CAJA"]
    return modulo in modulos_empleado

# ============================================
# VERIFICAR TURNO ACTIVO
# ============================================
def restaurar_turno_activo():
    if st.session_state.usuario_actual is None: return None
    try:
        resp = db.table("cierres").select("*").eq("estado", "abierto").eq("usuario_apertura", st.session_state.usuario_actual['nombre']).order("fecha_apertura", desc=True).limit(1).execute()
        if resp.data:
            turno = resp.data[0]
            st.session_state.id_turno = turno['id']
            st.session_state.tasa_dia = turno.get('tasa_apertura', 60.0)
            st.session_state.tasa_divisas = turno.get('tasa_divisas', 70.0)
            st.session_state.fondo_bs = turno.get('fondo_bs', 0)
            st.session_state.fondo_usd = turno.get('fondo_usd', 0)
            st.query_params['turno'] = str(turno['id'])
            return turno['id']
    except: pass
    return None

if st.session_state.id_turno is None and st.session_state.usuario_actual is not None:
    restaurar_turno_activo()

# ============================================
# FUNCIONES DE USUARIOS
# ============================================
def listar_usuarios():
    try: return db.table("usuarios").select("*").execute().data or []
    except: return []

def actualizar_usuario(id_usuario, campo, valor):
    try:
        if campo == 'clave': valor = hash_clave(valor)
        db.table("usuarios").update({campo: valor}).eq("id", id_usuario).execute()
        return True
    except: return False

def crear_usuario(usuario, clave, nombre, rol):
    try:
        db.table("usuarios").insert({"usuario": usuario, "clave": hash_clave(clave), "nombre": nombre, "rol": rol, "activo": True}).execute()
        return True
    except: return False

# ============================================
# MENÚ LATERAL
# ============================================
with st.sidebar:
    st.markdown("""
        <div style="background: linear-gradient(135deg, #1E88E5 0%, #FF8C00 100%); padding: 2rem 1rem; border-radius: 0 0 20px 20px; text-align: center; margin-top: -1rem; margin-bottom: 1rem;">
            <h1 style="color: white; margin: 0; font-size: 2.2rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">🛒 BODEGÓN VYM</h1>
            <p style="color: rgba(255,255,255,0.95); margin-top: 0.5rem; font-style: italic;">Víveres, carnes y licores</p>
        </div>
    """, unsafe_allow_html=True)
    
    col_tema1, col_tema2 = st.columns(2)
    with col_tema1:
        if st.button("☀️ Claro", use_container_width=True): st.session_state.tema = 'claro'; st.rerun()
    with col_tema2:
        if st.button("🌙 Oscuro", use_container_width=True): st.session_state.tema = 'oscuro'; st.rerun()
    
    st.divider()
    
    if not st.session_state.usuario_actual:
        with st.expander("🔐 Acceso al sistema", expanded=True):
            usuarios_activos = []
            try:
                res = db.table("usuarios").select("usuario").eq("activo", True).execute()
                if res.data: usuarios_activos = [u['usuario'] for u in res.data]
            except: pass
            if not usuarios_activos: usuarios_activos = ["admin"]
            
            col_user1, col_user2 = st.columns(2)
            with col_user1:
                usuario_sel = st.selectbox("Usuario", usuarios_activos)
            with col_user2:
                clave_input = st.text_input("Clave", type="password")
            if st.button("✅ Ingresar", use_container_width=True):
                if login(usuario_sel, clave_input):
                    st.toast(f"Bienvenido {st.session_state.usuario_actual['nombre']}", icon="👋")
                    time.sleep(0.5); st.rerun()
                else:
                    st.toast("Usuario o clave incorrecta", icon="❌")
    else:
        rol_texto = "Admin" if st.session_state.usuario_actual['rol'] == 'admin' else "Empleado"
        st.success(f"👤 Usuario: {st.session_state.usuario_actual['nombre']} ({rol_texto})")
        if st.button("🚪 Cerrar sesión", use_container_width=True): logout()
    
    st.divider()
    
    if st.session_state.id_turno:
        with st.container(border=True):
            st.markdown("**📊 INFORMACIÓN DEL TURNO**")
            st.metric("💱 Tasa BCV", f"{st.session_state.tasa_dia:.2f} Bs/$")
            
            try:
                turno_info = db.table("cierres").select("tasa_divisas").eq("id", st.session_state.id_turno).execute()
                if turno_info.data and turno_info.data[0].get('tasa_divisas') is not None:
                    tasa_divisas_actual = float(turno_info.data[0]['tasa_divisas'])
                else:
                    tasa_divisas_actual = st.session_state.tasa_divisas
            except: tasa_divisas_actual = st.session_state.tasa_divisas
            
            if es_admin():
                nueva_tasa_divisas = st.number_input("💱 Tasa divisas (mercado)", min_value=1.0, max_value=999.0, value=tasa_divisas_actual, step=0.5, format="%.2f", key="tasa_divisas_sidebar")
                if st.button("Actualizar tasa divisas", use_container_width=True):
                    try:
                        db.table("cierres").update({"tasa_divisas": nueva_tasa_divisas}).eq("id", st.session_state.id_turno).execute()
                        st.session_state.tasa_divisas = nueva_tasa_divisas
                        st.toast("Tasa divisas actualizada", icon="✅"); time.sleep(0.5); st.rerun()
                    except Exception as e: st.toast(f"Error: {e}", icon="❌")
            else:
                st.metric("💱 Tasa divisas (mercado)", f"{tasa_divisas_actual:.2f} Bs/$")
            
            st.metric("💰 Fondo inicial Bs", f"{st.session_state.fondo_bs:,.2f} Bs")
            st.metric("💰 Fondo inicial USD", f"${st.session_state.fondo_usd:,.2f}")
            st.caption("Datos fijados al abrir el turno")
    else:
        with st.container(border=True):
            st.markdown("**💱 TASA BCV**")
            st.metric("Bs/USD", f"{st.session_state.tasa_dia:.2f}")
            st.caption("Tasa por defecto (sin turno activo)")
    
    st.divider()
    
    modulos_disponibles = []
    if tiene_permiso("📦 INVENTARIO"): modulos_disponibles.append("📦 INVENTARIO")
    if tiene_permiso("🛒 PUNTO DE VENTA"): modulos_disponibles.append("🛒 PUNTO DE VENTA")
    if tiene_permiso("💸 GASTOS"): modulos_disponibles.append("💸 GASTOS")
    if tiene_permiso("📜 HISTORIAL"): modulos_disponibles.append("📜 HISTORIAL")
    if tiene_permiso("📊 CIERRE DE CAJA"): modulos_disponibles.append("📊 CIERRE DE CAJA")
    if es_admin(): modulos_disponibles.append("👥 ADMINISTRACIÓN")
    
    opcion = st.radio("MÓDULOS", modulos_disponibles, label_visibility="collapsed")
    
    st.divider()
    st.caption("Sistema en línea")
    if st.session_state.id_turno: st.info(f"📍 Turno activo: #{st.session_state.id_turno}")
    else: st.error("🔴 Caja cerrada")

# ============================================
# FUNCIONES AUXILIARES
# ============================================
def requiere_turno():
    if not st.session_state.id_turno:
        st.warning("⚠️ No hay un turno activo. Debe abrir caja en el módulo 'Cierre de Caja'."); st.stop()

def requiere_usuario():
    if not st.session_state.usuario_actual:
        st.warning("⚠️ Debe iniciar sesión para acceder a este módulo."); st.stop()

def formatear_usd(valor): return f"${valor:,.2f}"
def formatear_bs(valor): return f"{valor:,.2f} Bs"

def exportar_excel(df, nombre_archivo):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    b64 = base64.b64encode(output.getvalue()).decode()
    return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{nombre_archivo}.xlsx">📥 Descargar Excel</a>'

# ============================================
# MÓDULO 1: INVENTARIO (CON PRECIO OFERTA DIVISAS)
# ============================================
if opcion == "📦 INVENTARIO":
    st.markdown("<h1 class='main-header'>📦 Gestión de Inventario</h1>", unsafe_allow_html=True)
    
    CATEGORIAS = ["VIVERES", "CONFITERIA", "CHARCUTERIA", "BEBIDAS", "LACTEOS", "SNACK", "BISUTERIA", "PAPELERIA", "DETERGENTES", "ASEO PERSONAL", "QUINCALLERIA", "LICORES", "OTROS"]
    
    @st.cache_data(ttl=60, show_spinner=False)
    def cargar_inventario_completo():
        try:
            response = db.table("inventario").select("*").order("nombre").execute()
            return pd.DataFrame(response.data) if response.data else pd.DataFrame()
        except Exception as e:
            st.error(f"Error cargando inventario: {e}"); return pd.DataFrame()
    
    try:
        df = cargar_inventario_completo()
        if not df.empty:
            if 'categoria' not in df.columns: df['categoria'] = 'OTROS'
            if 'codigo_barras' not in df.columns: df['codigo_barras'] = ''
            if 'precio_divisas' not in df.columns: df['precio_divisas'] = 0.0
        
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Ver Inventario", "➕ Agregar Producto", "📊 Estadísticas", "📥 Respaldos"])
        
        with tab1:
            col_f1, col_f2, col_f3, col_f4 = st.columns([2, 1, 1, 1])
            with col_f1:
                busqueda = st.text_input("🔍 Buscar producto", placeholder="Nombre o código...")
            with col_f2:
                categoria_filtro = st.selectbox("Categoría", ["Todas"] + CATEGORIAS)
            with col_f3:
                ver_bajo_stock = st.checkbox("⚠️ Solo stock bajo")
            with col_f4:
                if st.button("📤 Exportar a Excel", use_container_width=True) and not df.empty:
                    export_df = df[['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'precio_divisas', 'min_mayor']].copy()
                    export_df.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Precio Detal $', 'Precio Mayor $', 'Oferta Divisas $', 'Min. Mayor']
                    st.markdown(exportar_excel(export_df, f"inventario_{datetime.now().strftime('%Y%m%d')}"), unsafe_allow_html=True)
            
            if not df.empty:
                df_filtrado = df.copy()
                if busqueda:
                    mask_nombre = df_filtrado['nombre'].str.contains(busqueda, case=False, na=False)
                    if 'codigo_barras' in df_filtrado.columns:
                        mask_codigo = df_filtrado['codigo_barras'].fillna('').astype(str).str.contains(busqueda, case=False, na=False)
                        df_filtrado = df_filtrado[mask_nombre | mask_codigo]
                    else: df_filtrado = df_filtrado[mask_nombre]
                if categoria_filtro != "Todas" and 'categoria' in df_filtrado.columns:
                    df_filtrado = df_filtrado[df_filtrado['categoria'] == categoria_filtro]
                if ver_bajo_stock:
                    df_filtrado = df_filtrado[df_filtrado['stock'] < 5]
                    if len(df_filtrado) > 0: st.warning(f"⚠️ Hay {len(df_filtrado)} productos con stock bajo")
                    else: st.success("✅ No hay productos con stock bajo")
                
                def colorear_stock(val):
                    if val < 5: return 'color: red; font-weight: bold; background-color: #ffe6e6'
                    elif val < 10: return 'color: orange; font-weight: bold;'
                    return 'color: green; font-weight: bold;'
                
                columnas_mostrar = ['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'precio_divisas', 'min_mayor']
                columnas_mostrar = [col for col in columnas_mostrar if col in df_filtrado.columns]
                df_mostrar = df_filtrado[columnas_mostrar].copy()
                df_mostrar.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Detal $', 'Mayor $', 'Oferta Divisas $', 'Mín. Mayor']
                st.dataframe(df_mostrar.style.map(colorear_stock, subset=['Stock']), use_container_width=True, hide_index=True)
                st.caption(f"Mostrando {len(df_filtrado)} de {len(df)} productos")
                
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
                                try: cat_index = CATEGORIAS.index(prod.get('categoria', 'OTROS'))
                                except: cat_index = len(CATEGORIAS) - 1
                                nueva_categoria = st.selectbox("Categoría", CATEGORIAS, index=cat_index)
                                nuevo_stock = st.number_input("Stock", value=float(prod['stock']), min_value=0.0, step=1.0)
                                nuevo_costo = st.number_input("Costo $", value=float(prod['costo']), min_value=0.0, step=0.01)
                                nuevo_codigo = st.text_input("Código de barras", value=prod.get('codigo_barras', ''))
                            with col_e2:
                                nuevo_detal = st.number_input("Precio Detal $", value=float(prod['precio_detal']), min_value=0.0, step=0.01)
                                nuevo_mayor = st.number_input("Precio Mayor $", value=float(prod['precio_mayor']), min_value=0.0, step=0.01)
                                nuevo_precio_divisas = st.number_input("Precio Oferta en Divisas $", value=float(prod.get('precio_divisas', 0)), min_value=0.0, step=0.01, help="0 si no aplica")
                                nuevo_min = st.number_input("Mín. Mayor", value=int(prod['min_mayor']), min_value=1, step=1)
                            if st.form_submit_button("💾 Guardar Cambios", use_container_width=True):
                                try:
                                    datos_actualizados = {"nombre": nuevo_nombre, "categoria": nueva_categoria, "stock": nuevo_stock, "costo": nuevo_costo, "precio_detal": nuevo_detal, "precio_mayor": nuevo_mayor, "precio_divisas": nuevo_precio_divisas, "min_mayor": nuevo_min}
                                    if nuevo_codigo: datos_actualizados["codigo_barras"] = nuevo_codigo
                                    db.table("inventario").update(datos_actualizados).eq("id", prod['id']).execute()
                                    st.toast("✅ Producto actualizado", icon="✅"); time.sleep(0.5); st.rerun()
                                except Exception as e: st.toast(f"Error: {e}", icon="❌")
                        
                        st.divider()
                        st.subheader("🔗 Códigos de barras alternos")
                        with st.container():
                            alt_codes = db.table("codigos_alternos").select("*").eq("producto_id", int(prod['id'])).execute()
                            if alt_codes.data:
                                st.write("**Códigos actuales:**")
                                for ac in alt_codes.data:
                                    col1, col2 = st.columns([4, 1])
                                    with col1: st.code(ac["codigo"])
                                    with col2:
                                        if st.button("❌", key=f"del_{ac['id']}"):
                                            db.table("codigos_alternos").delete().eq("id", int(ac["id"])).execute()
                                            st.toast("Código eliminado", icon="✅"); time.sleep(0.5); st.rerun()
                            else: st.info("No hay códigos alternos.")
                            nuevo_codigo_alt = st.text_input("Nuevo código (opcional)", key=f"new_alt_{prod['id']}")
                            if st.button("➕ Agregar código", key=f"add_alt_{prod['id']}"):
                                if nuevo_codigo_alt.strip():
                                    try:
                                        db.table("codigos_alternos").insert({"producto_id": int(prod['id']), "codigo": nuevo_codigo_alt.strip()}).execute()
                                        st.toast("Código agregado", icon="✅"); time.sleep(0.5); st.rerun()
                                    except Exception as e: st.toast(f"Error: {e}", icon="❌")
                                else: st.warning("Escribe un código válido")
                
                st.divider()
                st.subheader("🗑️ Eliminar producto")
                if es_admin():
                    col_d1, col_d2 = st.columns(2)
                    with col_d1:
                        producto_eliminar = st.selectbox("Seleccionar producto", [""] + df['nombre'].tolist(), key="eliminar")
                    if producto_eliminar and st.button("❌ Eliminar", type="primary", use_container_width=True):
                        try:
                            db.table("inventario").delete().eq("nombre", producto_eliminar).execute()
                            st.toast(f"Producto eliminado", icon="🗑️"); time.sleep(0.5); st.rerun()
                        except Exception as e: st.toast(f"Error: {e}", icon="❌")
                else: st.info("🔒 Solo el administrador puede eliminar productos.")
            else: st.info("No hay productos en el inventario")
        
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
                    precio_divisas = st.number_input("Precio Oferta Divisas $ (opcional)", min_value=0.0, step=0.01, format="%.2f", help="0 si no aplica")
                    min_mayor = st.number_input("Mínimo para Mayor *", min_value=1, value=6, step=1)
                if st.form_submit_button("📦 Registrar Producto", use_container_width=True):
                    if not nombre: st.error("El nombre es obligatorio")
                    elif stock < 0 or costo < 0 or precio_detal <= 0: st.error("Verifique los valores")
                    else:
                        existe = db.table("inventario").select("*").eq("nombre", nombre).execute()
                        if existe.data: st.error(f"Ya existe '{nombre}'")
                        else:
                            datos_nuevos = {"nombre": nombre, "categoria": categoria, "stock": stock, "costo": costo, "precio_detal": precio_detal, "precio_mayor": precio_mayor, "precio_divisas": precio_divisas, "min_mayor": min_mayor}
                            if codigo_barras: datos_nuevos["codigo_barras"] = codigo_barras
                            db.table("inventario").insert(datos_nuevos).execute()
                            st.toast(f"✅ Producto '{nombre}' registrado", icon="✅"); time.sleep(0.5); st.rerun()
        
        with tab3:
            if not df.empty:
                valor_inv = (df['stock'] * df['costo']).sum()
                valor_venta = (df['stock'] * df['precio_detal']).sum()
                bajo_stock = len(df[df['stock'] < 5])
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                col_m1.metric("Total productos", len(df))
                col_m2.metric("Valor inventario (costo)", formatear_usd(valor_inv))
                col_m3.metric("Valor venta potencial", formatear_usd(valor_venta))
                col_m4.metric("Stock bajo", bajo_stock, delta_color="inverse")
                ganancia_potencial = valor_venta - valor_inv
                st.metric("💰 Ganancia potencial total", formatear_usd(ganancia_potencial), delta=f"{(ganancia_potencial/valor_inv*100):.1f}%" if valor_inv else "")
                st.subheader("📊 Productos por categoría")
                if 'categoria' in df.columns:
                    cat_stats = df.groupby('categoria').agg({'nombre': 'count', 'stock': 'sum', 'costo': lambda x: (x * df.loc[x.index, 'stock']).sum()}).round(2)
                    cat_stats.columns = ['Cantidad', 'Stock total', 'Valor total $']
                    st.dataframe(cat_stats, use_container_width=True)
                st.subheader("⚠️ Productos con stock bajo (<5)")
                df_bajo = df[df['stock'] < 5][['nombre', 'categoria', 'stock', 'costo']]
                if not df_bajo.empty:
                    df_bajo.columns = ['Producto', 'Categoría', 'Stock', 'Costo unitario']
                    st.dataframe(df_bajo, use_container_width=True, hide_index=True)
                else: st.success("No hay productos con stock bajo")
            else: st.info("No hay datos")
        
        with tab4:
            st.subheader("📥 Respaldo de inventario")
            if not df.empty:
                col_r1, col_r2 = st.columns(2)
                with col_r1:
                    if st.button("📥 Exportar inventario completo", use_container_width=True):
                        export_df = df[['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'precio_divisas', 'min_mayor']].copy()
                        export_df.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Detal $', 'Mayor $', 'Oferta Divisas $', 'Mín. Mayor']
                        st.markdown(exportar_excel(export_df.sort_values('Producto'), f"inventario_completo_{datetime.now().strftime('%Y%m%d_%H%M')}"), unsafe_allow_html=True)
                with col_r2:
                    if st.button("📥 Exportar lista de precios", use_container_width=True):
                        precio_df = df[['nombre', 'categoria', 'precio_detal', 'precio_mayor', 'precio_divisas', 'min_mayor']].copy()
                        precio_df.columns = ['Producto', 'Categoría', 'Detal $', 'Mayor $', 'Oferta Divisas $', 'Mín. Mayor']
                        st.markdown(exportar_excel(precio_df.sort_values('Categoría'), f"lista_precios_{datetime.now().strftime('%Y%m%d')}"), unsafe_allow_html=True)
            else: st.info("No hay productos para respaldar")
    except Exception as e:
        st.error(f"Error en inventario: {e}")
        st.exception(e)

# ============================================
# MÓDULO 2: PUNTO DE VENTA (CON OFERTA DIVISAS Y CASHEA)
# ============================================
elif opcion == "🛒 PUNTO DE VENTA":
    requiere_turno(); requiere_usuario()
    
    id_turno = st.session_state.id_turno
    tasa = st.session_state.tasa_dia
    
    try:
        turno_info = db.table("cierres").select("tasa_divisas").eq("id", id_turno).execute()
        tasa_divisas = float(turno_info.data[0].get('tasa_divisas', tasa)) if turno_info.data else tasa
    except: tasa_divisas = tasa
    st.session_state.tasa_divisas = tasa_divisas
    
    st.markdown("<h1 class='main-header'>🛒 Punto de Venta</h1>", unsafe_allow_html=True)
    st.markdown(f"""
        <div style='background-color: #e7f3ff; padding: 0.5rem; border-radius: 8px; margin-bottom: 0.5rem; font-size:0.9rem;'>
            <span style='font-weight:600;'>📍 Turno #{id_turno}</span> | 
            <span>💱 BCV: {tasa:.2f} Bs/$</span> |
            <span>💱 Divisas: {tasa_divisas:.2f} Bs/$</span> |
            <span>👤 {st.session_state.usuario_actual['nombre']}</span>
        </div>
    """, unsafe_allow_html=True)
    
    if 'clientes' not in st.session_state:
        st.session_state.clientes = {
            'cliente_1': {'nombre': 'Cliente 1', 'carrito': [], 'cliente': ''},
            'cliente_2': {'nombre': 'Cliente 2', 'carrito': [], 'cliente': ''},
            'cliente_3': {'nombre': 'Cliente 3', 'carrito': [], 'cliente': ''},
            'cliente_4': {'nombre': 'Cliente 4', 'carrito': [], 'cliente': ''}
        }
    if 'cliente_actual' not in st.session_state: st.session_state.cliente_actual = 'cliente_1'
    if 'carrito_version' not in st.session_state: st.session_state.carrito_version = 0
    
    st.subheader("👥 Seleccionar Cliente / Cuenta")
    col_clientes = st.columns(4)
    for idx, (cliente_id, cliente_data) in enumerate(st.session_state.clientes.items()):
        with col_clientes[idx]:
            if st.button(f"{cliente_data['nombre']} ({len(cliente_data['carrito'])})", key=f"cliente_{cliente_id}", use_container_width=True, type="primary" if cliente_id == st.session_state.cliente_actual else "secondary"):
                st.session_state.cliente_actual = cliente_id; st.rerun()
    
    cliente_actual = st.session_state.clientes[st.session_state.cliente_actual]
    st.divider()
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1: st.markdown(f"**Cliente:** {cliente_actual['nombre']}")
    with col2:
        cliente = st.text_input("Nombre (opcional)", value=cliente_actual.get('cliente', ''), key="cliente_nombre", placeholder="Ej: Juan Pérez", label_visibility="collapsed")
        if cliente != cliente_actual.get('cliente', ''): st.session_state.clientes[st.session_state.cliente_actual]['cliente'] = cliente
    with col3:
        if len(cliente_actual['carrito']) > 0:
            if st.button("🧹 Limpiar", use_container_width=True):
                st.session_state.clientes[st.session_state.cliente_actual]['carrito'] = []; st.session_state.carrito_version += 1; st.rerun()
    
    @st.cache_data(ttl=60, show_spinner=False)
    def cargar_inventario():
        try: return db.table("inventario").select("*").execute().data or []
        except Exception as e:
            st.error(f"Error: {e}"); return []
    
    inventario = cargar_inventario()
    if not inventario: st.warning("No se pudo cargar el inventario."); st.stop()
    
    def agregar_producto(prod, es_oferta_divisas):
        carrito = st.session_state.clientes[st.session_state.cliente_actual]['carrito']
        if es_oferta_divisas and float(prod.get('precio_divisas', 0)) > 0:
            precio_base = float(prod['precio_divisas']); tipo_precio = " (Oferta Divisas)"
        else:
            precio_base = float(prod['precio_detal']); tipo_precio = ""
        
        encontrado = False
        for item in carrito:
            if item['id'] == prod['id']:
                nueva_cant = item['cantidad'] + 1
                if not es_oferta_divisas and nueva_cant >= prod['min_mayor']:
                    nuevo_precio = float(prod['precio_mayor']); tipo_precio = " (Mayor)"
                else: nuevo_precio = precio_base
                item['cantidad'] = nueva_cant; item['precio'] = nuevo_precio
                item['subtotal'] = nueva_cant * nuevo_precio; item['tipo_precio'] = tipo_precio
                encontrado = True; break
        if not encontrado:
            carrito.append({"id": prod['id'], "nombre": prod['nombre'], "cantidad": 1, "precio": precio_base, "costo": float(prod['costo']), "subtotal": precio_base, "tipo_precio": tipo_precio})
        st.session_state.carrito_version += 1; st.rerun()
    
    es_oferta_divisas = st.checkbox("💲 Oferta en divisas", help="Usa el precio especial en USD para pagos en divisas. El total se calcula a tasa divisas sin redondeo.")
    
    st.markdown("""
        <style>
        .stForm > div:first-child > div:last-child { display: none; }
        .stTextInput > div > div > input { border-radius: 30px; padding: 0.75rem 1rem; border: 1px solid #ccc; font-size: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    with st.form(key="codigo_form", clear_on_submit=True):
        codigo_input = st.text_input("🔖 Escanear código de barras", placeholder="Escanea o escribe el código...", label_visibility="collapsed")
        submitted = st.form_submit_button("")
    
    if submitted and codigo_input.strip():
        codigo = codigo_input.strip(); producto = None
        for p in inventario:
            if p.get('codigo_barras') == codigo and p['stock'] > 0:
                producto = p; break
        if not producto:
            try:
                resp_alt = db.table("codigos_alternos").select("producto_id").eq("codigo", codigo).execute()
                if resp_alt.data:
                    pid = resp_alt.data[0]['producto_id']
                    for p in inventario:
                        if p['id'] == pid and p['stock'] > 0: producto = p; break
            except: pass
        if producto: agregar_producto(producto, es_oferta_divisas)
        else: st.warning(f"Código '{codigo}' no encontrado o sin stock.")
    
    with st.popover("🔍 Buscar por nombre", use_container_width=True):
        busqueda = st.text_input("", placeholder="Escribe nombre del producto...", key="buscar_nombre_popover")
        if busqueda:
            resultados = [p for p in inventario if p['stock'] > 0 and busqueda.lower() in p['nombre'].lower()][:30]
            if resultados:
                st.markdown("---")
                cols_h = st.columns([3, 1, 1, 1, 0.5])
                cols_h[0].markdown("**Producto**"); cols_h[1].markdown("**Stock**"); cols_h[2].markdown("**Precio USD**"); cols_h[3].markdown("**Precio Bs**")
                st.markdown("---")
                for prod in resultados:
                    if es_oferta_divisas and float(prod.get('precio_divisas', 0)) > 0:
                        precio_usd = float(prod['precio_divisas']); etiqueta = " (Oferta)"
                    else:
                        precio_usd = float(prod['precio_detal']); etiqueta = ""
                    precio_bs = precio_usd * tasa
                    c1, c2, c3, c4, c5 = st.columns([3, 1, 1, 1, 0.5])
                    c1.write(prod['nombre']); c2.write(f"{prod['stock']:.0f}"); c3.write(f"${precio_usd:.2f}{etiqueta}"); c4.write(f"{precio_bs:,.2f} Bs")
                    if c5.button("➕", key=f"pop_{prod['id']}"):
                        agregar_producto(prod, es_oferta_divisas); st.rerun()
            else: st.info("No se encontraron productos.")
        else: st.info("Escribe el nombre para buscar")
    
    st.subheader(f"🛒 Carrito - {cliente_actual['nombre']}")
    carrito = cliente_actual['carrito']
    
    if not carrito:
        st.info("Carrito vacío")
    else:
        st.markdown('<style>.carrito-scroll { max-height: 450px; overflow-y: auto; margin-bottom: 1rem; border: 1px solid #ddd; border-radius: 8px; padding: 0.5rem; }</style>', unsafe_allow_html=True)
        st.markdown('<div class="carrito-scroll">', unsafe_allow_html=True)
        cols_h = st.columns([3, 1, 1.2, 1, 0.5])
        cols_h[0].write("**Producto**"); cols_h[1].write("**Precio USD**"); cols_h[2].write("**Precio Bs**"); cols_h[3].write("**Cantidad**"); cols_h[4].write("**Eliminar**")
        st.markdown("---")
        
        total_venta_usd = 0.0; total_costo = 0.0
        for item in carrito:
            cols = st.columns([3, 1, 1.2, 1, 0.5])
            cols[0].write(item['nombre']); cols[1].write(f"${item['precio']:.2f}"); cols[2].write(f"{item['precio'] * tasa:,.2f} Bs")
            key_cant = f"cant_{item['id']}_v{st.session_state.carrito_version}"
            nueva_cant = cols[3].number_input("", min_value=0.0, max_value=1000.0, value=float(item['cantidad']), step=1.0, key=key_cant, label_visibility="collapsed")
            if nueva_cant != item['cantidad']:
                if nueva_cant == 0:
                    st.session_state.clientes[st.session_state.cliente_actual]['carrito'].remove(item); st.session_state.carrito_version += 1; st.rerun()
                else:
                    prod_data = next((p for p in inventario if p['id'] == item['id']), None)
                    if prod_data:
                        if es_oferta_divisas and float(prod_data.get('precio_divisas', 0)) > 0:
                            item['precio'] = float(prod_data['precio_divisas'])
                        elif nueva_cant >= prod_data['min_mayor']: item['precio'] = float(prod_data['precio_mayor'])
                        else: item['precio'] = float(prod_data['precio_detal'])
                    item['cantidad'] = nueva_cant; item['subtotal'] = nueva_cant * item['precio']; st.session_state.carrito_version += 1; st.rerun()
            if cols[4].button("❌", key=f"del_{item['id']}_v{st.session_state.carrito_version}"):
                st.session_state.clientes[st.session_state.cliente_actual]['carrito'].remove(item); st.session_state.carrito_version += 1; st.rerun()
            total_venta_usd += item['subtotal']; total_costo += item['cantidad'] * item['costo']
        st.markdown('</div>', unsafe_allow_html=True)
        
        total_venta_bs = total_venta_usd * tasa
        st.divider()
        
        if es_oferta_divisas:
            total_minimo_bs = total_venta_usd * tasa_divisas
            st.markdown(f"### 💰 TOTAL A COBRAR (OFERTA DIVISAS): {total_minimo_bs:,.2f} Bs")
            st.caption(f"Equivalente: ${total_venta_usd:.2f} USD a tasa divisas {tasa_divisas:.2f}")
            monto_minimo_bs = total_minimo_bs
        else:
            total_redondeado_bs = math.ceil(total_venta_bs / 10) * 10
            st.markdown(f"### 💰 TOTAL A COBRAR: {total_redondeado_bs:,.2f} Bs")
            if total_redondeado_bs != total_venta_bs:
                st.caption(f"Redondeado al alza (original {total_venta_bs:,.2f})")
            monto_minimo_bs = total_redondeado_bs
        
        st.divider()
        
        with st.expander("💳 Detalle de pagos", expanded=True):
            st.markdown(f"**💰 MONTO MÍNIMO A COBRAR:** {monto_minimo_bs:,.2f} Bs")
            col_p1, col_p2 = st.columns(2)
            with col_p1:
                st.markdown("**💵 Pagos en Bs**")
                p_bs_ef = st.number_input("Efectivo Bs", min_value=0.0, step=1.0, format="%.2f", key="p_ef_bs")
                p_movil = st.number_input("Pago Móvil Bs", min_value=0.0, step=1.0, format="%.2f", key="p_movil")
                p_punto = st.number_input("Punto Bs", min_value=0.0, step=1.0, format="%.2f", key="p_punto")
                total_bs_recibido = p_bs_ef + p_movil + p_punto
            with col_p2:
                st.markdown("**💵 Pagos en USD**")
                st.caption(f"Se convierten a Bs a tasa divisas ({tasa_divisas:.2f})")
                p_usd_ef = st.number_input("Efectivo USD", min_value=0.0, step=1.0, format="%.2f", key="p_ef_usd")
                p_zelle = st.number_input("Zelle USD", min_value=0.0, step=1.0, format="%.2f", key="p_zelle")
                p_otros_usd = st.number_input("Otros USD", min_value=0.0, step=1.0, format="%.2f", key="p_otros_usd")
                p_cashea = st.number_input("💳 Cashea (USD a BCV)", min_value=0.0, step=1.0, format="%.2f", key="p_cashea", help="Monto financiado por Cashea a tasa BCV")
                total_usd_recibido = p_usd_ef + p_zelle + p_otros_usd
                total_bs_por_usd = total_usd_recibido * tasa_divisas
                total_bs_cashea = p_cashea * tasa
            
            total_pagado_bs = total_bs_recibido + total_bs_por_usd + total_bs_cashea
            st.markdown("---")
            col_r1, col_r2 = st.columns(2)
            col_r1.metric("Pagado en Bs", f"{total_pagado_bs:,.2f} Bs")
            col_r2.metric("Mínimo requerido", f"{monto_minimo_bs:,.2f} Bs")
            
            if total_pagado_bs >= monto_minimo_bs - 0.01:
                venta_valida = True
                st.success(f"✅ Pago suficiente.")
                if total_pagado_bs > monto_minimo_bs + 0.01:
                    st.info(f"📌 Excedente: {total_pagado_bs - monto_minimo_bs:,.2f} Bs (se sumará al total de la venta)")
            else:
                venta_valida = False
                st.error(f"❌ Faltante: {monto_minimo_bs - total_pagado_bs:,.2f} Bs")
        
        col_b1, col_b2, col_b3 = st.columns(3)
        with col_b1:
            if st.button("🔄 Limpiar carrito", use_container_width=True):
                st.session_state.clientes[st.session_state.cliente_actual]['carrito'] = []; st.session_state.carrito_version += 1; st.rerun()
        with col_b2:
            if st.button("✅ Cobrar y cerrar cuenta", type="primary", use_container_width=True, disabled=not (venta_valida and carrito)):
                try:
                    items_res = []
                    for item in carrito:
                        stock_actual = db.table("inventario").select("stock").eq("id", item['id']).execute().data[0]['stock']
                        db.table("inventario").update({"stock": stock_actual - item['cantidad']}).eq("id", item['id']).execute()
                        items_res.append(f"{item['cantidad']:.0f}x {item['nombre']}")
                    
                    info_cli = f" - Cliente: {cliente_actual.get('cliente', '')}" if cliente_actual.get('cliente') else ""
                    monto_cobrado_bs = total_pagado_bs
                    total_final_usd = monto_cobrado_bs / tasa_divisas if es_oferta_divisas else monto_cobrado_bs / tasa
                    costo_venta_bs = total_costo * tasa
                    
                    venta = {
                        "id_cierre": id_turno, "producto": ", ".join(items_res), "cantidad": len(carrito),
                        "total_usd": round(total_final_usd, 2), "monto_cobrado_bs": round(monto_cobrado_bs, 2),
                        "tasa_cambio": tasa, "pago_divisas": round(p_usd_ef, 2), "pago_zelle": round(p_zelle, 2),
                        "pago_otros": round(p_otros_usd, 2), "pago_efectivo": round(p_bs_ef, 2),
                        "pago_movil": round(p_movil, 2), "pago_punto": round(p_punto, 2),
                        "pago_cashea": round(p_cashea, 2), "costo_venta": round(total_costo, 2),
                        "costo_venta_bs": round(costo_venta_bs, 2), "estado": "Finalizado",
                        "items": json.dumps(carrito), "id_transaccion": str(int(datetime.now().timestamp())),
                        "fecha": datetime.now().isoformat(),
                        "cliente": cliente_actual.get('cliente', '') or f"{cliente_actual['nombre']}"
                    }
                    db.table("ventas").insert(venta).execute()
                    st.balloons()
                    st.success(f"✅ Venta registrada - {cliente_actual['nombre']}{info_cli}")
                    
                    @st.dialog("🧾 Ticket de Venta")
                    def ticket():
                        st.markdown("### BODEGÓN VYM")
                        st.write(f"**Fecha:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                        st.write(f"**Turno:** #{id_turno} | **Cliente:** {cliente_actual['nombre']}{info_cli}")
                        st.write(f"**Cajero:** {st.session_state.usuario_actual['nombre']}")
                        st.divider()
                        df_t = pd.DataFrame([{"Cant": f"{i['cantidad']:.0f}", "Producto": i['nombre'], "Precio USD": f"${i['precio']:.2f}", "Subtotal USD": f"${i['subtotal']:.2f}", "Subtotal Bs": f"{i['subtotal'] * tasa:,.2f}"} for i in carrito])
                        st.dataframe(df_t, use_container_width=True, hide_index=True)
                        st.divider()
                        col1, col2 = st.columns(2)
                        col1.metric("Total USD", f"${total_final_usd:,.2f}"); col2.metric("Total Bs", f"{monto_cobrado_bs:,.2f} Bs")
                        if p_cashea > 0: st.info(f"💳 Incluye Cashea: ${p_cashea:.2f}")
                        if st.button("Cerrar ticket", use_container_width=True): st.rerun()
                    ticket()
                    st.session_state.clientes[st.session_state.cliente_actual]['carrito'] = []; st.session_state.clientes[st.session_state.cliente_actual]['cliente'] = ''; st.session_state.carrito_version += 1; st.rerun()
                except Exception as e: st.error(f"Error: {e}")
        with col_b3:
            if carrito and st.button("⏸️ Dejar pendiente", use_container_width=True):
                st.session_state.cliente_actual = 'cliente_1'; st.rerun()

# ============================================
# MÓDULO 3: GASTOS
# ============================================
elif opcion == "💸 GASTOS":
    requiere_turno(); requiere_usuario()
    id_turno = st.session_state.id_turno
    st.markdown("<h1 class='main-header'>💸 Gestión de Gastos</h1>", unsafe_allow_html=True)
    try:
        response = db.table("gastos").select("*").eq("id_cierre", id_turno).order("fecha", desc=True).execute()
        df_gastos = pd.DataFrame(response.data) if response.data else pd.DataFrame()
        if not df_gastos.empty:
            st.subheader("📋 Gastos del turno")
            if 'fecha' in df_gastos.columns: df_gastos['fecha'] = pd.to_datetime(df_gastos['fecha']).dt.strftime('%d/%m/%Y %H:%M')
            columnas_mostrar = ['fecha', 'descripcion', 'monto_usd']
            if 'categoria' in df_gastos.columns: columnas_mostrar.append('categoria')
            st.dataframe(df_gastos[columnas_mostrar], use_container_width=True, hide_index=True)
            st.metric("💰 Total gastos USD", f"${df_gastos['monto_usd'].sum():,.2f}")
        else: st.info("No hay gastos registrados")
    except Exception as e: st.error(f"Error: {e}")
    
    st.divider()
    with st.form("nuevo_gasto"):
        st.subheader("➕ Registrar gasto")
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            descripcion = st.text_input("Descripción *", placeholder="Ej: Insumos...")
            monto_usd = st.number_input("Monto USD *", min_value=0.01, step=0.01, format="%.2f")
        with col_g2:
            categoria = st.selectbox("Categoría", ["", "Servicios", "Insumos", "Personal", "Alimentación", "Otros"])
            monto_bs_extra = st.number_input("Monto extra Bs (opcional)", min_value=0.0, step=10.0, format="%.2f")
        if st.form_submit_button("✅ Registrar", use_container_width=True):
            if descripcion and monto_usd > 0:
                gasto_data = {"id_cierre": id_turno, "descripcion": descripcion, "monto_usd": monto_usd, "estado": "activo", "fecha": datetime.now().isoformat()}
                if categoria: gasto_data["categoria"] = categoria
                if monto_bs_extra > 0: gasto_data["monto_bs_extra"] = monto_bs_extra
                db.table("gastos").insert(gasto_data).execute()
                st.toast("✅ Gasto registrado", icon="✅"); time.sleep(0.5); st.rerun()
            else: st.warning("Complete los campos (*)")

# ============================================
# MÓDULO 4: HISTORIAL
# ============================================
elif opcion == "📜 HISTORIAL":
    requiere_usuario()
    st.markdown("<h1 class='main-header'>📜 Historial de Ventas</h1>", unsafe_allow_html=True)
    st.markdown(f"<div style='background-color: #e7f3ff; padding: 0.8rem; border-radius: 8px; margin-bottom: 1.5rem;'><span style='font-weight:600;'>👤 {st.session_state.usuario_actual['nombre']}</span></div>", unsafe_allow_html=True)
    
    if 'historial_filtros' not in st.session_state:
        st.session_state.historial_filtros = {'fecha_desde': None, 'fecha_hasta': None, 'turno': 0, 'estado': "Todos", 'buscar': ""}
    if 'historial_offset' not in st.session_state: st.session_state.historial_offset = 0
    LIMITE = 100
    
    with st.form(key="filtros_historial"):
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            fecha_desde = st.date_input("📅 Desde", value=st.session_state.historial_filtros['fecha_desde'], key="hist_desde")
            turno_filtro = st.number_input("🔢 Turno", min_value=0, value=st.session_state.historial_filtros['turno'], step=1, key="filtro_turno")
        with col_f2:
            fecha_hasta = st.date_input("📅 Hasta", value=st.session_state.historial_filtros['fecha_hasta'], key="hist_hasta")
            estado_filtro = st.selectbox("Estado", ["Todos", "Finalizado", "Anulado"], index=["Todos", "Finalizado", "Anulado"].index(st.session_state.historial_filtros['estado']), key="filtro_estado")
        buscar_texto = st.text_input("🔍 Buscar producto", value=st.session_state.historial_filtros['buscar'], placeholder="Ej: Ron...", key="filtro_buscar")
        submitted = st.form_submit_button("🔍 Buscar", use_container_width=True)
    
    if submitted:
        st.session_state.historial_filtros = {'fecha_desde': fecha_desde, 'fecha_hasta': fecha_hasta, 'turno': turno_filtro, 'estado': estado_filtro, 'buscar': buscar_texto}
        st.session_state.historial_offset = 0; st.rerun()
    
    filtros = st.session_state.historial_filtros
    columnas = "id, id_cierre, fecha, producto, total_usd, monto_cobrado_bs, estado, items, pago_divisas, pago_zelle, pago_otros, pago_efectivo, pago_movil, pago_punto, pago_cashea, costo_venta, cliente"
    query = db.table("ventas").select(columnas).order("fecha", desc=True)
    if filtros['fecha_desde']: query = query.gte("fecha", filtros['fecha_desde'].strftime("%Y-%m-%d"))
    if filtros['fecha_hasta']: query = query.lte("fecha", filtros['fecha_hasta'].strftime("%Y-%m-%d"))
    if filtros['turno'] > 0: query = query.eq("id_cierre", filtros['turno'])
    if filtros['estado'] != "Todos": query = query.eq("estado", filtros['estado'])
    if filtros['buscar']: query = query.ilike("producto", f"%{filtros['buscar']}%")
    query = query.range(st.session_state.historial_offset, st.session_state.historial_offset + LIMITE - 1)
    
    try:
        response = query.execute()
        df = pd.DataFrame(response.data) if response.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Error: {e}"); df = pd.DataFrame()
    
    if not df.empty:
        df['fecha_dt'] = pd.to_datetime(df['fecha'])
        df['hora'] = df['fecha_dt'].dt.strftime('%H:%M')
        
        def resumen_pago(venta):
            metodos = []
            if venta.get('pago_efectivo', 0) > 0: metodos.append(f"Ef.Bs {venta['pago_efectivo']:,.0f}")
            if venta.get('pago_movil', 0) > 0: metodos.append(f"Móvil {venta['pago_movil']:,.0f}")
            if venta.get('pago_punto', 0) > 0: metodos.append(f"Punto {venta['pago_punto']:,.0f}")
            if venta.get('pago_divisas', 0) > 0: metodos.append(f"Ef.USD ${venta['pago_divisas']:.2f}")
            if venta.get('pago_zelle', 0) > 0: metodos.append(f"Zelle ${venta['pago_zelle']:.2f}")
            if venta.get('pago_otros', 0) > 0: metodos.append(f"Otros ${venta['pago_otros']:.2f}")
            if venta.get('pago_cashea', 0) > 0: metodos.append(f"Cashea ${venta['pago_cashea']:.2f}")
            return ", ".join(metodos) if metodos else "No registrado"
        
        df['tipo_pago'] = df.apply(resumen_pago, axis=1)
        df_activas = df[df['estado'] != 'Anulado']
        total_usd = df_activas['total_usd'].sum() if not df_activas.empty else 0
        total_bs = df_activas['monto_cobrado_bs'].sum() if not df_activas.empty else 0
        
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1: st.markdown(f"<div style='background: linear-gradient(135deg,#667eea,#764ba2);padding:1rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:0.9rem;'>💰 TOTAL USD</span><br><span style='font-size:1.8rem;font-weight:700;'>${total_usd:,.2f}</span></div>", unsafe_allow_html=True)
        with col_m2: st.markdown(f"<div style='background: linear-gradient(135deg,#f093fb,#f5576c);padding:1rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:0.9rem;'>💵 TOTAL BS</span><br><span style='font-size:1.8rem;font-weight:700;'>{total_bs:,.0f}</span></div>", unsafe_allow_html=True)
        with col_m3: st.markdown(f"<div style='background: linear-gradient(135deg,#4facfe,#00f2fe);padding:1rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:0.9rem;'>📊 VENTAS</span><br><span style='font-size:1.8rem;font-weight:700;'>{len(df_activas)}</span></div>", unsafe_allow_html=True)
        with col_m4: st.markdown(f"<div style='background: linear-gradient(135deg,#5f2c82,#49a09d);padding:1rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:0.9rem;'>📈 PROMEDIO</span><br><span style='font-size:1.8rem;font-weight:700;'>${(total_usd/len(df_activas) if len(df_activas)>0 else 0):,.2f}</span></div>", unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        cols_h = st.columns([0.6, 0.8, 0.8, 2.2, 1.0, 1.0, 1.8, 0.8, 0.8, 0.8])
        for h, t in zip(cols_h, ["**Turno**","**ID**","**Hora**","**Productos**","**USD**","**Bs**","**Tipo pago**","**Estado**","**Anular**","**Factura**"]): h.markdown(t)
        st.markdown("---")
        
        for idx, venta in df.iterrows():
            es_anulado = venta['estado'] == 'Anulado'
            badge = '<span style="background:#dc3545;color:white;padding:0.2rem 0.6rem;border-radius:12px;font-size:0.7rem;font-weight:600;">ANULADA</span>' if es_anulado else '<span style="background:#28a745;color:white;padding:0.2rem 0.6rem;border-radius:12px;font-size:0.7rem;font-weight:600;">FINALIZADA</span>'
            productos = venta['producto'][:35] + "..." if len(venta['producto']) > 35 else venta['producto']
            cols = st.columns([0.6, 0.8, 0.8, 2.2, 1.0, 1.0, 1.8, 0.8, 0.8, 0.8])
            with cols[0]: st.markdown(f"#{venta['id_cierre']}")
            with cols[1]: st.markdown(f"#{venta['id']}")
            with cols[2]: st.markdown(venta['hora'])
            with cols[3]: st.markdown(f"<span title='{venta['producto']}'>{productos}</span>", unsafe_allow_html=True)
            with cols[4]: st.markdown(f"**${venta['total_usd']:,.2f}**")
            with cols[5]: st.markdown(f"{venta['monto_cobrado_bs']:,.0f}")
            with cols[6]: st.markdown(f"<span style='font-size:0.8rem;'>{venta['tipo_pago']}</span>", unsafe_allow_html=True)
            with cols[7]: st.markdown(badge, unsafe_allow_html=True)
            with cols[8]:
                if not es_anulado and es_admin():
                    if st.button("🚫", key=f"anular_{venta['id']}"):
                        try:
                            items = venta.get('items')
                            if isinstance(items, str): items = json.loads(items)
                            if items:
                                for item in items:
                                    stock_res = db.table("inventario").select("stock").eq("id", item['id']).execute()
                                    if stock_res.data:
                                        db.table("inventario").update({"stock": stock_res.data[0]['stock'] + item['cantidad']}).eq("id", item['id']).execute()
                            db.table("ventas").update({"estado": "Anulado"}).eq("id", venta['id']).execute()
                            st.toast("✅ Anulada", icon="✅"); time.sleep(0.5); st.rerun()
                        except Exception as e: st.error(f"Error: {e}")
                elif es_anulado: st.markdown("—")
                else: st.markdown("🔒")
            with cols[9]:
                with st.popover("👁️"):
                    st.markdown("### 🧾 FACTURA")
                    st.markdown(f"**Turno:** #{venta['id_cierre']} | **Fecha:** {venta['fecha_dt'].strftime('%d/%m/%Y %H:%M')}")
                    st.markdown(f"**Cliente:** {venta.get('cliente', 'General')}")
                    st.divider()
                    items = venta.get('items')
                    if isinstance(items, str): items = json.loads(items)
                    if items:
                        df_t = pd.DataFrame([{"Cant": i.get('cantidad', 0), "Producto": i.get('nombre', ''), "Precio": f"${i.get('precio', 0):.2f}", "Subtotal": f"${i.get('subtotal', 0):.2f}"} for i in items])
                        st.dataframe(df_t, use_container_width=True, hide_index=True)
                    st.divider()
                    st.markdown(f"**Total USD:** ${venta['total_usd']:.2f}")
                    st.markdown(f"**Total Bs:** {venta['monto_cobrado_bs']:.2f} Bs")
                    if venta.get('pago_cashea', 0) > 0: st.markdown(f"**Cashea:** ${venta['pago_cashea']:.2f}")
            if idx < len(df) - 1: st.markdown("<hr style='margin:0.2rem 0; opacity:0.3;'>", unsafe_allow_html=True)
        
        col_p1, col_p2, col_p3 = st.columns([1, 1, 1])
        with col_p1:
            if st.session_state.historial_offset > 0:
                if st.button("⬅️ Anteriores", use_container_width=True):
                    st.session_state.historial_offset -= LIMITE; st.rerun()
        with col_p2: st.caption(f"Página {st.session_state.historial_offset//LIMITE + 1}")
        with col_p3:
            if len(df) == LIMITE:
                if st.button("Siguientes ➡️", use_container_width=True):
                    st.session_state.historial_offset += LIMITE; st.rerun()
    else: st.info("📭 No hay ventas que coincidan.")

# ============================================
# MÓDULO 5: CIERRE DE CAJA (CON DOS TASAS Y CASHEA)
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
                    tasa_divisas = st.number_input("💱 Tasa divisas (mercado)", min_value=1.0, value=70.0, step=0.5, format="%.2f")
                    st.info(f"👤 Abre: {st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo'}")
                if st.form_submit_button("🚀 ABRIR CAJA", type="primary", use_container_width=True):
                    try:
                        data = {"tasa_apertura": tasa_apertura, "tasa_divisas": tasa_divisas, "fondo_bs": fondo_bs, "fondo_usd": fondo_usd, "monto_apertura": fondo_usd, "estado": "abierto", "fecha_apertura": datetime.now().isoformat(), "usuario_apertura": st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo'}
                        res = db.table("cierres").insert(data).execute()
                        if res.data:
                            st.session_state.id_turno = res.data[0]['id']; st.session_state.tasa_dia = tasa_apertura
                            st.session_state.tasa_divisas = tasa_divisas; st.session_state.fondo_bs = fondo_bs; st.session_state.fondo_usd = fondo_usd
                            st.toast(f"✅ Turno #{res.data[0]['id']} abierto", icon="✅"); time.sleep(0.5); st.rerun()
                    except Exception as e: st.error(f"Error: {e}")
            st.stop()
        
        id_turno = st.session_state.id_turno
        try:
            turno_info = db.table("cierres").select("tasa_divisas").eq("id", id_turno).execute()
            tasa_divisas = turno_info.data[0].get('tasa_divisas', st.session_state.tasa_dia) if turno_info.data else st.session_state.tasa_dia
        except: tasa_divisas = st.session_state.tasa_dia
        st.session_state.tasa_divisas = tasa_divisas
        tasa = st.session_state.tasa_dia
        
        turno_info = db.table("cierres").select("*").eq("id", id_turno).execute()
        usuario_apertura = turno_info.data[0].get('usuario_apertura', 'N/A') if turno_info.data else 'N/A'
        
        col_info1, col_info2, col_info3, col_info4 = st.columns(4)
        col_info1.success(f"📍 Turno #{id_turno}"); col_info2.info(f"👤 {usuario_apertura}")
        col_info3.info(f"💱 BCV: {tasa:.2f}"); col_info4.info(f"💱 Divisas: {tasa_divisas:.2f}")
        
        ventas = db.table("ventas").select("*").eq("id_cierre", id_turno).eq("estado", "Finalizado").execute().data or []
        gastos = db.table("gastos").select("*").eq("id_cierre", id_turno).execute().data or []
        
        total_pagos_bs_raw = sum(float(v.get('pago_efectivo', 0)) + float(v.get('pago_movil', 0)) + float(v.get('pago_punto', 0)) for v in ventas)
        total_pagos_usd_raw = sum(float(v.get('pago_divisas', 0)) + float(v.get('pago_zelle', 0)) + float(v.get('pago_otros', 0)) for v in ventas)
        total_pagos_cashea_raw = sum(float(v.get('pago_cashea', 0)) for v in ventas)
        total_ingresos_reales_bs = total_pagos_bs_raw + (total_pagos_usd_raw * tasa_divisas) + (total_pagos_cashea_raw * tasa)
        total_costos_usd = sum(float(v.get('costo_venta', 0)) for v in ventas)
        total_gastos_usd = sum(float(g.get('monto_usd', 0)) for g in gastos)
        total_costos_reales_bs = (total_costos_usd + total_gastos_usd) * tasa
        ganancia_real_bs = total_ingresos_reales_bs - total_costos_reales_bs
        ganancia_real_usd_paralelo = ganancia_real_bs / tasa_divisas if tasa_divisas > 0 else 0
        total_ventas_usd = sum(float(v.get('total_usd', 0)) for v in ventas)
        reposicion = total_costos_usd
        
        st.subheader("📈 Resumen del turno")
        col_r1, col_r2, col_r3 = st.columns(3)
        col_r1.metric("💰 Ingresos reales", f"{total_ingresos_reales_bs:,.2f} Bs", help="Pagos Bs + (USD × tasa divisas) + (Cashea × BCV)")
        col_r2.metric("📦 Costos reales", f"{total_costos_reales_bs:,.2f} Bs", help="(Costo mercancía + Gastos) × BCV")
        col_r3.metric("💸 Gastos USD", f"${total_gastos_usd:,.2f}")
        
        st.markdown("---")
        col_g1, col_g2, col_g3 = st.columns(3)
        with col_g1: st.markdown(f"<div style='background:linear-gradient(135deg,#28a745,#20c997);padding:1.2rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:1rem;'>💰 GANANCIA REAL</span><br><span style='font-size:2rem;font-weight:700;'>{ganancia_real_bs:,.2f} Bs</span></div>", unsafe_allow_html=True)
        with col_g2: st.markdown(f"<div style='background:linear-gradient(135deg,#4facfe,#00f2fe);padding:1.2rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:1rem;'>💵 EQUIVALENTE USD</span><br><span style='font-size:2rem;font-weight:700;'>${ganancia_real_usd_paralelo:,.2f}</span><br><span style='font-size:0.75rem;'>a tasa divisas {tasa_divisas:.2f}</span></div>", unsafe_allow_html=True)
        with col_g3: st.markdown(f"<div style='background:linear-gradient(135deg,#ffc107,#fd7e14);padding:1.2rem;border-radius:10px;color:white;text-align:center;'><span style='font-size:1rem;'>📊 VENTAS</span><br><span style='font-size:2rem;font-weight:700;'>${total_ventas_usd:,.2f}</span></div>", unsafe_allow_html=True)
        
        st.markdown(f"<div style='background:#f0f2f6;padding:1rem;border-radius:8px;margin-top:1rem;display:flex;justify-content:space-between;'><span style='font-weight:600;'>📦 Para reponer mercancía:</span><span style='color:#dc3545;font-weight:600;'>${reposicion:,.2f} USD</span></div><div style='background:#e2e3e5;padding:1rem;border-radius:8px;margin-top:0.5rem;display:flex;justify-content:space-between;'><span style='font-weight:600;'>💳 Por cobrar a Cashea:</span><span style='color:#0d6efd;font-weight:600;'>${total_pagos_cashea_raw:,.2f} USD</span></div>", unsafe_allow_html=True)
        
        with st.expander("💰 Desglose por método de pago", expanded=False):
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.markdown("**💵 Pagos en USD (a tasa divisas)**")
                st.metric("Efectivo USD", f"${sum(float(v.get('pago_divisas', 0)) for v in ventas):,.2f}")
                st.metric("Zelle USD", f"${sum(float(v.get('pago_zelle', 0)) for v in ventas):,.2f}")
                st.metric("Otros USD", f"${sum(float(v.get('pago_otros', 0)) for v in ventas):,.2f}")
                st.metric("Cashea (Por cobrar)", f"${total_pagos_cashea_raw:,.2f}", help=f"Equiv Bs: {total_pagos_cashea_raw * tasa:,.2f}")
            with col_d2:
                st.markdown("**💵 Pagos en Bs**")
                st.metric("Efectivo Bs", f"{sum(float(v.get('pago_efectivo', 0)) for v in ventas):,.2f} Bs")
                st.metric("Pago Móvil Bs", f"{sum(float(v.get('pago_movil', 0)) for v in ventas):,.2f} Bs")
                st.metric("Punto Bs", f"{sum(float(v.get('pago_punto', 0)) for v in ventas):,.2f} Bs")
        
        st.divider()
        st.subheader("🧮 Ingreso de montos físicos")
        with st.form("form_ingreso_montos"):
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                st.markdown("**💰 Bs**")
                efec_bs = st.number_input("Efectivo Bs", min_value=0.0, value=0.0, step=1.0, format="%.2f", key="bs_efectivo")
                pmovil_bs = st.number_input("Pago Móvil Bs", min_value=0.0, value=0.0, step=1.0, format="%.2f", key="bs_pmovil")
                punto_bs = st.number_input("Punto Bs", min_value=0.0, value=0.0, step=1.0, format="%.2f", key="bs_punto")
            with col_f2:
                st.markdown("**💰 USD y Cashea**")
                efec_usd = st.number_input("Efectivo USD", min_value=0.0, value=0.0, step=1.0, format="%.2f", key="usd_efectivo")
                zelle_usd = st.number_input("Zelle USD", min_value=0.0, value=0.0, step=1.0, format="%.2f", key="usd_zelle")
                otros_usd = st.number_input("Otros USD", min_value=0.0, value=0.0, step=1.0, format="%.2f", key="usd_otros")
                cashea_usd = st.number_input("Cashea $ (BCV)", min_value=0.0, value=float(total_pagos_cashea_raw), step=1.0, format="%.2f", key="cashea_fisico")
            observaciones = st.text_area("📝 Observaciones", placeholder="Ej: Todo en orden...")
            if st.form_submit_button("👁️ PREVISUALIZAR CIERRE", use_container_width=True):
                st.session_state.montos_fisicos = {'efec_bs': efec_bs, 'pmovil_bs': pmovil_bs, 'punto_bs': punto_bs, 'efec_usd': efec_usd, 'zelle_usd': zelle_usd, 'otros_usd': otros_usd, 'cashea_usd': cashea_usd, 'observaciones': observaciones}
                st.session_state.montos_calculados = True; st.rerun()
        
        if st.session_state.get('montos_calculados', False):
            montos = st.session_state.montos_fisicos
            total_bs_fisico_original = montos['efec_bs'] + montos['pmovil_bs'] + montos['punto_bs']
            total_usd_fisico_original = montos['efec_usd'] + montos['zelle_usd'] + montos['otros_usd']
            cashea_fisico_usd = montos['cashea_usd']
            total_bs_fisico_convertido = total_bs_fisico_original + (total_usd_fisico_original * tasa_divisas) + (cashea_fisico_usd * tasa)
            esperado_bs = st.session_state.fondo_bs + (st.session_state.fondo_usd * tasa_divisas) + total_pagos_bs_raw + (total_pagos_usd_raw * tasa_divisas) + (total_pagos_cashea_raw * tasa) - (total_gastos_usd * tasa)
            esperado_usd = st.session_state.fondo_usd + total_pagos_usd_raw + total_pagos_cashea_raw - total_gastos_usd
            diff_bs = total_bs_fisico_convertido - esperado_bs
            diff_usd = (total_usd_fisico_original + cashea_fisico_usd) - esperado_usd
            diff_total = diff_usd + (diff_bs / tasa_divisas if tasa_divisas > 0 else 0)
            
            st.subheader("📊 Comparación Caja vs Sistema")
            col_x1, col_x2 = st.columns(2)
            with col_x1:
                st.markdown("**🇻🇪 Bs**"); st.metric("Esperado", f"{esperado_bs:,.2f} Bs"); st.metric("Físico", f"{total_bs_fisico_convertido:,.2f} Bs"); st.metric("Diferencia", f"{diff_bs:+,.2f} Bs")
            with col_x2:
                st.markdown("**🇺🇸 USD**"); st.metric("Esperado", f"${esperado_usd:,.2f}"); st.metric("Físico+Cashea", f"${(total_usd_fisico_original + cashea_fisico_usd):,.2f}"); st.metric("Diferencia", f"${diff_usd:+,.2f}")
            st.metric("DIFERENCIA TOTAL", f"${diff_total:+,.2f}")
            
            if abs(diff_total) < 0.1: st.success("✅ ¡CAJA CUADRADA!")
            elif diff_total > 0: st.warning(f"🟡 SOBRANTE: +${diff_total:,.2f}")
            else: st.error(f"🔴 FALTANTE: -${abs(diff_total):,.2f}")
            
            confirmar = st.checkbox("✅ Confirmo que los datos son correctos")
            if st.button("🔒 CONFIRMAR Y CERRAR TURNO", type="primary", use_container_width=True, disabled=not confirmar):
                try:
                    datos_cierre = {"fecha_cierre": datetime.now().isoformat(), "total_ventas": total_ventas_usd, "total_costos": total_costos_usd, "total_ganancias": ganancia_real_usd_paralelo, "diferencia": diff_total, "tasa_cierre": tasa, "estado": "cerrado", "usuario_cierre": st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo', "observaciones": montos['observaciones'], "fondo_bs_final": total_bs_fisico_convertido, "fondo_usd_final": total_usd_fisico_original + cashea_fisico_usd, "efectivo_bs_fisico": montos['efec_bs'], "pmovil_fisico": montos['pmovil_bs'], "punto_fisico": montos['punto_bs'], "efectivo_usd_fisico": montos['efec_usd'], "zelle_fisico": montos['zelle_usd'], "otros_fisico": montos['otros_usd'], "total_cashea_usd": cashea_fisico_usd}
                    db.table("cierres").update(datos_cierre).eq("id", id_turno).execute()
                    db.table("gastos").update({"estado": "cerrado"}).eq("id_cierre", id_turno).execute()
                    st.session_state.id_turno = None; st.session_state.montos_calculados = False
                    st.balloons(); st.success("✅ Turno cerrado")
                    st.markdown("---"); st.subheader("📄 REPORTE DE CIERRE")
                    st.markdown(f"**Turno:** #{id_turno} | **Abrió:** {usuario_apertura} | **Cerró:** {st.session_state.usuario_actual['nombre'] if st.session_state.usuario_actual else 'Anónimo'}")
                    st.markdown(f"**Ingresos reales:** {total_ingresos_reales_bs:,.2f} Bs")
                    st.markdown(f"**Costos reales:** {total_costos_reales_bs:,.2f} Bs")
                    st.markdown(f"**GANANCIA REAL:** {ganancia_real_bs:,.2f} Bs | ${ganancia_real_usd_paralelo:,.2f}")
                    st.markdown(f"**Cashea por cobrar:** ${cashea_fisico_usd:,.2f}")
                    st.markdown(f"**Diferencia caja:** ${diff_total:+,.2f}")
                    if st.button("🔄 Volver al inicio"): st.rerun()
                except Exception as e: st.error(f"Error: {e}")
            if st.button("✏️ CORREGIR MONTOS", use_container_width=True):
                st.session_state.montos_calculados = False; st.rerun()
    
    with tab_c2:
        st.subheader("📋 Historial de turnos cerrados")
        try:
            cierres = db.table("cierres").select("*").eq("estado", "cerrado").order("fecha_cierre", desc=True).execute()
            df_cierres = pd.DataFrame(cierres.data) if cierres.data else pd.DataFrame()
            if not df_cierres.empty:
                df_cierres['fecha_apertura'] = pd.to_datetime(df_cierres['fecha_apertura']).dt.strftime('%d/%m/%Y %H:%M')
                df_cierres['fecha_cierre'] = pd.to_datetime(df_cierres['fecha_cierre']).dt.strftime('%d/%m/%Y %H:%M')
                cols_mostrar = ['id', 'fecha_apertura', 'fecha_cierre', 'usuario_apertura', 'usuario_cierre', 'total_ventas', 'total_ganancias', 'diferencia']
                if 'total_cashea_usd' in df_cierres.columns: cols_mostrar.insert(-1, 'total_cashea_usd')
                st.dataframe(df_cierres[cols_mostrar], use_container_width=True, hide_index=True)
            else: st.info("No hay turnos cerrados.")
        except Exception as e: st.error(f"Error: {e}")

# ============================================
# MÓDULO 6: ADMINISTRACIÓN
# ============================================
elif opcion == "👥 ADMINISTRACIÓN":
    if not es_admin(): st.error("⛔ Sin permisos"); st.stop()
    st.markdown("<h1 class='main-header'>👥 Administración de Usuarios</h1>", unsafe_allow_html=True)
    
    try: usuarios = listar_usuarios()
    except Exception as e: st.error(f"Error: {e}"); st.stop()
    
    if not usuarios: st.info("No hay usuarios"); st.stop()
    
    st.subheader("📋 Lista de usuarios")
    df_u = pd.DataFrame(usuarios)[['id', 'usuario', 'nombre', 'rol', 'activo']].copy()
    df_u['activo'] = df_u['activo'].apply(lambda x: "✅ Activo" if x else "❌ Inactivo")
    df_u.columns = ['ID', 'Usuario', 'Nombre', 'Rol', 'Estado']
    st.dataframe(df_u, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    st.subheader("✏️ Editar usuario")
    opciones_editar = [u['usuario'] for u in usuarios if u['id'] != st.session_state.usuario_actual['id']]
    if opciones_editar:
        usuario_editar = st.selectbox("Usuario a editar", options=opciones_editar, key="select_editar_usuario")
        if usuario_editar:
            user_data = next((u for u in usuarios if u['usuario'] == usuario_editar), None)
            if user_data:
                with st.container(border=True):
                    col_e1, col_e2 = st.columns(2)
                    with col_e1:
                        nuevo_nombre = st.text_input("Nombre completo", value=user_data['nombre'], key=f"nom_{user_data['id']}")
                        if st.button("Actualizar nombre", key=f"bn_{user_data['id']}"):
                            if nuevo_nombre and nuevo_nombre != user_data['nombre']:
                                if actualizar_usuario(user_data['id'], 'nombre', nuevo_nombre):
                                    st.toast("✅ Nombre actualizado", icon="✅"); time.sleep(0.5); st.rerun()
                        nueva_clave = st.text_input("Nueva clave", type="password", placeholder="Vacío = no cambiar", key=f"cl_{user_data['id']}")
                        if st.button("Cambiar clave", key=f"bc_{user_data['id']}"):
                            if nueva_clave:
                                if actualizar_usuario(user_data['id'], 'clave', nueva_clave):
                                    st.toast("✅ Clave actualizada", icon="✅"); time.sleep(0.5); st.rerun()
                    with col_e2:
                        nuevo_rol = st.selectbox("Rol", ["admin", "empleado"], index=0 if user_data['rol'] == 'admin' else 1, key=f"rl_{user_data['id']}")
                        if st.button("Actualizar rol", key=f"br_{user_data['id']}"):
                            if nuevo_rol != user_data['rol']:
                                if actualizar_usuario(user_data['id'], 'rol', nuevo_rol):
                                    st.toast("✅ Rol actualizado", icon="✅"); time.sleep(0.5); st.rerun()
                        if user_data['activo']:
                            if st.button("🔴 Desactivar", key=f"bd_{user_data['id']}", type="secondary"):
                                if actualizar_usuario(user_data['id'], 'activo', False):
                                    st.toast("✅ Desactivado", icon="✅"); time.sleep(0.5); st.rerun()
                        else:
                            if st.button("🟢 Activar", key=f"ba_{user_data['id']}", type="primary"):
                                if actualizar_usuario(user_data['id'], 'activo', True):
                                    st.toast("✅ Activado", icon="✅"); time.sleep(0.5); st.rerun()
    else: st.info("No hay otros usuarios para editar.")
    
    st.markdown("---")
    st.subheader("🔑 Cambiar mi clave")
    with st.form("cambiar_mi_clave"):
        st.info(f"Usuario actual: **{st.session_state.usuario_actual['usuario']}**")
        nueva_clave_mi = st.text_input("Nueva clave", type="password", placeholder="Mín 4 caracteres")
        confirmar_clave_mi = st.text_input("Confirmar clave", type="password")
        if st.form_submit_button("Actualizar mi clave", type="primary", use_container_width=True):
            if nueva_clave_mi and len(nueva_clave_mi) >= 4:
                if nueva_clave_mi == confirmar_clave_mi:
                    if actualizar_usuario(st.session_state.usuario_actual['id'], 'clave', nueva_clave_mi):
                        st.toast("✅ Clave actualizada", icon="✅"); time.sleep(0.5); st.rerun()
                else: st.error("Las claves no coinciden")
            else: st.error("La clave debe tener al menos 4 caracteres")
    
    st.markdown("---")
    st.subheader("➕ Crear nuevo usuario")
    with st.form("form_nuevo_usuario"):
        col_n1, col_n2 = st.columns(2)
        with col_n1:
            nuevo_usuario = st.text_input("Usuario *", placeholder="Ej: maria")
            nuevo_nombre_completo = st.text_input("Nombre completo *", placeholder="Ej: María Pérez")
        with col_n2:
            nueva_clave_user = st.text_input("Clave *", type="password", placeholder="Mín 4 caracteres")
            nuevo_rol_user = st.selectbox("Rol", ["empleado", "admin"], key="nuevo_rol_select")
        if st.form_submit_button("✅ Crear usuario", use_container_width=True, type="primary"):
            if not nuevo_usuario: st.error("Usuario obligatorio")
            elif not nuevo_nombre_completo: st.error("Nombre obligatorio")
            elif not nueva_clave_user or len(nueva_clave_user) < 4: st.error("Clave mín 4 caracteres")
            else:
                if nuevo_usuario in [u['usuario'] for u in usuarios]: st.error(f"'{nuevo_usuario}' ya existe")
                else:
                    if crear_usuario(nuevo_usuario, nueva_clave_user, nuevo_nombre_completo, nuevo_rol_user):
                        st.toast(f"✅ Usuario '{nuevo_usuario}' creado", icon="✅"); time.sleep(0.5); st.rerun()
                    else: st.error("Error al crear usuario")
