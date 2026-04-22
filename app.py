import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta
import time
import json
import hashlib
import base64
from io import BytesIO

# ============================================
# CONFIGURACIÓN INICIAL
# ============================================
st.set_page_config(
    page_title="BODEGÓN VYM",
    page_icon="🛒",
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
                color: #FF8C00 !important;
            }
            .stMarkdown, .stText, p, span, label, h1, h2, h3, h4 {
                color: #ffffff !important;
            }
            .stButton > button {
                background-color: #FF8C00 !important;
                color: #000000 !important;
            }
            .stButton > button:hover {
                background-color: #E67E00 !important;
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
                background-color: #ffffff;
                color: #000000;
            }
            .main-header {
                color: #1E88E5 !important;
            }
            .stButton > button {
                background-color: #FF8C00 !important;
                color: #ffffff !important;
                border: none;
            }
            .stButton > button:hover {
                background-color: #E67E00 !important;
            }
            .stMarkdown, .stText, p, span, label, h1, h2, h3, h4 {
                color: #000000 !important;
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
        color: #1E88E5 !important;
    }
    .stButton > button {
        background-color: #FF8C00;
        color: white;
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
        border: none;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        background-color: #E67E00;
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
        background-color: #f9f9f9;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
        color: #000000;
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
# CONEXIÓN A SUPABASE (SIEMPRE ONLINE)
# ============================================
URL = "https://phcnjozdhhyvrcbyzahs.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBoY25qb3pkaGh5dnJjYnl6YWhzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ4ODY5NzksImV4cCI6MjA5MDQ2Mjk3OX0.pmFqG1qjuOiEK_SmNXpoimLcT-muLPRtfmUN62h7OYM"
CLAVE_ADMIN = "1234"

db = create_client(URL, KEY)

# ============================================
# SISTEMA DE USUARIOS CON PERSISTENCIA EN URL
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
        st.query_params['usuario'] = usuario
        return True
    return False

def logout():
    st.session_state.usuario_actual = None
    st.session_state.id_turno = None
    if 'usuario' in st.query_params:
        del st.query_params['usuario']
    if 'turno' in st.query_params:
        del st.query_params['turno']

if st.session_state.usuario_actual is None and 'usuario' in st.query_params:
    usuario = st.query_params['usuario']
    if usuario in USUARIOS:
        st.session_state.usuario_actual = USUARIOS[usuario]

# ============================================
# VERIFICAR TURNO ACTIVO (PERSISTENTE)
# ============================================
def restaurar_turno_activo():
    if st.session_state.usuario_actual is None:
        return None
    try:
        resp = db.table("cierres")\
            .select("*")\
            .eq("estado", "abierto")\
            .eq("usuario_apertura", st.session_state.usuario_actual['nombre'])\
            .order("fecha_apertura", desc=True)\
            .limit(1)\
            .execute()
        if hasattr(resp, 'data') and resp.data:
            turno = resp.data[0]
            st.session_state.id_turno = turno['id']
            st.session_state.tasa_dia = turno.get('tasa_apertura', 60.0)
            st.session_state.fondo_bs = turno.get('fondo_bs', 0)
            st.session_state.fondo_usd = turno.get('fondo_usd', 0)
            st.query_params['turno'] = str(turno['id'])
            return turno['id']
    except:
        pass
    return None

if 'id_turno' not in st.session_state or st.session_state.id_turno is None:
    if st.session_state.usuario_actual is not None:
        restaurar_turno_activo()
    else:
        st.session_state.id_turno = None

# ============================================
# MENÚ LATERAL (REDISEÑADO - TASA SOLO INFORMATIVA)
# ============================================
with st.sidebar:
    st.markdown("""
        <div style="background: linear-gradient(135deg, #1E88E5 0%, #FF8C00 100%); 
                    padding: 2rem 1rem; 
                    border-radius: 0 0 20px 20px; 
                    text-align: center; 
                    margin-top: -1rem;
                    margin-bottom: 1rem;">
            <h1 style="color: white; margin: 0; font-size: 2.2rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                🛒 BODEGÓN VYM
            </h1>
            <p style="color: rgba(255,255,255,0.95); margin-top: 0.5rem; font-style: italic;">
                Víveres, carnes y gaseosas
            </p>
        </div>
    """, unsafe_allow_html=True)
    
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
    
    # Tasa BCV (solo informativa)
    with st.container(border=True):
        st.markdown("**💱 TASA BCV**")
        st.metric(
            "Bs/USD",
            value=f"{st.session_state.get('tasa_dia', 60.0):.2f}",
            help="Tasa actualizada desde el cierre de caja"
        )
    
    st.divider()
    
    opcion = st.radio(
        "MÓDULOS",
        ["📦 INVENTARIO", "🛒 PUNTO DE VENTA", "💸 GASTOS", "📜 HISTORIAL", "📊 CIERRE DE CAJA"],
        label_visibility="collapsed"
    )
    
    st.divider()
    
    st.success("✅ Conectado a Internet")
    if st.session_state.id_turno:
        st.info(f"📍 Turno activo: #{st.session_state.id_turno}")
    else:
        st.error("🔴 Caja cerrada")

# ============================================
# FUNCIONES AUXILIARES
# ============================================
def requiere_turno():
    if not st.session_state.id_turno:
        st.warning("⚠️ No hay un turno activo. Debe abrir caja en el módulo 'Cierre de Caja'.")
        st.stop()

def requiere_usuario():
    if not st.session_state.usuario_actual:
        st.warning("⚠️ Debe iniciar sesión para acceder a este módulo.")
        st.stop()

def formatear_usd(valor):
    return f"${valor:,.2f}"

def formatear_bs(valor):
    return f"{valor:,.2f} Bs"

def exportar_excel(df, nombre_archivo):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{nombre_archivo}.xlsx">📥 Descargar Excel</a>'
    return href

# ============================================
# MÓDULO 1: INVENTARIO
# ============================================
if opcion == "📦 INVENTARIO":
    st.markdown("<h1 class='main-header'>📦 Gestión de Inventario</h1>", unsafe_allow_html=True)
    
    CATEGORIAS = [
        "VIVERES", "CONFITERIA", "CHARCUTERIA", "BEBIDAS", "LACTEOS",
        "SNACK", "BISUTERIA", "PAPELERIA", "DETERGENTES", "ASEO PERSONAL",
        "QUINCALLERIA", "OTROS"
    ]
    
    try:
        response = db.table("inventario").select("*").order("nombre").execute()
        df = pd.DataFrame(response.data) if response.data else pd.DataFrame()
        
        if not df.empty:
            if 'categoria' not in df.columns:
                df['categoria'] = 'OTROS'
            if 'codigo_barras' not in df.columns:
                df['codigo_barras'] = ''
        
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
                    export_df = df[['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'min_mayor']].copy()
                    export_df.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Precio Detal $', 'Precio Mayor $', 'Min. Mayor']
                    href = exportar_excel(export_df, f"inventario_{datetime.now().strftime('%Y%m%d')}")
                    st.markdown(href, unsafe_allow_html=True)
            
            if not df.empty:
                df_filtrado = df.copy()
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
                    st.warning(f"⚠️ Hay {len(df_filtrado)} productos con stock bajo") if len(df_filtrado) > 0 else st.success("✅ No hay productos con stock bajo")
                
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
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
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
                                try:
                                    cat_index = CATEGORIAS.index(prod.get('categoria', 'OTROS'))
                                except ValueError:
                                    cat_index = len(CATEGORIAS) - 1
                                nueva_categoria = st.selectbox("Categoría", CATEGORIAS, index=cat_index)
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
                                    db.table("inventario").update(datos_actualizados).eq("id", prod['id']).execute()
                                    st.success("✅ Producto actualizado")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        
                        st.divider()
                        st.subheader("🔗 Códigos de barras alternos")
                        with st.container():
                            alt_codes = db.table("codigos_alternos").select("*").eq("producto_id", int(prod['id'])).execute()
                            if alt_codes.data:
                                st.write("**Códigos actuales:**")
                                for ac in alt_codes.data:
                                    col1, col2 = st.columns([4, 1])
                                    with col1:
                                        st.code(ac["codigo"])
                                    with col2:
                                        if st.button("❌", key=f"del_{ac['id']}"):
                                            db.table("codigos_alternos").delete().eq("id", int(ac["id"])).execute()
                                            st.success("Código eliminado")
                                            time.sleep(1)
                                            st.rerun()
                            else:
                                st.info("No hay códigos alternos para este producto.")
                            nuevo_codigo_alt = st.text_input("Nuevo código de barras (opcional)", key=f"new_alt_{prod['id']}")
                            if st.button("➕ Agregar código", key=f"add_alt_{prod['id']}"):
                                if nuevo_codigo_alt.strip():
                                    try:
                                        db.table("codigos_alternos").insert({
                                            "producto_id": int(prod['id']),
                                            "codigo": nuevo_codigo_alt.strip()
                                        }).execute()
                                        st.success("Código agregado correctamente")
                                        time.sleep(1)
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")
                                else:
                                    st.warning("Escribe un código válido")
                
                st.divider()
                st.subheader("🗑️ Eliminar producto")
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    producto_eliminar = st.selectbox("Seleccionar producto", [""] + df['nombre'].tolist(), key="eliminar")
                with col_d2:
                    clave = st.text_input("Clave Admin", type="password", key="clave_eliminar")
                if producto_eliminar and st.button("❌ Eliminar", type="primary", use_container_width=True):
                    if clave == CLAVE_ADMIN:
                        db.table("inventario").delete().eq("nombre", producto_eliminar).execute()
                        st.success(f"Producto '{producto_eliminar}' eliminado")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Clave incorrecta")
            else:
                st.info("No hay productos en el inventario")
        
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
                if st.form_submit_button("📦 Registrar Producto", use_container_width=True):
                    if not nombre:
                        st.error("El nombre es obligatorio")
                    elif stock < 0 or costo < 0 or precio_detal <= 0:
                        st.error("Verifique los valores ingresados")
                    else:
                        existe = db.table("inventario").select("*").eq("nombre", nombre).execute()
                        if existe.data:
                            st.error(f"Ya existe un producto con el nombre '{nombre}'")
                        else:
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
                            db.table("inventario").insert(datos_nuevos).execute()
                            st.success(f"✅ Producto '{nombre}' registrado exitosamente")
                            time.sleep(1)
                            st.rerun()
        
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
                    if st.button("📥 Exportar inventario completo", use_container_width=True):
                        export_df = df[['nombre', 'categoria', 'stock', 'costo', 'precio_detal', 'precio_mayor', 'min_mayor']].copy()
                        export_df.columns = ['Producto', 'Categoría', 'Stock', 'Costo $', 'Precio Detal $', 'Precio Mayor $', 'Min. Mayor']
                        export_df = export_df.sort_values('Producto')
                        href = exportar_excel(export_df, f"inventario_completo_{datetime.now().strftime('%Y%m%d_%H%M')}")
                        st.markdown(href, unsafe_allow_html=True)
                with col_r2:
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
# MÓDULO 2: PUNTO DE VENTA (CARRITO SIMPLIFICADO + BUSCADOR LIMPIO)
# ============================================
elif opcion == "🛒 PUNTO DE VENTA":
    requiere_turno()
    requiere_usuario()
    
    id_turno = st.session_state.id_turno
    tasa = st.session_state.tasa_dia
    
    st.markdown("<h1 class='main-header'>🛒 Punto de Venta</h1>", unsafe_allow_html=True)
    st.markdown(f"""
        <div style='background-color: #e7f3ff; padding: 0.5rem; border-radius: 8px; margin-bottom: 0.5rem; font-size:0.9rem;'>
            <span style='font-weight:600;'>📍 Turno #{id_turno}</span> | 
            <span>💱 Tasa: {tasa:.2f} Bs/$</span> |
            <span>👤 Cajero: {st.session_state.usuario_actual['nombre']}</span>
        </div>
    """, unsafe_allow_html=True)
    
    # ============================================
    # SISTEMA DE CLIENTES
    # ============================================
    if 'clientes' not in st.session_state:
        st.session_state.clientes = {
            'cliente_1': {'nombre': 'Cliente 1', 'carrito': [], 'activa': True, 'cliente': ''},
            'cliente_2': {'nombre': 'Cliente 2', 'carrito': [], 'activa': True, 'cliente': ''},
            'cliente_3': {'nombre': 'Cliente 3', 'carrito': [], 'activa': True, 'cliente': ''},
            'cliente_4': {'nombre': 'Cliente 4', 'carrito': [], 'activa': True, 'cliente': ''}
        }
    
    if 'cliente_actual' not in st.session_state:
        st.session_state.cliente_actual = 'cliente_1'
    
    st.subheader("👥 Seleccionar Cliente / Cuenta")
    col_clientes = st.columns(4)
    for idx, (cliente_id, cliente_data) in enumerate(st.session_state.clientes.items()):
        with col_clientes[idx]:
            if st.button(
                f"{cliente_data['nombre']} ({len(cliente_data['carrito'])})",
                key=f"cliente_{cliente_id}",
                use_container_width=True,
                type="primary" if cliente_id == st.session_state.cliente_actual else "secondary"
            ):
                st.session_state.cliente_actual = cliente_id
                st.rerun()
    
    cliente_actual = st.session_state.clientes[st.session_state.cliente_actual]
    st.divider()
    
    # CABECERA DEL CLIENTE ACTUAL
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.markdown(f"**Cliente:** {cliente_actual['nombre']}")
    with col2:
        cliente = st.text_input(
            "Nombre (opcional)",
            value=cliente_actual.get('cliente', ''),
            key="cliente_nombre",
            placeholder="Ej: Juan Pérez",
            label_visibility="collapsed"
        )
        if cliente != cliente_actual.get('cliente', ''):
            st.session_state.clientes[st.session_state.cliente_actual]['cliente'] = cliente
    with col3:
        if len(cliente_actual['carrito']) > 0:
            if st.button("🧹 Limpiar", use_container_width=True):
                st.session_state.clientes[st.session_state.cliente_actual]['carrito'] = []
                st.rerun()
    
    # ============================================
    # CACHE DEL INVENTARIO
    # ============================================
    @st.cache_data(ttl=60, show_spinner=False)
    def cargar_inventario():
        try:
            response = db.table("inventario").select("*").execute()
            return response.data if response.data else []
        except Exception as e:
            st.error(f"Error cargando inventario: {e}")
            return []
    
    inventario = cargar_inventario()
    if not inventario:
        st.warning("No se pudo cargar el inventario. Verifica la conexión.")
        st.stop()
    
    # ============================================
    # FUNCIÓN PARA AGREGAR PRODUCTO (CON RECÁLCULO DE PRECIO)
    # ============================================
    def agregar_producto(prod):
        carrito = st.session_state.clientes[st.session_state.cliente_actual]['carrito']
        encontrado = False
        for item in carrito:
            if item['id'] == prod['id']:
                # Incrementar cantidad
                nueva_cant = item['cantidad'] + 1
                # Recalcular precio según nueva cantidad
                if nueva_cant >= prod['min_mayor']:
                    nuevo_precio = float(prod['precio_mayor'])
                    tipo_precio = " (Mayor)"
                else:
                    nuevo_precio = float(prod['precio_detal'])
                    tipo_precio = ""
                item['cantidad'] = nueva_cant
                item['precio'] = nuevo_precio
                item['subtotal'] = item['cantidad'] * item['precio']
                item['tipo_precio'] = tipo_precio
                encontrado = True
                break
        if not encontrado:
            precio_base = float(prod['precio_detal'])
            if 1 >= prod['min_mayor']:
                precio_final = float(prod['precio_mayor'])
                tipo_precio = " (Mayor)"
            else:
                precio_final = precio_base
                tipo_precio = ""
            carrito.append({
                "id": prod['id'],
                "nombre": prod['nombre'],
                "cantidad": 1,
                "precio": precio_final,
                "costo": float(prod['costo']),
                "subtotal": precio_final,
                "tipo_precio": tipo_precio
            })
        st.rerun()
    
    # ============================================
    # CAMPO PRINCIPAL PARA CÓDIGO DE BARRAS (con clear_on_submit)
    # ============================================
    st.markdown("""
        <style>
        .stForm > div:first-child > div:last-child {
            display: none;
        }
        .stTextInput > div > div > input {
            border-radius: 30px;
            padding: 0.75rem 1rem;
            border: 1px solid #ccc;
            font-size: 1rem;
        }
        </style>
    """, unsafe_allow_html=True)
    
    with st.form(key="codigo_form", clear_on_submit=True):
        codigo_input = st.text_input(
            "🔖 Escanear código de barras",
            placeholder="Escanea o escribe el código...",
            label_visibility="collapsed"
        )
        submitted = st.form_submit_button("")
    
    if submitted and codigo_input.strip():
        codigo = codigo_input.strip()
        producto = None
        for p in inventario:
            if p.get('codigo_barras') == codigo and p['stock'] > 0:
                producto = p
                break
        if not producto:
            try:
                resp_alt = db.table("codigos_alternos").select("producto_id").eq("codigo", codigo).execute()
                if hasattr(resp_alt, 'data') and resp_alt.data:
                    pid = resp_alt.data[0]['producto_id']
                    for p in inventario:
                        if p['id'] == pid and p['stock'] > 0:
                            producto = p
                            break
            except:
                pass
        if producto:
            agregar_producto(producto)
        else:
            st.warning(f"Código '{codigo}' no encontrado o sin stock.")
    
    # ============================================
    # BUSCADOR POR NOMBRE EN POPOVER (SIEMPRE LIMPIO)
    # ============================================
    # Contador para generar clave única cada vez que se abre el popover
    if 'popover_counter' not in st.session_state:
        st.session_state.popover_counter = 0
    
    with st.popover("🔍 Buscar por nombre", use_container_width=True):
        # Incrementar contador para clave nueva (campo vacío)
        st.session_state.popover_counter += 1
        key_busqueda = f"buscar_nombre_{st.session_state.popover_counter}"
        
        st.markdown("**Escribe el nombre del producto:**")
        busqueda = st.text_input("", key=key_busqueda, placeholder="Ej: Harina, Aceite...", label_visibility="collapsed")
        
        if busqueda:
            resultados = []
            for p in inventario:
                if p['stock'] <= 0:
                    continue
                if busqueda.lower() in p['nombre'].lower():
                    resultados.append(p)
            resultados = resultados[:30]
            
            if resultados:
                st.markdown("---")
                cols_head = st.columns([3, 1, 1, 1, 0.8])
                cols_head[0].markdown("**Producto**")
                cols_head[1].markdown("**Stock**")
                cols_head[2].markdown("**Precio USD**")
                cols_head[3].markdown("**Precio Bs**")
                cols_head[4].markdown("")
                st.markdown("---")
                for prod in resultados:
                    precio_usd = float(prod['precio_detal'])
                    precio_bs = precio_usd * tasa
                    c1, c2, c3, c4, c5 = st.columns([3, 1, 1, 1, 0.8])
                    c1.write(prod['nombre'])
                    c2.write(f"{prod['stock']:.0f}")
                    c3.write(f"${precio_usd:.2f}")
                    c4.write(f"{precio_bs:,.2f} Bs")
                    if c5.button("➕", key=f"pop_{prod['id']}_{st.session_state.popover_counter}"):
                        agregar_producto(prod)
                        # El rerun cerrará el popover y la próxima vez se generará una nueva clave
                        st.rerun()
            else:
                st.info("No se encontraron productos.")
        else:
            st.info("Escribe al menos una letra para buscar.")
    
    # ============================================
    # CARRITO SIMPLIFICADO (5 COLUMNAS)
    # ============================================
    st.subheader(f"🛒 Carrito - {cliente_actual['nombre']}")
    carrito = cliente_actual['carrito']
    
    if not carrito:
        st.info("Carrito vacío")
    else:
        st.markdown("""
            <style>
            .carrito-scroll {
                max-height: 450px;
                overflow-y: auto;
                margin-bottom: 1rem;
                border: 1px solid #ddd;
                border-radius: 8px;
                background-color: #fefefe;
                padding: 0.5rem;
            }
            .carrito-row {
                display: flex;
                align-items: center;
                padding: 0.5rem 0;
                border-bottom: 1px solid #eee;
            }
            .carrito-row:hover {
                background-color: #f9f9f9;
            }
            </style>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="carrito-scroll">', unsafe_allow_html=True)
        
        # Cabeceras: Producto, Precio USD, Precio Bs, Cantidad, Eliminar
        cols_head = st.columns([3, 1, 1.2, 1, 0.5])
        cols_head[0].write("**Producto**")
        cols_head[1].write("**Precio USD**")
        cols_head[2].write("**Precio Bs**")
        cols_head[3].write("**Cantidad**")
        cols_head[4].write("**Eliminar**")
        st.markdown("---")
        
        total_venta_usd = 0.0
        total_costo = 0.0
        
        # Mostrar cada producto en una fila
        for item in carrito:
            cols = st.columns([3, 1, 1.2, 1, 0.5])
            cols[0].write(item['nombre'])
            cols[1].write(f"${item['precio']:.2f}")
            cols[2].write(f"{item['precio'] * tasa:,.2f} Bs")
            
            # Input de cantidad
            nueva_cant = cols[3].number_input(
                "",
                min_value=0.0,
                max_value=1000.0,
                value=float(item['cantidad']),
                step=1.0,
                key=f"cant_{item['id']}",
                label_visibility="collapsed"
            )
            if nueva_cant != item['cantidad']:
                if nueva_cant == 0:
                    # Eliminar producto
                    st.session_state.clientes[st.session_state.cliente_actual]['carrito'].remove(item)
                    st.rerun()
                else:
                    # Recalcular precio mayorista
                    prod_data = None
                    for p in inventario:
                        if p['id'] == item['id']:
                            prod_data = p
                            break
                    if prod_data:
                        if nueva_cant >= prod_data['min_mayor']:
                            nuevo_precio = float(prod_data['precio_mayor'])
                        else:
                            nuevo_precio = float(prod_data['precio_detal'])
                        item['precio'] = nuevo_precio
                    item['cantidad'] = nueva_cant
                    item['subtotal'] = item['cantidad'] * item['precio']
                    st.rerun()
            
            # Botón eliminar
            if cols[4].button("❌", key=f"del_{item['id']}"):
                st.session_state.clientes[st.session_state.cliente_actual]['carrito'].remove(item)
                st.rerun()
            
            total_venta_usd += item['subtotal']
            total_costo += item['cantidad'] * item['costo']
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        total_venta_bs = total_venta_usd * tasa
        st.divider()
        col_t1, col_t2 = st.columns(2)
        col_t1.markdown(f"### Total USD: ${total_venta_usd:,.2f}")
        col_t2.markdown(f"### Total Bs: {total_venta_bs:,.2f}")
        
        # REDONDEO
        total_final_usd = total_venta_usd
        total_final_bs = total_venta_bs
        with st.expander("🔧 Ajustar monto final (redondeo)", expanded=False):
            st.markdown("Si deseas redondear el total a cobrar, selecciona una opción e ingresa el monto:")
            opcion = st.radio("Ajustar en:", ["No ajustar (usar calculado)", "Bolívares (Bs)", "Dólares (USD)"], horizontal=True, key="redondeo")
            if opcion == "Bolívares (Bs)":
                nuevo_bs = st.number_input("Monto final en Bs", min_value=0.0, value=float(total_venta_bs), step=10.0, format="%.2f", key="ajuste_bs")
                total_final_bs = nuevo_bs
                total_final_usd = nuevo_bs / tasa if tasa else 0
            elif opcion == "Dólares (USD)":
                nuevo_usd = st.number_input("Monto final en USD", min_value=0.0, value=float(total_venta_usd), step=1.0, format="%.2f", key="ajuste_usd")
                total_final_usd = nuevo_usd
                total_final_bs = nuevo_usd * tasa
        
        st.divider()
        
        # PAGOS (sin cambios)
        with st.expander("💳 Detalle de pagos", expanded=True):
            st.markdown("**Ingresa los montos recibidos:**")
            col_p1, col_p2 = st.columns(2)
            with col_p1:
                p_usd_ef = st.number_input("Efectivo USD", min_value=0.0, step=5.0, format="%.2f", key="p_ef_usd")
                p_zelle = st.number_input("Zelle USD", min_value=0.0, step=5.0, format="%.2f", key="p_zelle")
                p_otros_usd = st.number_input("Otros USD", min_value=0.0, step=5.0, format="%.2f", key="p_otros_usd")
            with col_p2:
                p_bs_ef = st.number_input("Efectivo Bs", min_value=0.0, step=100.0, format="%.2f", key="p_ef_bs")
                p_movil = st.number_input("Pago Móvil Bs", min_value=0.0, step=100.0, format="%.2f", key="p_movil")
                p_punto = st.number_input("Punto de Venta Bs", min_value=0.0, step=100.0, format="%.2f", key="p_punto")
            
            total_usd = p_usd_ef + p_zelle + p_otros_usd
            total_bs = p_bs_ef + p_movil + p_punto
            total_equiv = total_usd + (total_bs / tasa if tasa else 0)
            esperado_usd = total_final_bs / tasa if tasa else 0
            vuelto = total_equiv - esperado_usd
            
            st.divider()
            col_r1, col_r2, col_r3 = st.columns(3)
            col_r1.metric("Pagado USD eq.", f"${total_equiv:,.2f}")
            col_r2.metric("Esperado USD", f"${esperado_usd:,.2f}")
            if vuelto >= 0:
                col_r3.metric("Vuelto USD", f"${vuelto:,.2f}")
            else:
                col_r3.metric("Faltante USD", f"${abs(vuelto):,.2f}", delta_color="inverse")
            st.success(f"✅ Pago suficiente. Vuelto: ${vuelto:.2f} / {(vuelto * tasa):,.2f} Bs" if vuelto >= -0.01 else f"❌ Faltante: ${abs(vuelto):,.2f} / {(abs(vuelto) * tasa):,.2f} Bs")
        
        # BOTONES DE ACCIÓN
        col_b1, col_b2, col_b3 = st.columns(3)
        with col_b1:
            if st.button("🔄 Limpiar carrito", use_container_width=True):
                st.session_state.clientes[st.session_state.cliente_actual]['carrito'] = []
                st.rerun()
        with col_b2:
            if st.button("✅ Cobrar y cerrar cuenta", type="primary", use_container_width=True, disabled=not (vuelto >= -0.01 and carrito)):
                try:
                    items_res = [f"{item['cantidad']:.0f}x {item['nombre']}" for item in carrito]
                    for item in carrito:
                        stock_actual = db.table("inventario").select("stock").eq("id", item['id']).execute().data[0]['stock']
                        db.table("inventario").update({"stock": stock_actual - item['cantidad']}).eq("id", item['id']).execute()
                    info_cli = f" - Cliente: {cliente_actual.get('cliente', '')}" if cliente_actual.get('cliente') else ""
                    venta = {
                        "id_cierre": id_turno,
                        "producto": ", ".join(items_res),
                        "cantidad": len(carrito),
                        "total_usd": round(total_final_usd, 2),
                        "monto_cobrado_bs": round(total_final_bs, 2),
                        "tasa_cambio": tasa,
                        "pago_divisas": round(p_usd_ef, 2),
                        "pago_zelle": round(p_zelle, 2),
                        "pago_otros": round(p_otros_usd, 2),
                        "pago_efectivo": round(p_bs_ef, 2),
                        "pago_movil": round(p_movil, 2),
                        "pago_punto": round(p_punto, 2),
                        "costo_venta": round(total_costo, 2),
                        "estado": "Finalizado",
                        "items": json.dumps(carrito),
                        "id_transaccion": str(int(datetime.now().timestamp())),
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
                        df_ticket = pd.DataFrame([{
                            "Cant": f"{item['cantidad']:.0f}",
                            "Producto": item['nombre'],
                            "Precio USD": f"${item['precio']:.2f}",
                            "Subtotal USD": f"${item['subtotal']:.2f}",
                            "Subtotal Bs": f"{item['subtotal'] * tasa:,.2f}"
                        } for item in carrito])
                        st.dataframe(df_ticket, use_container_width=True, hide_index=True)
                        st.divider()
                        col1, col2 = st.columns(2)
                        col1.metric("Total USD", f"${total_final_usd:,.2f}")
                        col2.metric("Total Bs", f"{total_final_bs:,.2f} Bs")
                        st.metric("Vuelto", f"${vuelto:.2f} USD / {(vuelto * tasa):,.2f} Bs")
                        if st.button("Cerrar ticket", use_container_width=True):
                            st.rerun()
                    ticket()
                    st.session_state.clientes[st.session_state.cliente_actual]['carrito'] = []
                    st.session_state.clientes[st.session_state.cliente_actual]['cliente'] = ''
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        with col_b3:
            if carrito and st.button("⏸️ Dejar pendiente", use_container_width=True):
                st.session_state.cliente_actual = 'cliente_1'
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
        response = db.table("gastos").select("*").eq("id_cierre", id_turno).order("fecha", desc=True).execute()
        df_gastos = pd.DataFrame(response.data) if response.data else pd.DataFrame()
        
        if not df_gastos.empty:
            st.subheader("📋 Gastos del turno")
            if 'fecha' in df_gastos.columns:
                df_gastos['fecha'] = pd.to_datetime(df_gastos['fecha']).dt.strftime('%d/%m/%Y %H:%M')
            columnas_mostrar = ['fecha', 'descripcion', 'monto_usd']
            if 'categoria' in df_gastos.columns:
                columnas_mostrar.append('categoria')
            if 'estado' in df_gastos.columns:
                columnas_mostrar.append('estado')
            st.dataframe(df_gastos[columnas_mostrar], use_container_width=True, hide_index=True)
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
        if st.form_submit_button("✅ Registrar gasto", use_container_width=True):
            if descripcion and monto_usd > 0:
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
                db.table("gastos").insert(gasto_data).execute()
                st.success("✅ Gasto registrado correctamente")
                time.sleep(1)
                st.rerun()
            else:
                st.warning("⚠️ Complete los campos obligatorios (*)")

# ============================================
# MÓDULO 4: HISTORIAL DE VENTAS
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
        response = db.table("ventas").select("*").order("fecha", desc=True).execute()
        df = pd.DataFrame(response.data) if response.data else pd.DataFrame()
        
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
                estado_filtro = st.selectbox("Estado", ["Todos", "Finalizado", "Anulado"], key="filtro_estado")
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
                    .venta-row { display: flex; align-items: center; padding: 0.8rem; margin: 0.2rem 0; border-radius: 8px; transition: all 0.2s; }
                    .venta-row:hover { transform: translateX(5px); box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
                    .venta-finalizada { background-color: #ffffff; border-left: 4px solid #28a745; }
                    .venta-anulada { background-color: #f8f9fa; border-left: 4px solid #dc3545; opacity: 0.7; }
                    .badge-finalizada { background-color: #28a745; color: white; padding: 0.2rem 0.6rem; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
                    .badge-anulada { background-color: #dc3545; color: white; padding: 0.2rem 0.6rem; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
                    </style>
                """, unsafe_allow_html=True)
                
                col_h1, col_h2, col_h3, col_h4, col_h5, col_h6, col_h7, col_h8 = st.columns([0.6, 0.8, 0.8, 2.2, 1.0, 1.0, 0.8, 0.8])
                col_h1.markdown("**Turno**")
                col_h2.markdown("**ID**")
                col_h3.markdown("**Hora**")
                col_h4.markdown("**Productos**")
                col_h5.markdown("**USD**")
                col_h6.markdown("**Bs**")
                col_h7.markdown("**Estado**")
                col_h8.markdown("**Acción**")
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
                                                stock_res = db.table("inventario").select("stock").eq("id", item['id']).execute()
                                                if stock_res.data:
                                                    stock_actual = stock_res.data[0]['stock']
                                                    db.table("inventario").update({
                                                        "stock": stock_actual + item['cantidad']
                                                    }).eq("id", item['id']).execute()
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
# MÓDULO 5: CIERRE DE CAJA
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
