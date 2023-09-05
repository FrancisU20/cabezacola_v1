import pandas as pd
import numpy as np
import traceback
import os
from tqdm import tqdm
import impala.util
from impala.dbapi import connect
from datetime import datetime
from ENVIAR_MENSAJE import enviar_mensaje

try:
    cabecera_mensaje = "ESTADO ACTUAL PROCESO CABEZA COLA:\n"
    pdvs = pd.DataFrame()
    pdvs["BODEGA"] = [
        "173",
        "254",
        "161",
        "212",
        "1120",
    ]
    List_PDV = list(pdvs["BODEGA"].unique())
    anio_mes = datetime.now()
    anio_mes = str(anio_mes.strftime("%Y%m"))
    cnxn7 = connect(
        "192.168.238.7", port=21050, auth_mechanism="NOSASL", database="dwh"
    )
    cursor7 = cnxn7.cursor(user="ccalan")
    cursor7.execute("select user();")
    cursor7.execute(
        """
                with categorizacion as (
                        select
                        sim.pp_idbodega BODEGA,
                        sim.pp_codigo CODIGO,
                        sim.pp_categoria_final CATEGORIA,
                        sim.pp_promedio PROMEDIO,
                        sim.pp_dia_min DIA_MIN,
                        sim.pp_dia_max DIA_MAX,
                        sim.pp_minimo MINIMO,
                        sim.pp_maximo MAXIMO,
                        sim.pp_semana_no_vendida SEMANA_NO_VENDIDA
                        from reportes.categorizacionpdvtablacedis sim
                        where sim.pp_idbodega in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                        )
            ,stock as     (
                select bp.idbodega, bp.codigo, (bp.existencias/fp.valor_pos)  STOCK  from easygestionempresarial.tbl_bodegaproducto bp
                left join dwh.farmaproductos fp on fp.codproducto = bp.codigo
                where aniomes = '{anio_mes}' and bp.idbodega in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                )
            ,bloqueoproducto as (
                        select bpf_codigo_producto CODIGO, bpf_codigo_bodega idbodega, bpf_usuario USUARIO_BLOQUEO_SC, bpf_observacion OBSERVACION_BLOQ	  from easygestionempresarial.tbl_bloqueoproductosfarmacias
                        where bpf_estado = 'A' and bpf_codigo_bodega in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                        )
            ,bloqueoabastecimiento as     (
                                select aba_codigo_producto CODIGO, aba_oficina idbodega, aba_usuario_creacion USUARIO_BLOQUEO_PDV, aba_motivo_bloqueo MOTIVO_BLOQUEO_PDV  from easygestionempresarial.tbl_articulobloqueoabastecimiento
                                where aba_estado = 'ACTIVO' and aba_oficina in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                                )
            ,siembra as (
                select si_idsiembra BODEGA,si_codigo codigo, si_cantidad SIEMBRA,si_detalle MOTIVO_SIEMBRA, si_usuario_ingreso USUARIO_SIEMBRA from easygestionempresarial.tbl_siembrainventario
                where si_estado = 'A' and si_idsiembra in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                )
            ,siembrapdv as (
                select spv_idoficina BODEGA, spv_codigo codigo, spv_cantidad siembra_pdv from easygestionempresarial.tbl_siembrainventario_pv 
                where spv_estado = 'A' and spv_idoficina in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                ) 
            ,alpha as (
                select oa_oficina BODEGA FROM easygestionempresarial.tbl_oficinaalpha
                where oa_enproduccion = 'SI' and oa_oficina in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """
                                   )
select
                        sim.BODEGA,
                        sim.CODIGO,
                        sim.CATEGORIA,
                        sim.PROMEDIO,
                        sim.DIA_MIN,
                        sim.DIA_MAX,
                        sim.MINIMO,
                        sim.MAXIMO,
                        st.STOCK,
                        sim.SEMANA_NO_VENDIDA,
                        bk.USUARIO_BLOQUEO_SC,
                        bk.OBSERVACION_BLOQ,
                        bk2.USUARIO_BLOQUEO_PDV,
                        bk2.MOTIVO_BLOQUEO_PDV,
                        si.SIEMBRA,
                        si.MOTIVO_SIEMBRA,
                        si.USUARIO_SIEMBRA,
                        spv.SIEMBRA_PDV
from categorizacion sim
left join stock st on st.idbodega = sim.BODEGA and st.CODIGO = sim.CODIGO
left join bloqueoproducto bk on bk.idbodega=sim.BODEGA and bk.CODIGO=sim.CODIGO
left join bloqueoabastecimiento bk2 on bk2.idbodega = sim.BODEGA and bk2.CODIGO=sim.CODIGO
left join siembra si on si.BODEGA = sim.BODEGA and si.CODIGO = sim.CODIGO
left join siembraPDV spv on spv.BODEGA = sim.BODEGA and spv.CODIGO = sim.CODIGO
inner join alpha al on al.BODEGA = sim.BODEGA                            
where sim.BODEGA in """
        + str(tuple(pdvs["BODEGA"].unique()))
        + """                
                """
    )
    ArchivoDeClasificacion = impala.util.as_pandas(cursor7)
    cnxn7.close()

    ArchivoDeClasificacion.rename(
        columns={
            "bodega": "BODEGA",
            "codigo": "CODIGO",
            "categoria": "CATEGORIA",
            "promedio": "PROMEDIO",
            "dia_min": "DIA_MIN",
            "dia_max": "DIA_MAX",
            "minimo": "MINIMO",
            "maximo": "MAXIMO",
            "stock": "STOCK",
            "semana_no_vendida": "SEMANA_NO_VENDIDA",
            "usuario_bloqueo_sc": "USUARIO_BLOQUEO_SC",
            "observacion_bloq": "OBSERVACION_BLOQ",
            "usuario_bloqueo_pdv": "USUARIO_BLOQUEO_PDV",
            "motivo_bloqueo_pdv": "MOTIVO_BLOQUEO_PDV",
            "siembra": "SIEMBRA",
            "motivo_siembra": "MOTIVO_SIEMBRA",
            "usuario_siembra": "USUARIO_SIEMBRA",
            "siembra_pdv": "SIEMBRA_PDV",
        },
        inplace=True,
    )  # "usuario_siembra_modf":"USUARIO_SIEMBRA_MODF",
    ArchivoDeClasificacion = ArchivoDeClasificacion.drop_duplicates(
        subset=["BODEGA", "CODIGO"]
    )
    enviar_mensaje("0998630405", f"{cabecera_mensaje}UNIENDO EL NOMBRE DE LA FARMACIA")
    cnxn7 = connect(
        "192.168.238.7", port=21050, auth_mechanism="NOSASL", database="dwh"
    )
    cursor7 = cnxn7.cursor(user="ccalan")
    cursor7.execute("select user();")
    cursor7.execute("""select codfarmacia,farmacia from dwh.farmafarmacia """)
    farmafarmacia = impala.util.as_pandas(cursor7)
    cnxn7.close()
    farmafarmacia = farmafarmacia.drop_duplicates()
    farmafarmacia = farmafarmacia.rename(
        columns={"codfarmacia": "BODEGA", "farmacia": "FARMACIA"}
    )
    ArchivoDeClasificacion = pd.merge(
        ArchivoDeClasificacion, farmafarmacia, how="left", on=["BODEGA"]
    )
    ArchivoDeClasificacion = ArchivoDeClasificacion[
        [
            "BODEGA",
            "FARMACIA",
            "CODIGO",
            "CATEGORIA",
            "PROMEDIO",
            "DIA_MIN",
            "DIA_MAX",
            "MINIMO",
            "MAXIMO",
            "STOCK",
            "SEMANA_NO_VENDIDA",
            "USUARIO_BLOQUEO_SC",
            "OBSERVACION_BLOQ",
            "USUARIO_BLOQUEO_PDV",
            "MOTIVO_BLOQUEO_PDV",
            "SIEMBRA",
            "MOTIVO_SIEMBRA",
            "USUARIO_SIEMBRA",
            "SIEMBRA_PDV",
        ]
    ]  # 'USUARIO_SIEMBRA_MODF',
    ArchivoDeClasificacion["SEMANA_NO_VENDIDA"] = ArchivoDeClasificacion[
        "SEMANA_NO_VENDIDA"
    ].astype(int)
    ArchivoDeClasificacion["CATEGORIA"] = ArchivoDeClasificacion["CATEGORIA"].fillna(
        "X"
    )
    ArchivoDeClasificacion = ArchivoDeClasificacion[
        ArchivoDeClasificacion["CATEGORIA"] != "X"
    ]
    ArchivoDeClasificacion["OBS"] = np.where(
        (ArchivoDeClasificacion["PROMEDIO"] == 0)
        & (ArchivoDeClasificacion["CATEGORIA"] == "E"),
        "FN",
        "PS",
    )
    ArchivoDeClasificacion = ArchivoDeClasificacion[
        ArchivoDeClasificacion["OBS"] != "FN"
    ]
    ArchivoDeClasificacion["OBS"] = np.where(
        (ArchivoDeClasificacion["CATEGORIA"] == "D")
        | (ArchivoDeClasificacion["CATEGORIA"] == "N")
        | (ArchivoDeClasificacion["CATEGORIA"] == "Z"),
        "ELIMINAR",
        "NO",
    )
    ArchivoDeClasificacion = ArchivoDeClasificacion[
        ArchivoDeClasificacion["OBS"] != "ELIMINAR"
    ]
    del ArchivoDeClasificacion["OBS"]
    list_catalogo = []
    for dirpath, dirnames, filenames in os.walk(
        r"C:\OneD\BI\OneDrive - Farmaenlace\CATÁLOGO BODEGA CENTRAL"
    ):
        for filename in filenames:
            list_catalogo.append(os.path.join(dirpath, filename))
            list_catalogo = list_catalogo[-1]
            try:
                CatalogoActualizado = pd.read_excel(
                    list_catalogo,
                    skiprows=4,
                    converters={"Cód. Art.": str},
                    parse_dates=["fechaIngreso"],
                )
            except FileNotFoundError:
                enviar_mensaje(
                    "0998630405",
                    f"{cabecera_mensaje}El archivo no existe / No está en la ubicación correcta. {list_catalogo}",
                )
            except PermissionError:
                enviar_mensaje(
                    "0998630405",
                    f"{cabecera_mensaje}No tienes permiso para leer este archivo. {list_catalogo}",
                )
            except UnicodeDecodeError:
                enviar_mensaje(
                    "0998630405",
                    f"{cabecera_mensaje}El archivo no está en el formato correcto. {list_catalogo}",
                )
            except IsADirectoryError:
                enviar_mensaje(
                    "0998630405",
                    f"{cabecera_mensaje}Estás intentando leer un directorio en lugar de un archivo. {list_catalogo}",
                )
            except IOError:
                enviar_mensaje(
                    "0998630405",
                    f"{cabecera_mensaje}No se pudo leer el archivo debido a un problema de E/S. {list_catalogo}",
                )
                enviar_mensaje(
                    "0998630405",
                    f"{cabecera_mensaje}Ruta CATÁLOGO leído: {list_catalogo}",
                )

    del dirnames, dirpath, filename, filenames, list_catalogo
    CatalogoActualizado = CatalogoActualizado.rename(
        columns={
            "Cód. Art.": "CODIGO",
            "Artículo": "DESCRIPCION",
            "Categ.": "CatBC",
            "Stock BC": "STOCK_BC",
            "estadoBC": "Estado_BC",
        }
    )
    CatalogoActualizado["UBI"] = CatalogoActualizado.loc[:, "UBICACION"].str[0:2]
    CatalogoActualizado = CatalogoActualizado[
        ["CODIGO", "CatBC", "Nivel1", "UBI", "PPP"]
    ]
    ArchivoDeClasificacion = ArchivoDeClasificacion.merge(
        CatalogoActualizado, how="left"
    )
    ArchivoDeClasificacion = ArchivoDeClasificacion[
        (ArchivoDeClasificacion["CATEGORIA"] != "Z")
        & (ArchivoDeClasificacion["CATEGORIA"] != "N")
        & (ArchivoDeClasificacion["CATEGORIA"] != "D")
        & (ArchivoDeClasificacion["Nivel1"] != "SERVICIOS")
        & (ArchivoDeClasificacion["Nivel1"] != "SUMINISTROS")
        & (ArchivoDeClasificacion["UBI"] != "Z-")
        & (ArchivoDeClasificacion["CatBC"] != "Z")
        & (ArchivoDeClasificacion["CatBC"] != "D")
    ]

    ArchivoDeClasificacion1 = ArchivoDeClasificacion[
        [
            "BODEGA",
            "CODIGO",
            "CATEGORIA",
            "MINIMO",
            "MAXIMO",
            "DIA_MIN",
            "DIA_MAX",
            "PROMEDIO",
            "SIEMBRA",
            "CatBC",
            "Nivel1",
            "UBI",
            "PPP",
        ]
    ]
    List_PDV = [
        "173",
        "161",
        "212",
        "065",
        "654",
        "842",
        "562",
        "213",
        "295",
        "199",
        "2078",
        "3157",
        "751",
        "848",
        "849",
    ]  # Farmacias Piloto
    consolidado = pd.DataFrame(
        columns=(
            [
                "BODEGA",
                "CODIGO",
                "CATEGORIA",
                "MINIMO",
                "MAXIMO",
                "DIA_MIN",
                "DIA_MAX",
                "PROMEDIO",
                "SIEMBRA",
                "CatBC",
                "Nivel1",
                "UBI",
                "PPP",
                "Brecha",
                "Desvest",
                "NumGrupos",
                "NumSKU",
                "ReglaScott",
                "Validación",
                "GRUPO",
                "NEW_MAX",
                "ACCION",
            ]
        )
    )
    DataReglaScott = pd.DataFrame()
    for PDV in tqdm(List_PDV):
        PDVAux = ArchivoDeClasificacion1[ArchivoDeClasificacion1["BODEGA"] == PDV]
        List_CAT = list(PDVAux["CATEGORIA"].unique())
        List_CAT = sorted(List_CAT)
        E = List_CAT.pop(3)
        List_CAT.insert(0, E)
    for CAT in List_CAT:
        PDVCatAux = PDVAux[PDVAux["CATEGORIA"] == CAT]
        Brecha = PDVCatAux["MAXIMO"].max() - PDVCatAux["MAXIMO"].min()
        Desvest = round(np.std(PDVCatAux["MAXIMO"]), 0)
        NumSKU = len(PDVCatAux["MAXIMO"])
        ReglaScott = (3.5 * np.std(PDVCatAux["MAXIMO"])) / (NumSKU ** (1 / 3))
        NumGrupos = (
            round(Brecha / ReglaScott, 0) if (Brecha != 0) and (ReglaScott != 0) else 0
        )
        data = {
            "BODEGA": PDV,
            "CATEGORIA": CAT,
            "NumSKU": NumSKU,
            "Brecha": Brecha,
            "Desvest": Desvest,
            "ReglaScott": ReglaScott,
            "NumGrupos": NumGrupos,
        }
        DataReglaScott = DataReglaScott._append(data, ignore_index=True)
    # ANÁLISIS
    CabCol = [
        (DataReglaScott["ReglaScott"] >= 1) & (DataReglaScott["NumSKU"] > 1),
        (DataReglaScott["ReglaScott"] >= 0.5)
        & (DataReglaScott["ReglaScott"] < 1)
        & (DataReglaScott["NumSKU"] > 1),
        (DataReglaScott["ReglaScott"] > 0)
        & (DataReglaScott["ReglaScott"] < 0.5)
        & (DataReglaScott["NumSKU"] > 1)
        & (DataReglaScott["Brecha"] > 1),
    ]
    etiquetas = [
        "SuperCabezaCola",
        "SuperCabeza y 50% Cola > Prom (+1)",
        "Todo a la media",
    ]
    DataReglaScott["Validación"] = np.select(CabCol, etiquetas, default="No aplica")
    PDVAux = PDVAux.merge(DataReglaScott, how="left", on=["BODEGA", "CATEGORIA"])
    del (
        data,
        NumGrupos,
        ReglaScott,
        NumSKU,
        Desvest,
        Brecha,
        PDVCatAux,
        E,
        CabCol,
        etiquetas,
    )
    parametros = DataReglaScott[
        (DataReglaScott["Validación"] != "No aplica")
        & (DataReglaScott["BODEGA"] == PDV)
    ]
    for i in parametros["Validación"].unique():
        if i == "SuperCabezaCola":
            data = PDVAux[PDVAux["Validación"] == i]
            for j in data["CATEGORIA"].unique():
                if j == "E":
                    data1 = data[data["CATEGORIA"] == "E"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "E"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cola = data1[data1["GRUPO"] == 1]
                    media_cola = round(media_cola[["MAXIMO"]].mean(), 0)
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 1)
                        & (data1["NEW_MAX"] <= media_cola.iloc[0]),
                        media_cola.iloc[0],
                        data1["NEW_MAX"],
                    )
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)
                if j == "A":
                    data1 = data[data["CATEGORIA"] == "A"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "A"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cola = data1[data1["GRUPO"] == 1]
                    media_cola = round(media_cola[["MAXIMO"]].mean(), 0)
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 1)
                        & (data1["NEW_MAX"] <= media_cola.iloc[0]),
                        media_cola.iloc[0],
                        data1["NEW_MAX"],
                    )
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "B":
                    data1 = data[data["CATEGORIA"] == "B"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "B"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cola = data1[data1["GRUPO"] == 1]
                    media_cola = round(media_cola[["MAXIMO"]].mean(), 0)
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 1)
                        & (data1["NEW_MAX"] <= media_cola.iloc[0]),
                        media_cola.iloc[0],
                        data1["NEW_MAX"],
                    )
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

        if i == "SuperCabeza y 50% Cola > Prom (+1)":
            data = PDVAux[PDVAux["Validación"] == i]
            for j in data["CATEGORIA"].unique():
                if j == "E":
                    data1 = data[data["CATEGORIA"] == "E"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "E"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1 = data1.sort_values(by=["PROMEDIO"], ascending=False)
                    data1y = data1[data1["GRUPO"] == 0]
                    data1x = data1[data1["GRUPO"] == 1]
                    data1xx = data1x[data1x["MAXIMO"] == min(data1x["MAXIMO"])]
                    data1xy = data1x[data1x["MAXIMO"] != min(data1x["MAXIMO"])]
                    del data1x
                    for k in range(int(round((data1xx["CODIGO"].count() / 2), 0))):
                        data1xx.iloc[k, 19] = data1xx.iloc[k, 19] + 1
                    data1 = pd.concat([data1xx, data1xy, data1y], ignore_index=False)
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "A":
                    data1 = data[data["CATEGORIA"] == "A"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "A"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1 = data1.sort_values(by=["PROMEDIO"], ascending=False)
                    data1y = data1[data1["GRUPO"] == 0]
                    data1x = data1[data1["GRUPO"] == 1]
                    data1xx = data1x[data1x["MAXIMO"] == min(data1x["MAXIMO"])]
                    data1xy = data1x[data1x["MAXIMO"] != min(data1x["MAXIMO"])]
                    del data1x
                    for k in range(int(round((data1xx["CODIGO"].count() / 2), 0))):
                        data1xx.iloc[k, 19] = data1xx.iloc[k, 19] + 1
                    data1 = pd.concat([data1xx, data1xy, data1y], ignore_index=False)
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "B":
                    data1 = data[data["CATEGORIA"] == "B"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "B"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1 = data1.sort_values(by=["PROMEDIO"], ascending=False)
                    data1y = data1[data1["GRUPO"] == 0]
                    data1x = data1[data1["GRUPO"] == 1]
                    data1xx = data1x[data1x["MAXIMO"] == min(data1x["MAXIMO"])]
                    data1xy = data1x[data1x["MAXIMO"] != min(data1x["MAXIMO"])]
                    del data1x
                    for k in range(int(round((data1xx["CODIGO"].count() / 2), 0))):
                        data1xx.iloc[k, 19] = data1xx.iloc[k, 19] + 1
                    data1 = pd.concat([data1xx, data1xy, data1y], ignore_index=False)
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "C":
                    data1 = data[data["CATEGORIA"] == "C"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "C"]
                    ancho_grupo = round(ancho_grupo.iloc[0, 6])
                    data1["GRUPO"] = np.where(
                        (data1["MAXIMO"] <= min(data1["MAXIMO"]) + ancho_grupo), 1, 0
                    )
                    media_cabeza = data1[data1["GRUPO"] == 0]
                    media_cabeza = round(media_cabeza[["MAXIMO"]].mean(), 0)
                    data1["NEW_MAX"] = np.where(
                        (data1["GRUPO"] == 0)
                        & (data1["MAXIMO"] >= media_cabeza.iloc[0]),
                        media_cabeza.iloc[0],
                        data1["MAXIMO"],
                    )
                    data1 = data1.sort_values(by=["PROMEDIO"], ascending=False)
                    data1y = data1[data1["GRUPO"] == 0]
                    data1x = data1[data1["GRUPO"] == 1]
                    data1xx = data1x[data1x["MAXIMO"] == min(data1x["MAXIMO"])]
                    data1xy = data1x[data1x["MAXIMO"] != min(data1x["MAXIMO"])]
                    del data1x
                    for k in range(int(round((data1xx["CODIGO"].count() / 2), 0))):
                        data1xx.iloc[k, 19] = data1xx.iloc[k, 19] + 1
                    data1 = pd.concat([data1xx, data1xy, data1y], ignore_index=False)
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if i == "Todo a la media":
                    data = PDVAux[PDVAux["Validación"] == i]
                    for j in data["CATEGORIA"].unique():
                        if j == "E":
                            data1 = data[data["CATEGORIA"] == "E"]
                            ancho_grupo = parametros[parametros["CATEGORIA"] == "E"]
                            # data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                            data1["NEW_MAX"] = np.where(
                                (
                                    data1["MAXIMO"]
                                    <= int(round(data1[["MAXIMO"]].mean(), 0))
                                ),
                                int(round(data1[["MAXIMO"]].mean(), 0)),
                                data1["MAXIMO"],
                            )

                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "A":
                    data1 = data[data["CATEGORIA"] == "A"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "A"]
                    # data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1["NEW_MAX"] = np.where(
                        (data1["MAXIMO"] <= int(round(data1[["MAXIMO"]].mean(), 0))),
                        int(round(data1[["MAXIMO"]].mean(), 0)),
                        data1["MAXIMO"],
                    )

                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "B":
                    data1 = data[data["CATEGORIA"] == "B"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "B"]
                    # data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1["NEW_MAX"] = np.where(
                        (data1["MAXIMO"] <= int(round(data1[["MAXIMO"]].mean(), 0))),
                        int(round(data1[["MAXIMO"]].mean(), 0)),
                        data1["MAXIMO"],
                    )

                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "C":
                    data1 = data[data["CATEGORIA"] == "C"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "C"]
                    data1["NEW_MAX"] = np.where(
                        (data1["MAXIMO"] <= int(round(data1[["MAXIMO"]].mean(), 0))),
                        int(round(data1[["MAXIMO"]].mean(), 0)),
                        data1["MAXIMO"],
                    )

                    # data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

                if j == "L":
                    data1 = data[data["CATEGORIA"] == "L"]
                    ancho_grupo = parametros[parametros["CATEGORIA"] == "L"]
                    # data1['NEW_MAX'] = int(round(data1[['MAXIMO']].mean(),0))
                    data1["NEW_MAX"] = np.where(
                        (data1["MAXIMO"] <= int(round(data1[["MAXIMO"]].mean(), 0))),
                        int(round(data1[["MAXIMO"]].mean(), 0)),
                        data1["MAXIMO"],
                    )
                    # data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    data1["ACCION"] = i
                    consolidado = pd.concat([consolidado, data1], ignore_index=False)

        categorias_modificadas = parametros["CATEGORIA"].unique()
        try:
            min_maxE = consolidado[
                (consolidado["BODEGA"] == PDV) & (consolidado["CATEGORIA"] == "E")
            ]
            maxE = int(max(min_maxE["NEW_MAX"]))
            minE = int(min(min_maxE["NEW_MAX"]))
        except:
            enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin categoria E")

        try:
            min_maxA = consolidado[
                (consolidado["BODEGA"] == PDV) & (consolidado["CATEGORIA"] == "A")
            ]
            maxA = int(max(min_maxA["NEW_MAX"]))
            minA = int(min(min_maxA["NEW_MAX"]))
        except:
            enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin categoria A")

        try:
            min_maxB = consolidado[
                (consolidado["BODEGA"] == PDV) & (consolidado["CATEGORIA"] == "B")
            ]
            maxB = int(max(min_maxB["NEW_MAX"]))
            minB = int(min(min_maxB["NEW_MAX"]))

        except:
            enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin categoria B")

        try:
            min_maxC = consolidado[
                (consolidado["BODEGA"] == PDV) & (consolidado["CATEGORIA"] == "C")
            ]
            maxC = int(max(min_maxC["NEW_MAX"]))
            minC = int(min(min_maxC["NEW_MAX"]))

        except:
            enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin categoria C")

        try:
            consolidado["NEW_MAX"] = np.where(
                (consolidado["CATEGORIA"] == "E")
                & (consolidado["BODEGA"] == PDV)
                & (consolidado["NEW_MAX"] < maxA),
                maxA,
                consolidado["NEW_MAX"],
            )
            del maxA
        except:
            enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin correción en A")

    try:
        consolidado["NEW_MAX"] = np.where(
            (consolidado["CATEGORIA"] == "A")
            & (consolidado["BODEGA"] == PDV)
            & (consolidado["NEW_MAX"] < maxB),
            maxB,
            consolidado["NEW_MAX"],
        )
        del maxB
    except:
        enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin corrección en B")

        try:
            consolidado["NEW_MAX"] = np.where(
                (consolidado["CATEGORIA"] == "B")
                & (consolidado["BODEGA"] == PDV)
                & (consolidado["NEW_MAX"] < maxC),
                maxC,
                consolidado["NEW_MAX"],
            )
            del maxC
        except:
            enviar_mensaje("0998630405", f"{cabecera_mensaje}Sin corrección en C")

    excluidos = pd.DataFrame(columns=["BODEGA"])
    excluidos["BODEGA"] = [
        "264",
        "793",
        "273",
        "9004",
        "615",
        "9002",
        "238",
        "300",
        "254",
        "9003",
        "003",
        "522",
        "120",
        "4101",
        "4102",
        "4103",
    ]
    consolidado = consolidado[~consolidado["BODEGA"].isin(excluidos["BODEGA"])]
    consolidado = consolidado[["BODEGA", "CODIGO", "NEW_MAX"]]
    consolidado.to_csv("NUEVOS_MAXIMOS.csv", index=False)
except Exception as ex:
    # Obtener la información de la excepción y la pila de llamadas
    excepcion_info = traceback.format_exc()
    max_length = 1700
    excepcion_info = excepcion_info[:max_length]

    cabecera_mensaje = "ESTADO ACTUAL PROCESO CABEZA COLA:\n"
    enviar_mensaje(
        "0998630405",
        f"{cabecera_mensaje}Error en la cabecera del mensaje. {ex}\n\n{excepcion_info}",
    )
