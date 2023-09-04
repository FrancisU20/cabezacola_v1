import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import impala.util
from impala.dbapi import connect
from datetime import datetime

pdvs=pd.DataFrame()
pdvs['BODEGA']=['173','254','161','212','065','654','494','510','289','1858','528','1826','427','044','3139','3134','037','1750','081','3147','926','464','011','184','152','671','622','392','1124','1789','1956','220','226','580','653','359','559','444','156','054','881','155','008','589','3135','3148','1044','088','1867','131','489','1981','395','759','1935','1025','682','1113','185','206','328','322','517','303','636','1020','844','372','832','400','772','396','1735','1876','393','762','247','234','302','160','354','2012','229','886','678','224','3144','153','157','189','2061','1892','434','061','202','781','1029','077','1777','277','317','391','377','715','523','841','1868','1952','777','451','787','113','806','3145','1849','137','825','1031','1015','217','670','276','871','877','3133','1003','1710','830','552','389','727','504','018','004','051','197','1764','863','190','269','048','1040','383','314','457','540','439','380','629','890','1105','309','251','1997','1108','1131','195','116','332','1757','345','791','816','573','1107','243','684','1696','101','511','305','244','525','3146','1037','419','780','343','577','1941','861','470','228','291','1739','477','820','115','486','1917','355','2058','103','3149','1130','069','1697','429','587','565','774','592','075','394','1002','1065','043','2087','1833','256','114','056','017','1809','924','200','453','1825','036','1793','1804','1695','1763','272','125','346','694','1718','402','080','079','1883','337','033','109','403','1880','352','485','390','369','1855','106','1042','060','090','321','310','461','319','1070','513','312','509','1024','561','1032','415','640','1741','401','250','842','562','213','295','199','1019','207','034','042','219','1783','1909','614','1759','582','801','353','176','164','283','068','3127','566','897','3137','521','563','713','642','811','604','445','275','013','138','366','1134','274','1052','082','339','141','418','208','010','502','744','608','273','3136','1046','3129','946','837','3151','518','760','775','527','869','850','557','3152','560','632','773','163','709','358','542','497','066','792','3150','268','894','133','1120']
List_PDV = list(pdvs['BODEGA'].unique())
anio_mes = datetime.now()
anio_mes = str(anio_mes.strftime("%Y%m"))


cnxn7 = connect('192.168.238.7',  port=21050, auth_mechanism='NOSASL', database='dwh')
cursor7 = cnxn7.cursor(user = 'ccalan')
cursor7.execute('select user();')
cursor7.execute("""
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
                        where sim.pp_idbodega in """+str(tuple(pdvs['BODEGA'].unique()))+"""
                        )
            ,stock as     (
                select bp.idbodega, bp.codigo, (bp.existencias/fp.valor_pos)  STOCK  from easygestionempresarial.tbl_bodegaproducto bp
                left join dwh.farmaproductos fp on fp.codproducto = bp.codigo
                where aniomes = '{anio_mes}' and bp.idbodega in """+str(tuple(pdvs['BODEGA'].unique()))+"""
                )
            ,bloqueoproducto as (
                        select bpf_codigo_producto CODIGO, bpf_codigo_bodega idbodega, bpf_usuario USUARIO_BLOQUEO_SC, bpf_observacion OBSERVACION_BLOQ	  from easygestionempresarial.tbl_bloqueoproductosfarmacias
                        where bpf_estado = 'A' and bpf_codigo_bodega in """+str(tuple(pdvs['BODEGA'].unique()))+"""
                        )
            ,bloqueoabastecimiento as     (
                                select aba_codigo_producto CODIGO, aba_oficina idbodega, aba_usuario_creacion USUARIO_BLOQUEO_PDV, aba_motivo_bloqueo MOTIVO_BLOQUEO_PDV  from easygestionempresarial.tbl_articulobloqueoabastecimiento
                                where aba_estado = 'ACTIVO' and aba_oficina in """+str(tuple(pdvs['BODEGA'].unique()))+"""
                                )
            ,siembra as (
                select si_idsiembra BODEGA,si_codigo codigo, si_cantidad SIEMBRA,si_detalle MOTIVO_SIEMBRA, si_usuario_ingreso USUARIO_SIEMBRA from easygestionempresarial.tbl_siembrainventario
                where si_estado = 'A' and si_idsiembra in """+str(tuple(pdvs['BODEGA'].unique()))+"""
                )
            ,siembrapdv as (
                select spv_idoficina BODEGA, spv_codigo codigo, spv_cantidad siembra_pdv from easygestionempresarial.tbl_siembrainventario_pv 
                where spv_estado = 'A' and spv_idoficina in """+str(tuple(pdvs['BODEGA'].unique()))+"""
                ) 
            ,alpha as (
                select oa_oficina BODEGA FROM easygestionempresarial.tbl_oficinaalpha
                where oa_enproduccion = 'SI' and oa_oficina in """+str(tuple(pdvs['BODEGA'].unique()))+"""
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
where sim.BODEGA in """+str(tuple(pdvs['BODEGA'].unique()))+"""                
                """)

ArchivoDeClasificacion = impala.util.as_pandas(cursor7)
cnxn7.close()

ArchivoDeClasificacion.rename(columns={"bodega":"BODEGA","codigo":"CODIGO","categoria":"CATEGORIA","promedio":"PROMEDIO","dia_min":"DIA_MIN","dia_max":"DIA_MAX","minimo":"MINIMO","maximo":"MAXIMO","stock":"STOCK","semana_no_vendida":"SEMANA_NO_VENDIDA","usuario_bloqueo_sc":"USUARIO_BLOQUEO_SC","observacion_bloq":"OBSERVACION_BLOQ", "usuario_bloqueo_pdv":"USUARIO_BLOQUEO_PDV","motivo_bloqueo_pdv":"MOTIVO_BLOQUEO_PDV","siembra":"SIEMBRA","motivo_siembra":"MOTIVO_SIEMBRA","usuario_siembra":"USUARIO_SIEMBRA","siembra_pdv":"SIEMBRA_PDV"},inplace=True)#"usuario_siembra_modf":"USUARIO_SIEMBRA_MODF",
ArchivoDeClasificacion=ArchivoDeClasificacion.drop_duplicates(subset=['BODEGA', 'CODIGO'])

print('UNIENDO EL NOMBRE DE LA FARMACIA')
cnxn7 = connect('192.168.238.7',  port=21050, auth_mechanism='NOSASL', database='dwh')
cursor7 = cnxn7.cursor(user = 'ccalan')
cursor7.execute('select user();')
cursor7.execute("""select codfarmacia,farmacia from dwh.farmafarmacia """)
farmafarmacia = impala.util.as_pandas(cursor7)
cnxn7.close()
farmafarmacia=farmafarmacia.drop_duplicates()
farmafarmacia=farmafarmacia.rename(columns={'codfarmacia':'BODEGA', 'farmacia':'FARMACIA'})


ArchivoDeClasificacion=pd.merge(ArchivoDeClasificacion,farmafarmacia, how= 'left', on=['BODEGA'])
ArchivoDeClasificacion=ArchivoDeClasificacion[['BODEGA', 'FARMACIA', 'CODIGO', 'CATEGORIA', 'PROMEDIO','DIA_MIN','DIA_MAX','MINIMO','MAXIMO','STOCK','SEMANA_NO_VENDIDA','USUARIO_BLOQUEO_SC','OBSERVACION_BLOQ','USUARIO_BLOQUEO_PDV','MOTIVO_BLOQUEO_PDV','SIEMBRA','MOTIVO_SIEMBRA','USUARIO_SIEMBRA','SIEMBRA_PDV']] #'USUARIO_SIEMBRA_MODF',
ArchivoDeClasificacion["SEMANA_NO_VENDIDA"]=ArchivoDeClasificacion["SEMANA_NO_VENDIDA"].astype(int)
ArchivoDeClasificacion['CATEGORIA']=ArchivoDeClasificacion['CATEGORIA'].fillna('X')
ArchivoDeClasificacion=ArchivoDeClasificacion[ArchivoDeClasificacion['CATEGORIA']!='X']
ArchivoDeClasificacion['OBS'] = np.where((ArchivoDeClasificacion['PROMEDIO']==0)&(ArchivoDeClasificacion['CATEGORIA']=='E'),'FN','PS')
ArchivoDeClasificacion = ArchivoDeClasificacion[ArchivoDeClasificacion['OBS']!='FN']
ArchivoDeClasificacion['OBS'] = np.where((ArchivoDeClasificacion['CATEGORIA']=='D')|(ArchivoDeClasificacion['CATEGORIA']=='N')|(ArchivoDeClasificacion['CATEGORIA']=='Z'),'ELIMINAR','NO')
ArchivoDeClasificacion = ArchivoDeClasificacion[ArchivoDeClasificacion['OBS']!='ELIMINAR']
del ArchivoDeClasificacion['OBS']

list_catalogo = []
for dirpath, dirnames, filenames in os.walk(r'C:\OneD\BI\OneDrive - Farmaenlace\CATÁLOGO BODEGA CENTRAL'):
    for filename in filenames:
        list_catalogo.append(os.path.join(dirpath, filename))
list_catalogo = list_catalogo[-1]
try:
    CatalogoActualizado = pd.read_excel(list_catalogo, skiprows=4, converters={'Cód. Art.':str},parse_dates=['fechaIngreso'])
except FileNotFoundError: print("El archivo no existe / No está en la ubicación correcta. ",list_catalogo)
except PermissionError: print("No tienes permiso para leer este archivo. ",list_catalogo)
except UnicodeDecodeError: print("El archivo no está en el formato correcto. ",list_catalogo)
except IsADirectoryError: print("Estás intentando leer un directorio en lugar de un archivo. ",list_catalogo)
except IOError: print("No se pudo leer el archivo debido a un problema de E/S. ",list_catalogo)
print('Ruta CATÁLOGO leído: ',list_catalogo)

del dirnames, dirpath, filename, filenames,list_catalogo
CatalogoActualizado = CatalogoActualizado.rename(columns={'Cód. Art.':'CODIGO','Artículo':'DESCRIPCION','Categ.':'CatBC','Stock BC':'STOCK_BC','estadoBC':'Estado_BC'})
CatalogoActualizado['UBI'] = CatalogoActualizado.loc[:,'UBICACION'].str[0:2]
CatalogoActualizado = CatalogoActualizado[['CODIGO', 'CatBC','Nivel1','UBI','PPP']]
ArchivoDeClasificacion= ArchivoDeClasificacion.merge(CatalogoActualizado, how="left")
ArchivoDeClasificacion = ArchivoDeClasificacion[(ArchivoDeClasificacion['CATEGORIA']!='Z') & (ArchivoDeClasificacion['CATEGORIA']!='N')&(ArchivoDeClasificacion['CATEGORIA']!='D')&(ArchivoDeClasificacion['Nivel1']!='SERVICIOS')&(ArchivoDeClasificacion['Nivel1']!='SUMINISTROS')&(ArchivoDeClasificacion['UBI']!='Z-')&(ArchivoDeClasificacion['CatBC']!='Z')&(ArchivoDeClasificacion['CatBC']!='D')]

ArchivoDeClasificacion1 = ArchivoDeClasificacion[['BODEGA', 'CODIGO', 'CATEGORIA', 'MINIMO', 'MAXIMO', 'DIA_MIN','DIA_MAX', 'PROMEDIO', 'SIEMBRA','CatBC', 'Nivel1', 'UBI', 'PPP']]
List_PDV=['173','161','212','065','654','842','562','213','295','199','2078','3157','751', '848','849'] #Farmacias Piloto
consolidado=pd.DataFrame(columns=(['BODEGA', 'CODIGO', 'CATEGORIA', 'MINIMO', 'MAXIMO', 'DIA_MIN',
       'DIA_MAX', 'PROMEDIO', 'SIEMBRA','CatBC', 'Nivel1', 'UBI', 'PPP', 'Brecha',
       'Desvest', 'NumGrupos', 'NumSKU', 'ReglaScott', 'Validación', 'GRUPO',
       'NEW_MAX','ACCION']))
DataReglaScott = pd.DataFrame()
for PDV in tqdm(List_PDV):
    PDVAux = ArchivoDeClasificacion1[ArchivoDeClasificacion1['BODEGA']==PDV]
    List_CAT =list(PDVAux['CATEGORIA'].unique())
    List_CAT = sorted(List_CAT)
    E = List_CAT.pop(3)
    List_CAT.insert(0, E)
    for CAT in List_CAT:
        PDVCatAux = PDVAux[PDVAux['CATEGORIA']==CAT]
        Brecha = PDVCatAux['MAXIMO'].max() - PDVCatAux['MAXIMO'].min()
        Desvest = round(np.std(PDVCatAux['MAXIMO']),0)
        NumSKU = len(PDVCatAux['MAXIMO'])
        ReglaScott = (3.5 * np.std(PDVCatAux['MAXIMO'])) / (NumSKU ** (1/3))
        NumGrupos = round(Brecha / ReglaScott, 0) if (Brecha != 0) and (ReglaScott != 0) else 0
        data = {
                "BODEGA": PDV,
                "CATEGORIA": CAT,
                "NumSKU": NumSKU,
                "Brecha": Brecha,
                "Desvest": Desvest,
                "ReglaScott": ReglaScott,
                "NumGrupos": NumGrupos
                }
        DataReglaScott = DataReglaScott._append(data, ignore_index=True)
    #ANÁLISIS
    CabCol = [(DataReglaScott['ReglaScott']>=1)&(DataReglaScott['NumSKU']>1),
             (DataReglaScott['ReglaScott']>=0.5)&(DataReglaScott['ReglaScott']<1)&(DataReglaScott['NumSKU']>1),
             (DataReglaScott['ReglaScott']>0)&(DataReglaScott['ReglaScott']<0.5)&(DataReglaScott['NumSKU']>1)&(DataReglaScott['Brecha']>1)]
    etiquetas = ['SuperCabezaCola','SuperCabeza y 50% Cola > Prom (+1)','Todo a la media']
    DataReglaScott['Validación'] = np.select(CabCol,etiquetas,default='No aplica')  
    PDVAux = PDVAux.merge(DataReglaScott, how='left', on=['BODEGA','CATEGORIA'])
    del data,NumGrupos,ReglaScott,NumSKU,Desvest,Brecha,PDVCatAux,E,CabCol, etiquetas
    parametros=DataReglaScott[(DataReglaScott['Validación']!='No aplica')&(DataReglaScott['BODEGA']==PDV)]
    for i in parametros['Validación'].unique():
        if i=='SuperCabezaCola':
            data=PDVAux[PDVAux['Validación']==i]
            for j in data['CATEGORIA'].unique():
                if j=='E':
                    data1=data[data['CATEGORIA']=='E']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='E']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cola=data1[data1['GRUPO']==1]
                    media_cola=round(media_cola[['MAXIMO']].mean(),0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==1)&(data1['NEW_MAX']<=media_cola.iloc[0]),media_cola.iloc[0],data1['NEW_MAX'])
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                if j=='A':
                    data1=data[data['CATEGORIA']=='A']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='A']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cola=data1[data1['GRUPO']==1]
                    media_cola=round(media_cola[['MAXIMO']].mean(),0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==1)&(data1['NEW_MAX']<=media_cola.iloc[0]),media_cola.iloc[0],data1['NEW_MAX'])
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)

                if j=='B':    
                    data1=data[data['CATEGORIA']=='B']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='B']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cola=data1[data1['GRUPO']==1]
                    media_cola=round(media_cola[['MAXIMO']].mean(),0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==1)&(data1['NEW_MAX']<=media_cola.iloc[0]),media_cola.iloc[0],data1['NEW_MAX'])
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)

        if i=='SuperCabeza y 50% Cola > Prom (+1)':
            data=PDVAux[PDVAux['Validación']==i]
            for j in data['CATEGORIA'].unique():
                if j=='E':    
                    data1=data[data['CATEGORIA']=='E']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='E']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1 = data1.sort_values(by=['PROMEDIO'],ascending=False)
                    data1y=data1[data1['GRUPO']==0]
                    data1x=data1[data1['GRUPO']==1]
                    data1xx=data1x[data1x['MAXIMO']==min(data1x['MAXIMO'])]
                    data1xy=data1x[data1x['MAXIMO']!=min(data1x['MAXIMO'])]
                    del data1x
                    for k in range(int(round((data1xx['CODIGO'].count()/2),0))):
                        data1xx.iloc[k,19]=data1xx.iloc[k,19]+1                    
                    data1=pd.concat([data1xx,data1xy,data1y], ignore_index=False)
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                    
                if j=='A':    
                    data1=data[data['CATEGORIA']=='A']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='A']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1 = data1.sort_values(by=['PROMEDIO'],ascending=False)
                    data1y=data1[data1['GRUPO']==0]
                    data1x=data1[data1['GRUPO']==1]
                    data1xx=data1x[data1x['MAXIMO']==min(data1x['MAXIMO'])]
                    data1xy=data1x[data1x['MAXIMO']!=min(data1x['MAXIMO'])]
                    del data1x
                    for k in range(int(round((data1xx['CODIGO'].count()/2),0))):
                        data1xx.iloc[k,19]=data1xx.iloc[k,19]+1                    
                    data1=pd.concat([data1xx,data1xy,data1y], ignore_index=False)
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                    
                if j=='B':    
                    data1=data[data['CATEGORIA']=='B']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='B']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1 = data1.sort_values(by=['PROMEDIO'],ascending=False)
                    data1y=data1[data1['GRUPO']==0]
                    data1x=data1[data1['GRUPO']==1]
                    data1xx=data1x[data1x['MAXIMO']==min(data1x['MAXIMO'])]
                    data1xy=data1x[data1x['MAXIMO']!=min(data1x['MAXIMO'])]
                    del data1x
                    for k in range(int(round((data1xx['CODIGO'].count()/2),0))):
                        data1xx.iloc[k,19]=data1xx.iloc[k,19]+1                    
                    data1=pd.concat([data1xx,data1xy,data1y], ignore_index=False)
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                    
                if j=='C':    
                    data1=data[data['CATEGORIA']=='C']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='C']
                    ancho_grupo=round(ancho_grupo.iloc[0,6])
                    data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)
                    media_cabeza=data1[data1['GRUPO']==0]
                    media_cabeza=round(media_cabeza[['MAXIMO']].mean(),0)
                    data1['NEW_MAX'] = np.where((data1['GRUPO']==0)&(data1['MAXIMO']>=media_cabeza.iloc[0]),media_cabeza.iloc[0],data1['MAXIMO'])
                    data1 = data1.sort_values(by=['PROMEDIO'],ascending=False)
                    data1y=data1[data1['GRUPO']==0]
                    data1x=data1[data1['GRUPO']==1]
                    data1xx=data1x[data1x['MAXIMO']==min(data1x['MAXIMO'])]
                    data1xy=data1x[data1x['MAXIMO']!=min(data1x['MAXIMO'])]
                    del data1x
                    for k in range(int(round((data1xx['CODIGO'].count()/2),0))):
                        data1xx.iloc[k,19]=data1xx.iloc[k,19]+1                    
                    data1=pd.concat([data1xx,data1xy,data1y], ignore_index=False)
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)

        if i=='Todo a la media':
            data=PDVAux[PDVAux['Validación']==i]
            for j in data['CATEGORIA'].unique():
                if j=='E':
                    data1=data[data['CATEGORIA']=='E']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='E'] 
                    #data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1['NEW_MAX'] = np.where((data1['MAXIMO']<=int(round(data1[['MAXIMO']].mean(),0))),int(round(data1[['MAXIMO']].mean(),0)),data1['MAXIMO'])
    
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)

                if j=='A':
                    data1=data[data['CATEGORIA']=='A']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='A'] 
                    #data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1['NEW_MAX'] = np.where((data1['MAXIMO']<=int(round(data1[['MAXIMO']].mean(),0))),int(round(data1[['MAXIMO']].mean(),0)),data1['MAXIMO'])
    
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)

                if j=='B':
                    data1=data[data['CATEGORIA']=='B']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='B'] 
                    #data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1['NEW_MAX'] = np.where((data1['MAXIMO']<=int(round(data1[['MAXIMO']].mean(),0))),int(round(data1[['MAXIMO']].mean(),0)),data1['MAXIMO'])
    
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                
                if j=='C':
                    data1=data[data['CATEGORIA']=='C']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='C'] 
                    data1['NEW_MAX'] = np.where((data1['MAXIMO']<=int(round(data1[['MAXIMO']].mean(),0))),int(round(data1[['MAXIMO']].mean(),0)),data1['MAXIMO'])
    
                    #data1['NEW_MAX'] = int(math.ceil(data1[['MAXIMO']].mean()))
                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                
                if j=='L':
                    data1=data[data['CATEGORIA']=='L']
                    ancho_grupo=parametros[parametros['CATEGORIA']=='L']
                    #data1['NEW_MAX'] = int(round(data1[['MAXIMO']].mean(),0))
                    data1['NEW_MAX'] = np.where((data1['MAXIMO']<=int(round(data1[['MAXIMO']].mean(),0))),int(round(data1[['MAXIMO']].mean(),0)),data1['MAXIMO'])
                    #data1['GRUPO'] = np.where((data1['MAXIMO']<=min(data1['MAXIMO'])+ancho_grupo),1,0)

                    data1['ACCION']=i
                    consolidado=pd.concat([consolidado,data1], ignore_index=False)
                    
    categorias_modificadas=parametros['CATEGORIA'].unique()                
    try:
        min_maxE=consolidado[(consolidado['BODEGA']==PDV)&(consolidado['CATEGORIA']=='E')]
        maxE=int(max(min_maxE['NEW_MAX']))
        minE=int(min(min_maxE['NEW_MAX']))
    except:
        print('Sin categoria E')

    try:
        min_maxA=consolidado[(consolidado['BODEGA']==PDV)&(consolidado['CATEGORIA']=='A')]
        maxA=int(max(min_maxA['NEW_MAX']))
        minA=int(min(min_maxA['NEW_MAX']))
    
    except:
        print('Sin categoria A')
     
    try:
        min_maxB=consolidado[(consolidado['BODEGA']==PDV)&(consolidado['CATEGORIA']=='B')]
        maxB=int(max(min_maxB['NEW_MAX']))
        minB=int(min(min_maxB['NEW_MAX']))
        
    except:
        print('Sin categoria B')     
        
    try:
        min_maxC=consolidado[(consolidado['BODEGA']==PDV)&(consolidado['CATEGORIA']=='C')]
        maxC=int(max(min_maxC['NEW_MAX']))
        minC=int(min(min_maxC['NEW_MAX']))
        
    except:
        print('Sin categoria C')     
    
        
    try:
        consolidado['NEW_MAX'] = np.where((consolidado['CATEGORIA']=='E')&(consolidado['BODEGA']==PDV)&(consolidado['NEW_MAX']<maxA),maxA,consolidado['NEW_MAX'])
        del maxA
    except:
        print('Sin correción en A')     
    
    try:
        consolidado['NEW_MAX'] = np.where((consolidado['CATEGORIA']=='A')&(consolidado['BODEGA']==PDV)&(consolidado['NEW_MAX']<maxB),maxB,consolidado['NEW_MAX'])
        del maxB
    except:
        print('Sin corrección en B')     
        
    try:
        consolidado['NEW_MAX'] = np.where((consolidado['CATEGORIA']=='B')&(consolidado['BODEGA']==PDV)&(consolidado['NEW_MAX']<maxC),maxC,consolidado['NEW_MAX'])
        del maxC
    except:
        print('Sin corrección en C')     
    
excluidos=pd.DataFrame(columns=['BODEGA']) 
excluidos['BODEGA']=['264','793','273','9004','615','9002','238','300','254','9003','003','522','120','4101','4102','4103']
consolidado=consolidado[~consolidado['BODEGA'].isin(excluidos['BODEGA'])]
consolidado=consolidado[['BODEGA','CODIGO','NEW_MAX']]
consolidado.to_csv('NUEVOS_MAXIMOS.csv',index=False)