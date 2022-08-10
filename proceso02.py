import pandas as pd
from modulos.db.dbconn import conectarDB
def f1(mes,anio):
    sqlQry = """SELECT * 
        FROM auditorias a INNER JOIN auditoriasprecios a3
        ON a.IdAuditoria = a3.IdAuditoria 
        WHERE SUBSTR(a.Fecha,4,7) ='{}-{}'"""

    df = pd.read_sql(sqlQry.format(mes,anio), conectarDB())

    df.to_csv('aud{}-{}.csv'.format(anio,mes))
    aud=pd.read_csv('aud{}-{}.csv'.format(anio,mes))
    cli=pd.read_csv('clientes.csv')
    rub=pd.read_csv('rubros.csv')
    eq=pd.read_csv('equivalencias.csv')
    marca=pd.read_csv('marcas.csv')
    cli=cli.drop(columns=['telefono1', 'telefono2','telefono3', 'email', 'direccion', 'Psw_Incializ', 'CtaDolar',
        'IdVendedor', 'IdCondPg', 'Bonific', 'FchCohefPrecio1', 'CohefPrecio1','NroActu_Vigente', 'FchActu_Vigente',
                    'obs_empresa', 'obs_vendedor', 'sincronizado',
        'CantLicenciasDesktop', 'CantLicenciasAndroid', 'Categoria_IVA',
        'Cond_VTA', 'Usuario_IV', 'Cuit', 'Codigo'])
    rub=rub.rename({'Nombre':'Elemento'}, axis='columns')
    rub=rub.drop(columns=['OrdenInt', 'Lst_RamosCL'])
    eq=eq.drop(columns=['IdPrefijo', 'IdSufijo', 'IV_Version', 'Baja'])
    marca=marca.drop(columns=['Grupo', 'Baja'])
    maceq= eq.merge(marca, on="IdMarca")
    maceq=maceq.rename({'Nombre':'NomMarca'}, axis='columns')
    art=pd.read_csv('articulos.csv')
    art=art.drop(columns=['AlicIVA', 'Stock', 'StEntr_Fch', 'Oferta', 'PcioOFER', 'FchaOFER',
        'IdPais', 'Baja', 'IV_Version', 'UnidadMedida', 'Presentacion'])
    art = art.rename({"Detalle":"Articulo"}, axis='columns')
    art_maceq= maceq.merge(art, on="IdItem")
    art_maceq_rub= art_maceq.merge(rub, on="IdRubro")
    aud1=aud.drop(columns=['IdTipoAuditoria', 'Tipo_Busqueda', 'Detalle','Sincronizado', 'Version', 'IdAuditoria.1', 'Sincronizado.1', 'Presupuesto', 'Codigo_Activacion'])
    aud_cli= aud1.merge(cli, on="IdCliente")
    data= aud_cli.merge(art_maceq_rub, on="IdItem")
    data=data.drop(columns=['Baja_x', 'Baja_y'])
    data=data.drop(columns=['IdAuditoria', 'IdAuditoriaPrecio'])
    data['IdVendedor'] = data['IdVendedor'].astype(str)
    data=data[(data['IdVendedor']=='nan')]
    desired_columns=['IdCliente', 'Fecha', 'IdItem', 'Reserva', 'Oferta', 'Pendiente', 'IdMarca']
    subset=data[desired_columns]
    subset['IdCliente'] = subset['IdCliente'].astype(str)
    subset['IdItem'] = subset['IdItem'].astype(str)
    subset['Pendiente'] = subset['Pendiente'].astype(str)
    subset['Reserva'] = subset['Reserva'].astype(str)
    subset['Fecha'].astype(str)
    subset[['Date','Time']] = subset.Fecha.str.split(" ",expand=True,)
    subset=subset.drop(columns=['Fecha', 'Time'])
    subset[['Dia','Mes', 'Año']] = subset.Date.str.split("-",expand=True,)
    subset=subset.drop(columns=['Date', 'Dia'])

    def func(x):
        if x ==0:
            return 1
        else:
            return 0


    subset['NO_oferta'] = subset['Oferta'].apply(func)

    def func2(y):
        if y==1:
            return 1
        else:
            return 0
    
    subset['SI_Oferta'] = subset['Oferta'].apply(func2)

    dummy_reserva=pd.get_dummies(subset['Reserva'], prefix='Reserva')
    dummy_pendiente = pd.get_dummies(subset['Pendiente'], prefix='Pendiente')

    subset = pd.concat([subset, dummy_reserva, dummy_pendiente], axis=1)
    subset = subset.drop(['Reserva', 'Oferta', 'Pendiente'], axis=1)

    subset=subset.rename({"Reserva_0":"NO_reservo"}, axis=1)
    subset=subset.rename({"Reserva_1":"SI_reservo"}, axis=1)
    subset=subset.rename({"Pendiente_0":"NO_esta_pendiente"}, axis=1)
    subset=subset.rename({"Pendiente_1":"SI_esta_pendiente"}, axis=1)

    total=subset['NO_reservo']+subset['SI_reservo']
    subset['total']=total
    subset2=subset.groupby(['IdCliente', 'IdItem', 'Mes', 'Año', 'IdMarca']).sum()
    subset2.to_csv('busqueda{}-{}.csv'.format(anio,mes))
    print(subset2)

f1('02','2020')