import pandas as pd 


def read_csv_file(file_name) -> pd.DataFrame:
    """
        Read a csv file using pandas
        :param file_name: File name
        return: Un dataframe pandas
    """
    df = pd.read_csv(f'./sqlite_exports/{file_name}.csv', low_memory=False)
    if 'index' in df.columns:
        df = df.drop(columns=['index'])

    print(f"====== Statistics on {file_name} =======")
    print(f"Dimension {df.shape}\n\n")

    return df


def inspecter_data(file_name: str, df: pd.DataFrame, head=5) -> None:
    """
    Docstring for inspecter_data
    
    :param df: Le dataframe a inspecter
    :param head: Le nombre de lignes a afficher
    """
    df = df.copy()

    print(f"=============== Inspecter les donnees de {file_name}  =================")
    print(f"La dimension du dataframe est : {df.shape}")
    print(f"Les types de donnees sont : {df.dtypes}")
    print(f"Les {head} premieres lignes du dataframe sont : ")
    print(df.head(head))

    print(f"Le nombre de valeurs manquantes par colonne est : ")
    print(df.isnull().sum())
    print(f"=============== Fin de l'inspection des donnees sur {file_name} =================\n\n")


def extract_sources() -> dict[str, pd.DataFrame]:
    """
    Docstring for extract_sources_data
    Charger les donnees sources a partir des fichiers csv et les stocker
    dans un dictionnaire de dataframe
    """
    # La liste des sources
    fichiers = [
        'customers','orders','order_pymts',
        'products','geoloc','order_items',
        'order_reviews','sellers','translation',
    ]
    
    # Cree un dict vide
    data_dict = {}

    for file in fichiers:
        data_dict[file] = read_csv_file(file)

    return data_dict


def parser_date(X) -> pd.Series:
    """
    Docstring for parser_date
    Pour parser une colonne entiere de date
    
    :param X: La colonne de date a parser
    :param date_format: Le format de la date a parser
    return: La date au format datetime de pandas
    """
    # TODO: La necessite de la fonction
    return pd.to_datetime(X, errors='coerce')


def parser_date_columns(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    '''
    Docstring for parser_date_columns 
    Parser les colonnes de date dans les dataframes du dictionnaire

    :param dfs: Le dictionnaire de dataframes
    :return: Le dictionnaire de dataframes avec les colonnes de date parsees
    '''
    # Convertir les colonnes de date en format datetime
    cols_date = {
        'order_items': ['shipping_limit_date'],
        'orders': ['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date'],
        'order_reviews': ['review_creation_date','review_answer_timestamp'],
    }

    # Parcourrir les colonnes et dans les tables
    for table_name, date_cols in cols_date.items():
        if table_name in dfs:
            df = dfs[table_name]
            for col in date_cols:
                if col in df.columns:
                    df[col] = parser_date(df[col])
            dfs[table_name] = df
    
    return dfs


def detecter_et_supprimer_doublons(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Docstring for detecter_et_supprimer_doublons
    Detecter et supprimer les doublons dans un dataframe
    :param df: Le dataframe a traiter
    :param table_name: Le nom de la table
    return: Le dataframe sans doublons
    """
    # Detecter les doublons
    doublons = df.duplicated()
    nb_doublons = doublons.sum()
    percent = (nb_doublons / df.size) * 100
    print(f"Nombre de doublons dans la table {table_name} : {nb_doublons} ({percent:.2f}%).")

    # Supprimer les doublons
    if nb_doublons > 0:
        print(f"Dimension avant suppression des doublons dans la table {table_name} : {df.shape}")
        df = df.drop_duplicates()
        print(f"Doublons supprimes dans la table {table_name}. Nouvelle dimension : {df.shape}\n\n")
    
    return df


def nbre_nan_pourcentage(df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    '''
    Docstring for nbre_nan_pourcentage
    Afficher le nombre et le pourcentage de valeurs manquantes dans un dataframe
    
    :param df: Le dataframe a traiter
    :type df: pd.DataFrame
    :return: Le dataframe avec une colonne de pourcentage de valeurs manquantes
    :rtype: DataFrame
    '''
    nb_nan = df.isna().sum().sum()
    pourcentage_nan = (nb_nan / df.size) * 100

    print(f"Nombre de valeurs manquantes : {nb_nan} ({pourcentage_nan:.2f}%)")

    return df, pourcentage_nan


def supprimer_colonnes_inutiles(df: pd.DataFrame, table_name) -> pd.DataFrame:
    """
    Docstring for supprimer_colonnes_inutiles
    Supprimer les colonnes inutiles dans un dataframe
    :param df: Le dataframe a traiter
    :param table_name: Le nom de la table
    return: Le dataframe sans les colonnes inutiles
    """
    # Colonnes textuelles lourdes non utilisees dans les agregations du TP
    colonnes_a_supprimer = {
        'order_reviews': ['review_comment_title', 'review_comment_message'],
    }

    cols = colonnes_a_supprimer.get(table_name, ())
    if cols:
        cols_existantes = [col for col in cols if col in df.columns]
        if cols_existantes:
            df = df.drop(columns=cols_existantes)
            print(
                f"Colonnes supprimees dans la table {table_name}: "
                f"{', '.join(cols_existantes)}"
            )

    return df


def traiter_nan_products(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Docstring for traiter_nan_products
    
    :param df: Le dataframe products a traiter
    :type df: pd.DataFrame
    :return: Le dataframe products avec les valeurs manquantes geres
    :rtype: DataFrame
    '''
    df, percent = nbre_nan_pourcentage(df)

    # Categories manquantes -> 'Unknown'
    if 'product_category_name' in df.columns:
        # Nombre de val manquantes dans la colonne product_category_name
        nb_nan_col_name = df['product_category_name'].isna().sum()

        df['product_category_name'] = df['product_category_name'].fillna('Inconnu')
        print(f"Remplacement de {nb_nan_col_name} categories manquantes par 'Inconnu'.\n\n")

    cols_zero = ['product_description_lenght', 'product_photos_qty']
    for col in cols_zero:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    print("--- Analyse products (Apres) ---")
    df_temp, percent = nbre_nan_pourcentage(df)
    print("Suppression en cours...\n\n")
    # Supprimer les autres nan dans la table
    df = df.dropna()
    
    return df


def gerer_valeurs_manquantes(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Docstring for gerer_valeurs_manquantes
    Gerer les valeurs manquantes dans un dataframe
    
    :param df: Le dataframe a traiter
    :param table_name: Le nom de la table
    return: Le dataframe avec les valeurs manquantes geres
    """
    print(f"--- Traitement des NaN pour {table_name} ---")
    
    if table_name == 'products':
        df = traiter_nan_products(df)

    elif table_name == 'orders':
        print("Aucune transformation utile pour la table orders.")
        # explication dans le rapport
        return df

    return df


def analyser_qualite_donnees(data: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Analyse la qualite des donnees et les relations entre tables
    
    :param data: Le dictionnaire de DataFrame netoye
    :type data: Dictionnaire
    :return: Un type de dataframe
    """
    print("\n" + "="*60)
    print("ANALYSE DE LA QUALITE DES DONNEES")
    print("="*60)
    
    # Analyse relation commandes avec les clients
    print("\n--- RELATION COMMANDES - CLIENTS ---")
    df_orders_customers = analyser_commandes_clients(
        data['orders'], data['customers']
    )
    
    # Analyse relation commandes-paiements
    print("\n--- RELATION COMMANDES - PAIEMENTS ---")
    df_orders_payments, df_missing_payment = analyser_commandes_paiements(
        data['orders'], data['order_pymts']
    )
    
    return df_orders_customers, df_orders_payments, df_missing_payment


def analyser_commandes_clients(orders: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse la relation entre les commandes et les clients
    
    :param orders: La table orders
    :param customers: La table customers
    """
    print(f"Commandes: {orders.shape}, Clients: {customers.shape}")
    
    # Jointure
    df_merged = pd.merge(orders, customers, on='customer_id', how='outer', indicator=True)
    
    print(f"Apres jointure: {df_merged.shape}")
    print("\nRepartition des jointures :")
    print(df_merged['_merge'].value_counts())
    
    # Analyses sur la table merge
    left_only = df_merged[df_merged['_merge'] == 'left_only']
    right_only = df_merged[df_merged['_merge'] == 'right_only']
    
    if len(left_only) > 0:
        print(f"\n{len(left_only)} commandes sans client correspondant")
        print(left_only[['order_id', 'customer_id']].head())
    else:
        print("Il n'y a aucune commande sans client")

    if len(right_only) > 0:
        print(f"\n{len(right_only)} clients sans commande !")
    else:
        print("Il n'y aucun client client sans commande !")
    
    return df_merged


def analyser_commandes_paiements(orders: pd.DataFrame, payments: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyse la relation entre les commandes et les paiements
    
    :param orders: la table orders
    "param payments: la table order_pymts
    """
    print(f"Commandes: {orders.shape}, Paiements: {payments.shape}")
    
    # Jointure 
    df_merged = pd.merge(
        orders, 
        payments,
        left_on='order_id', 
        right_on='order_id', 
        how='outer', 
        indicator=True
    )
    
    print(f"\nApres jointure: {df_merged.shape}")
    print("\nRepartition des jointures:")
    print(df_merged['_merge'].value_counts())
    
    # Identifier les commandes sans paiement
    df_missing_payment = df_merged[df_merged['_merge'] == 'left_only']
    
    if len(df_missing_payment) > 0:
        print(f"\n{len(df_missing_payment)} commandes sans paiement")
    
    return df_merged, df_missing_payment


def create_fact_order_items_table(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Cree la table de faits en joignant order_items avec les dimensions

    :param data: Le dictionnaire
    :return: Un dictionnaire
    """
    print("\n" + "="*50)
    print("TRANSFORMATION: Creation de la table de faits")
    print("="*50)
    
    
    # Analyse sur les donnees
    df_orders_customers, df_orders_payments, df_missing = analyser_qualite_donnees(data)
    
    # Configuration des jointures
    jointures = [
        ('orders', 'order_id'),
        ('customers', 'customer_id'),
        ('sellers', 'seller_id'),
        ('products', 'product_id')
    ]
    
    # Verification des tables dans le dict
    tables_requises = ['order_items'] + [j[0] for j in jointures]
    
    missing = [t for t in tables_requises if t not in data]
    if missing:
        print(f"ERREUR: Tables manquantes: {missing}")
        return data
    
    # Construction
    fact = data['order_items'].copy()
    print(f"\nDimension initiale order_items: {fact.shape}")
    
    for table, key in jointures:
        print(f"Jointure avec {table}...")
        fact = fact.merge(data[table], on=key, how='outer')
    
    # Calculs
    print("\nCALCULS DES METRIQUES")
    fact['item_total'] = fact['price'] + fact['freight_value']
    fact['year_month'] = fact['order_purchase_timestamp'].dt.to_period('M').astype(str)
    
    if 'order_delivered_customer_date' in fact.columns:
        fact['delivery_days'] = (
            fact['order_delivered_customer_date'] - fact['order_purchase_timestamp']
        ).dt.days
        
        print(f"\nStatistiques de livraison:")
        print(f"Moyenne: {fact['delivery_days'].mean():.1f} jours")
        print(f"Mediane: {fact['delivery_days'].median():.1f} jours")
        print(f"Min: {fact['delivery_days'].min():.1f} jours")
        print(f"Max: {fact['delivery_days'].max():.1f} jours")
    
    # Ajouter cette table de fait dans le dictionnaire
    data['fact_order_items'] = fact
    print(f"\nTable de faits finale: {fact.shape}")
    
    # Ajouter les autres tables
    data['orders_customers'] = df_orders_customers
    data['orders_payments'] = df_orders_payments
    data['missing_payments'] = df_missing
    
    return data


def create_fact_customers_geoloc_table(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    '''
    Docstring for create_fact_customers_geoloc_table
    Cree une table de faits  clients - localisation
    
    :param data: Le dictionnaire netoye
    :type data: dict[str, pd.DataFrame]
    :return: Le dictionnaire contenant le jeu de faits
    :rtype: dict[str, DataFrame]
    '''
    print("\n" + "="*50)
    print("TRANSFORMATION: Creation de la table de faits customers - geoloc")
    print("="*50)
    
    # Verification des tables necessaires
    required_tables = ['customers', 'geoloc']
    for table in required_tables:
        if table not in data:
            print(f"ERREUR: Table {table} manquante!")
            return data
    
    # Copie des dataframes
    customers = data['customers'].copy()
    geoloc = data['geoloc'].copy()
    
    print(f"\nDimensions initiales:")
    print(f"  customers: {customers.shape}")
    print(f"  geoloc: {geoloc.shape}")
    
    # ANALYSE DES CODES POSTAUX
    print("\nANALYSE DES CODES POSTAUX")
    
    if 'customer_zip_code_prefix' in customers or 'geolocation_zip_code_prefix' in geoloc:
        # Extraire les codes postaux uniques des clients
        customer_zip = customers['customer_zip_code_prefix'].unique()
        print(f"Codes postaux clients: {len(customer_zip)} uniques")
        
        # Codes postaux disponibles dans geoloc
        geoloc_zip = geoloc['geolocation_zip_code_prefix'].unique()
        print(f"Codes postaux geoloc: {len(geoloc_zip)} uniques")
    else:
        print("La colonne geolocation_zip_code_prefix n'existe pas a la fois dans les deux tables")
        return data
    
    # Codes postaux clients non trouves dans geoloc
    missing_zips = set(customer_zip) - set(geoloc_zip)
    if missing_zips:
        print(f"\n{len(missing_zips)} codes postaux clients non trouves dans geoloc")
    
    # JOINTURE
    print("\nCREATION DE LA TABLE DE FAITS")
    
    # Jointure customers avec geoloc
    fact = pd.merge(
        customers,
        geoloc,
        left_on='customer_zip_code_prefix',
        right_on='geolocation_zip_code_prefix',
        how='left',
        indicator=True
    )
    
    print(f"Dimension apres jointure: {fact.shape}")
    print("\nRepartition des jointures:")
    print(fact['_merge'].value_counts())
    
    # ANALYSE DES CLIENTS SANS GEOLOC
    missing_geoloc = fact[fact['_merge'] == 'left_only']
    if len(missing_geoloc) > 0:
        print(f"\n{len(missing_geoloc)} clients sans correspondance geoloc")
        print(f"Soit {len(missing_geoloc)/len(fact)*100:.1f}% des clients")
    
    # VERIFICATIONS FINALES
    print("\nVERIFICATIONS FINALES")
    print(f"Dimension finale: {fact.shape}")
    print(f"Colonnes: {fact.columns.tolist()}")
    print(f"\nApercu des donnees:")
    
    
    # Stocker dans le dictionnaire
    data['fact_customers_geoloc'] = fact
    
    print(f"\nTable de faits customers-geoloc creee avec succes!")
    
    return data

def transform_data(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Docstring for transform
    Transformer les donnees sources pour les rendre plus propres
    """

    # Transformer les colonnes de date en format datetime
    dfs = parser_date_columns(dfs)

    # Supprimer les doublons dans les tables
    for table_name, df in dfs.items():
        dfs[table_name] = detecter_et_supprimer_doublons(df, table_name)

    # Suppression des colonnes inutiles
    for table_name, df in dfs.items():
        dfs[table_name] = supprimer_colonnes_inutiles(df, table_name)

    # Gerer les valeurs manquantes
    for table_name, df in dfs.items():
        if df.isna().sum().sum() > 0:
            # Gerer les valeurs manquantes
            dfs[table_name] = gerer_valeurs_manquantes(df, table_name)
    
    # Creer et ajouter le jeu de faits
    dfs = create_fact_order_items_table(data=dfs)
    dfs = create_fact_customers_geoloc_table(data=dfs)

    return dfs


def afficher_menu():
    '''
    Docstring for afficher_menu
        Afficher le menu du projet
    '''
    print("\n=== MENU ETL - Projet 1 ===")
    print("1. Charger les donnees (Extract)")
    print("2. Inspecter tous les tables de donnees")
    print("3. Inspecter une table specifique")
    print("4. Transformer les donnees (Transform)")
    print("5. Charger les donnees transformees (Load CSV/SQLite)")
    print("0. Quitter")

def main():
    dfs = {} # Le dictionnaire pour stocker tous les df
    final_tables = {} # Pour stocker les df transformer

    while True:
        afficher_menu()

        choix = input("Votre choix : ")

        if choix == '1':
            # Extraire les donnees sources
            dfs = extract_sources()
            print("Donnees chargees avec succes.")
        
        elif choix == '2':
            # Verifier que les donnees sont chargees
            if not dfs:
                print("Erreur : Chargez d'abord les donnees (Option 1).")
            else:
                for nom_table, df in dfs.items():
                    inspecter_data(nom_table, df)

        elif choix == '3':
            # Verifier que les donnees sont chargees
            if not dfs:
                print("Erreur : Chargez d'abord les donnees (Option 1).")
            else:
                nom_table = input("Quelle table inspecter ? (ex: orders) : ")
                if nom_table in dfs:
                    inspecter_data(nom_table, dfs[nom_table])
                else:
                    print("Table introuvable.")

        elif choix == '4':
            # Verifier que les donnees sont chargees
            if not dfs:
                print("Erreur : Chargez d'abord les donnees.")
            else:
                # Tranformer les donnees
                final_tables = transform_data(dfs)
                print("Transformation terminee.")

        elif choix == '5':
            if not final_tables:
                print("Erreur : Transformez d'abord les donnees (Option 4).")
            else:
                pass

        elif choix == '0':
            break


if __name__ == "__main__":
    main()
