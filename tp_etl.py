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
    return: Le dictionnaire de dataframes avec les colonnes de date parsees
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
    print("\n--- RELATION COMMANDES-CLIENTS ---")
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
    
    if len(right_only) > 0:
        print(f"\n{len(right_only)} clients sans commande")
    
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
    
    # Calculs necessaire pour calculer les metriques
    # print("\nCALCULS DES METRIQUES")
    # ajout de  item total: revenu total par article

    fact['item_total'] = fact['price'] + fact['freight_value']
    # transforme le la date complete en mois pour les analyses mensuelles
    # exp: 2026/02
    fact['year_month'] = fact['order_purchase_timestamp'].dt.to_period('M').astype(str)
    
    # calcul delai de livraison (date livraison - date commande)
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


def calculer_metriques(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Calcule les metriques d'agregation demandees
    
    :param data: dictionnaire de DataFrames
    :return: dictionnaire avec les metriques ajoutees
    """
    print("\n" + "="*50)
    print("TRANSFORMATION: Calcul des metriques")
    print("="*50)
    
    # T3: Chiffre d'affaires par mois
    if 'fact_order_items' in data:
        fact = data['fact_order_items']
        
        # Groupe par mois et somme les revenus
        monthly_revenue = fact.groupby('year_month')['item_total'].sum().reset_index()
        monthly_revenue.columns = ['year_month', 'revenue_total']
        # Convertit year_month en string pour pouvoir sauvegarder en CSV
        monthly_revenue['year_month'] = monthly_revenue['year_month'].astype(str)
        
        data['monthly_revenue'] = monthly_revenue
        print(f"\nT3 - Chiffre d'affaires par mois : {monthly_revenue.shape}")
        print("-"*50)

        print(monthly_revenue.head())
    
    
    # T4: Top 10 categories par revenu
    # if 'fact_order_items' in data:
        fact = data['fact_order_items']
        
        # Groupe par categorie et somme les revenus
        top_categories = fact.groupby('product_category_name')['item_total'].sum().reset_index()
        top_categories.columns = ['product_category', 'revenue']
        # Trie par revenu decroissant et prend les 10 premiers
        top_categories = top_categories.sort_values('revenue', ascending=False).head(10)
        
        data['top_categories'] = top_categories
        print(f"\nT4 - Top catégories par revenu.: {top_categories.shape}")
        print("-"*50)
        print(top_categories.head())
    
        # T5: Delais de livraison moyens par mois
        if 'fact_order_items' in data and 'delivery_days' in data['fact_order_items'].columns:
            fact = data['fact_order_items']
            
            # Filtre les lignes avec delai de livraison valide
            fact_with_delivery = fact[fact['delivery_days'].notna()]
            
            # Calcule la moyenne des delais par mois
            delivery_metrics = fact_with_delivery.groupby('year_month')['delivery_days'].mean().reset_index()
            delivery_metrics.columns = ['year_month', 'avg_delivery_days']
            delivery_metrics['year_month'] = delivery_metrics['year_month'].astype(str)
            
            data['delivery_metrics'] = delivery_metrics
            print(f"\nT5 - Temps de livraison moyen: {delivery_metrics.shape}")
            print("-"*50)

            print(delivery_metrics.head())
        
        # BONUS: Note moyenne des avis par mois (version compacte)
        if 'order_reviews' in data and 'orders' in data:
            reviews = data['order_reviews'].copy()
            orders_ids = data['orders']['order_id'].unique()
            
            # Vérification d'intégrité
            avis_valides = reviews[reviews['order_id'].isin(orders_ids)]
            nb_orphelins = len(reviews) - len(avis_valides)
            
            if nb_orphelins > 0:
                print(f"{nb_orphelins} avis orphelins exclus")
            
            # Calcul avec la date des avis
            if 'review_creation_date' in avis_valides.columns:
                avis_valides['year_month'] = avis_valides['review_creation_date'].dt.to_period('M').astype(str)
                
                data['reviews_monthly'] = avis_valides.groupby('year_month').agg(
                    avg_review_score=('review_score', 'mean'),
                    review_count=('review_score', 'count')
                ).reset_index()
                
                print(f"\nScore moyen des avis par mois: {data['reviews_monthly'].shape}")
                print("-" * 50)
                print(data['reviews_monthly'].head())
        
    else: 
        print("veuillez d'abord construire le jeu de faits")
        return data
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
    
    #calculer les metriques
    
    dfs = calculer_metriques(data= dfs)

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
