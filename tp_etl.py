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


def extract() -> dict[str, pd.DataFrame]:
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


def parser_date(X, date_format='%m/%d/%Y %H:%M') -> pd.Series:
    """
    Docstring for parser_date
    Pour parser une colonne entiere de date
    
    :param X: La colonne de date a parser
    :param date_format: Le format de la date a parser
    return: La date au format datetime de pandas
    """
    return pd.to_datetime(X, format=date_format, errors='coerce')


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
    print(f"Nombre de doublons dans la table {table_name} : {nb_doublons}")

    # Supprimer les doublons
    if nb_doublons > 0:
        print(f"Dimension avant suppression des doublons dans la table {table_name} : {df.shape}")
        df = df.drop_duplicates()
        print(f"Doublons supprimes dans la table {table_name}. Nouvelle dimension : {df.shape}\n\n")
    
    return df


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
        # Categories manquantes -> 'Unknown'
        if 'product_category_name' in df.columns:
            nb_nan = df['product_category_name'].isna().sum()
            pourcentage_nan = (nb_nan) / len(df['product_category_name']) * 100

            print(f"Nombre de categories manquantes : {nb_nan} ({pourcentage_nan:.2f}%)")

            df['product_category_name'] = df['product_category_name'].fillna('Inconnu')
            print(f"Remplacement de {nb_nan} categories manquantes par 'Inconnu'.\n\n")
        
            # Calculer la longueur de chaque nom de catégorie
            df['product_name_lenght'] = df['product_category_name'].str.len()

        # Supprimer pour les autres colonnes
        cols_meta = [
            'product_description_lenght',
            'product_photos_qty', 'product_width_cm', 'product_length_cm',
            'product_height_cm', 'product_weight_g',
        ]

        for col in cols_meta:
            if col in df.columns:
                # Rempli par 0
                df[col] = df[col].fillna(0)

    return df


def transform(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
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
            dfs = extract()
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
                final_tables = transform(dfs)
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





def _extract():
    df = extract()
    df_customers = df['customers']
    df_orders = df['orders']
    df_orders_customers = pd.merge(df_orders, df_customers, on='customer_id')
    print(df_orders_customers.shape)

    df_payments = read_csv_file('order_pymts')

    df_orders_payments = pd.merge(df_orders, df_payments,
                                  left_on='order_id', 
                                  right_on='order_id', how='outer', indicator=True)

    print(df_orders_payments.shape)
    print(df_orders_payments['_merge'].value_counts())
    df_missing_payment = df_orders_payments[df_orders_payments['_merge'] == 'left_only']

    print(df_missing_payment)
