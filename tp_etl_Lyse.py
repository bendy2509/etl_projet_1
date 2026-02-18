"""
TP ETL - Traitement des donnees Olist
Auteur: Nherlyse MORISSET
Date: Fevrier 2026
"""

from datetime import datetime
import pandas as pd 
import os
from typing import dict


# ============================================================================
# PARTIE 1: FONCTIONS D'EXTRACTION
# ============================================================================

def read_csv_file(file_name):
    """
        Lit un fichier CSV avec pandas
        :param file_name: Nom du fichier (sans l'extension .csv)
        :return: DataFrame pandas contenant les donnees
    """
    file_path = f'./sqlite_exports/{file_name}.csv'
    df = pd.read_csv(file_path, low_memory=False)
    if 'index' in df.columns:
        df = df.drop(columns=['index'])
        
    # afficher les stats du ficher charge
    print(f"\n====== Statistics on {file_name} =======")
    print(f"Dimensions (lignes, colonnes): {df.shape}")
    print(f"Colonnes: {list(df.columns)}")
    print(f"First rows in {file_name}")
    print(df.head(5))

    return df

def extract_all_files(): 
    """
        Extrait tous les fichiers CSV necessaires pour le TP
        :return: dictionnaire contenant tous les DataFrames charges
    """
    print("\n" + "="*50)
    print("PHASE 1: EXTRACTION DES DONNeES")
    print("="*50)
    
    
    
    # dict pour stocker les df
    data = {}
    
    # liste des fichiers a charger
    files_to_load = [
        'customers',      # Informations sur les clients
        'sellers',        # -------------------- vendeurs
        'products',       # Catalogue produits
        'orders',         # Commandes
        'order_items',    # Lignes de commande 
        'order_pymts',    # Paiements des commandes
        'order_reviews',  # Avis des clients
        'geoloc',         # Geolocalisation par code postal
        'translation'     # Traduction des categories de produits
    ]
    
    # charge chaque fichier et le met dans un dict
    for file in files_to_load:
        data[file] = read_csv_file(file)
    
    return data


# ============================================================================
# PARTIE 2: FONCTIONS DE TRANSFORMATION
# ============================================================================

# def parse_dates(data) -> pd.DataFrame:
def parse_dates(data:dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Convertit toutes les colonnes contenant des dates en format datetime

    :param data: dictionnaire de DataFrames
    :return: dictionnaire mis à jour avec les dates parsees
    :rtype: dict[str, pd.DataFrame]
    """
    
    print("\n" + "="*50)
    print("TRANSFORMATION: Parsing des dates")
    print("="*50)
    
    # parcourt chaque table du dict
    for table_name , df in data.items():
        print(f"\nTable: {table_name}")
        
        # parcourt chauqe col de la table
        for col in df.columns:
            # si le nom contient "date " ou " time"
            if 'date' in col.lower() or 'time' in col.lower():
                print(f"Conversion de {col} ...")
                avant =  df[col].count()
                
                # convertit en datetime err NaT si la conversion echoue
                df[col] = pd.to_datetime(df[col], errors = 'coerce')
                
                # compte les caleurs valides apre converison
                apres = df[col].count()
                
                print(f'{avant} valeurs -> {apres} date valides')
    return data

def remove_duplicates(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    '''
    Supprime les doublons dans chaque table
    :param data: dictionnaire de DataFrames
    :return: dictionnaire sans doublons
    
    '''
    print("\n" + "="*50)
    print("TRANSFORMATION: Suppression des doublons")
    print("="*50)
                
    for table_name, df, in data.items():
        # taille av supp
        avant = len(df)
        
        # supp les doub
        df = df.drop_duplicates()
        
        # taille apres supp
        apres = len(df)
        
        if avant > apres:
            print(f"{table_name} : {avant - apres} doublons supprimes")
    
    return data

def handle_missing_values(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Gère les valeurs manquantes dans les donnees
    :param data: dictionnaire de DataFrames
    :return: dictionnaire avec valeurs manquantes traitees
    """
    print("\n" + "="*50)
    print("TRANSFORMATION: Gestion des valeurs manquantes")
    print("="*50)
    
    # pou prduts yo, categ manquantes = unknown
    if 'products' in data:
        print(f"Traitement des produits:")
        # konte cat manqunates
        missing = data['products']['product_category_name'].isna().sum()
        if missing > 0:
            print(f"  {missing} produits sans categorie -> remplace par 'unknown'")
            # remplit les val manquantes avec unknown
            data['products']['product_category_name'] = data['products']['product_category_name'].fillna('unknown')
            
    
    # Pour les avis: note manquante = 3 
    if 'order_reviews' in data:
        print("\nTraitement des avis:")
        missing = data['order_reviews']['review_score'].isna().sum()
        if missing > 0:
            print(f"  {missing} avis sans note -> remplace par 3")
            data['order_reviews']['review_score'] = data['order_reviews']['review_score'].fillna(3)
    
    return data          

def create_fact_table(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Cree la table de faits en joignant order_items avec les dimensions

    :param data: dictionnaire de DataFrames
    :type date: dict()
    :return: dictionnaire incluant fact_order_items
    """
    print("\n" + "="*50)
    print("TRANSFORMATION: Creation de la table de faits")
    print("="*50)
    
    # Verifie que toutes les tables necessaires existent
    required_tables = ['order_items', 'orders', 'customers', 'sellers', 'products']
    for table in required_tables:
        if table not in data:
            print(f"ERREUR: Table {table} manquante!")
            return data
    
    # Copie de order_items (table de base)
    fact = data['order_items'].copy()
    print(f"\nDimension initiale: {fact.shape}")
    
    # 1. Jointure avec orders (commandes)
    print("Jointure avec orders...")
    fact = pd.merge(
        fact, 
        data['orders'],
        on='order_id',
        how='left'  # Garde toutes les lignes de order_items
    )
    print(f"  Après jointure orders: {fact.shape}")
    
    # 2. Jointure avec customers 
    print("Jointure avec customers...")
    fact = pd.merge(
        fact,
        data['customers'],
        on='customer_id',
        how='left'
    )
    print(f"  Après jointure customers: {fact.shape}")
    
    # 3. Jointure avec sellers (vendeurs)
    print("Jointure avec sellers...")
    fact = pd.merge(
        fact,
        data['sellers'],
        on='seller_id',
        how='left'
    )
    print(f"  Après jointure sellers: {fact.shape}")
    
    # 4. Jointure avec products (produits)
    print("Jointure avec products...")
    fact = pd.merge(
        fact,
        data['products'],
        on='product_id',
        how='left'
    )
    print(f"  Après jointure products: {fact.shape}")
    
    # Verification des jointures: combien de lignes sans client?
    orphan_rows = fact[fact['customer_id'].isna()]

    #TODO: Les orphelins...
    if len(orphan_rows) > 0:
        print(f"  ATTENTION: {len(orphan_rows)} lignes sans client associe")
    
    # Calcul du revenu par article (prix + frais de livraison)
    # Si freight_value manque, on le remplace par 0
    # fact['freight_value'] = fact['freight_value'].fillna(0) # sipoze fet deja nan tretman val manquantes
    fact['item_total'] = fact['price'] + fact['freight_value']
    
    # Extraction du mois/annee à partir de la date de commande
    fact['year_month'] = fact['order_purchase_timestamp'].dt.to_period('M').astype(str)
    
    # Calcul du temps de livraison (en jours)
    if 'order_delivered_customer_date' in fact.columns:
        fact['delivery_days'] = (
            fact['order_delivered_customer_date'] - fact['order_purchase_timestamp']
        ).dt.days
        
        # Statistiques sur les delais de livraison
        print(f"\nStatistiques des delais de livraison:")
        print(f"  Moyenne: {fact['delivery_days'].mean():.1f} jours")
        print(f"  Mediane: {fact['delivery_days'].median():.1f} jours")
        print(f"  Minimum: {fact['delivery_days'].min():.1f} jours")
        print(f"  Maximum: {fact['delivery_days'].max():.1f} jours")
    
    # Stocke la table de faits dans le dictionnaire
    data['fact_order_items'] = fact
    print(f"\nTable de faits finale: {fact.shape}")
    
    return data
    

def calculate_metrics(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
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
        monthly_revenue.columns = ['year_month', 'revenue']
        # Convertit year_month en string pour pouvoir sauvegarder en CSV
        monthly_revenue['year_month'] = monthly_revenue['year_month'].astype(str)
        
        data['monthly_revenue'] = monthly_revenue
        print(f"\nT3 - monthly_revenue creee: {monthly_revenue.shape}")
        print(monthly_revenue.head())
    
    # T4: Top 10 categories par revenu
    if 'fact_order_items' in data:
        fact = data['fact_order_items']
        
        # Groupe par categorie et somme les revenus
        top_categories = fact.groupby('product_category_name')['item_total'].sum().reset_index()
        top_categories.columns = ['product_category', 'revenue']
        # Trie par revenu decroissant et prend les 10 premiers
        top_categories = top_categories.sort_values('revenue', ascending=False).head(10)
        
        data['top_categories'] = top_categories
        print(f"\nT4 - top_categories creee: {top_categories.shape}")
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
        print(f"\nT5 - delivery_metrics creee: {delivery_metrics.shape}")
        print(delivery_metrics.head())
    
    # BONUS: Note moyenne des avis par mois
    if 'order_reviews' in data and 'orders' in data:
        # Jointure pour recuperer la date des commandes
        reviews_with_date = pd.merge(
            data['order_reviews'],
            data['orders'][['order_id', 'order_purchase_timestamp']],
            on='order_id',
            how='left'
        )
        
        # Extrait le mois/annee
        reviews_with_date['year_month'] = reviews_with_date['order_purchase_timestamp'].dt.to_period('M').astype(str)
        
        # Calcule la note moyenne par mois
        reviews_monthly = reviews_with_date.groupby('year_month')['review_score'].mean().reset_index()
        reviews_monthly.columns = ['year_month', 'avg_review_score']
        reviews_monthly['year_month'] = reviews_monthly['year_month'].astype(str)
        
        data['reviews_monthly'] = reviews_monthly
        print(f"\nBONUS - reviews_monthly creee: {reviews_monthly.shape}")
        print(reviews_monthly.head())
    
    return data

# ============================================================================
# PARTIE 3: FONCTIONS DE CHARGEMENT
# ============================================================================

def save_to_csv(data, output_dir='outputs'):
    """
    Sauvegarde les DataFrames en fichiers CSV
    :param data: dictionnaire de DataFrames
    :param output_dir: Dossier de sortie
    """
    print("\n" + "="*50)
    print("PHASE 3: CHARGEMENT - Sauvegarde CSV")
    print("="*50)
    
    # Cree le dossier de sortie s'il n'existe pas
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Dossier {output_dir} cree")
    
    # Liste des tables à sauvegarder
    tables_to_save = [
        'fact_order_items',    # Table de faits 
        'monthly_revenue',      # T3
        'top_categories',       # T4
        'delivery_metrics',     # T5
        'reviews_monthly'       # Bonus
    ]
    
    for table_name in tables_to_save:
        if table_name in data:
            # Construit le chemin complet du fichier
            file_path = f'{output_dir}/{table_name}.csv'
            
            # Sauvegarde sans l'index
            data[table_name].to_csv(file_path, index=False)
            
            print(f" {table_name}.csv sauvegarde ({len(data[table_name])} lignes)")


def save_to_sqlite(data, db_path='outputs/etl.db'):
    """
    Sauvegarde les DataFrames dans une base SQLite
    :param data: dictionnaire de DataFrames
    :param db_path: Chemin vers la base SQLite
    """
    print("\n" + "="*50)
    print("PHASE 3: CHARGEMENT - Sauvegarde SQLite")
    print("="*50)
    
    # Import de sqlite3 (module standard, pas besoin de l'installer)
    import sqlite3
    
    # Cree une connexion à la base SQLite
    # Si le fichier n'existe pas, il sera cree automatiquement
    conn = sqlite3.connect(db_path)
    
    # dictionnaire de correspondance: (nom dans data, nom dans la base)
    tables_to_save = [
        ('fact_order_items', 'fact_order_items'),
        ('monthly_revenue', 'monthly_revenue'),
        ('top_categories', 'top_categories'),
        ('delivery_metrics', 'delivery_metrics'),
        ('reviews_monthly', 'reviews_monthly')
    ]
    
    for data_name, table_name in tables_to_save:
        if data_name in data:
            # Sauvegarde dans SQLite
            # if_exists='replace' remplace la table si elle existe dejà
            data[data_name].to_sql(table_name, conn, if_exists='replace', index=False)
            
            # Verifie que la sauvegarde a fonctionne
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            
            print(f" Table {table_name}: {count} lignes dans SQLite")
    
    # Ferme la connexion
    conn.close()
    print(f"Base SQLite sauvegardee: {db_path}")


def generate_report(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    T2: Genère un rapport sur les donnees
    :param data: dictionnaire de DataFrames
    """
    print("\n" + "="*50)
    print("T2 - RAPPORT DE SYNTHÈSE")
    print("="*50)
    
    total_records = 0
    
    for table_name, df in data.items():
        # Calcule le nombre total d'enregistrements
        total_records += len(df)
        
        print(f"\n{table_name.upper()}:")
        print(f"  - Lignes: {len(df)}")
        print(f"  - Colonnes: {len(df.columns)}")
        
        # Affiche les 5 premières colonnes
        cols = list(df.columns)[:5]
        print(f"  - Colonnes: {', '.join(cols)}...")
    
    print(f"\n{'='*50}")
    print(f"TOTAL GENERAL: {total_records} enregistrements")
    print(f"Date du rapport: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ============================================================================
# PARTIE 4: PIPELINE PRINCIPAL
# ============================================================================

def run_etl_pipeline():
    """
    Execute le pipeline ETL complet
    """
    print("\n" + "="*60)
    print("DEBUT DU PIPELINE ETL")
    print("="*60)
    
    # 1. EXTRACTION
    data = extract_all_files()
    
    # 2. TRANSFORMATION
    data = parse_dates(data)
    data = remove_duplicates(data)
    data = handle_missing_values(data)
    data = create_fact_table(data)
    data = calculate_metrics(data)
    
    # 3. RAPPORT
    generate_report(data)
    
    # 4. CHARGEMENT
    save_to_csv(data)
    save_to_sqlite(data)
    
    print("\n" + "="*60)
    print("PIPELINE ETL TERMINE AVEC SUCCES!")
    print("="*60)
    
    
    
    
    
    
# ============================================================================
# PARTIE 5: POINT D'ENTRÉE DU SCRIPT
# ============================================================================

if __name__ == "__main__":
    """
        Point d'entrée du script
    """
    # Exécute le pipeline complet
    run_etl_pipeline()
