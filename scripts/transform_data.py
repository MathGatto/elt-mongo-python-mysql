from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd


def visualize_collection(col):
    
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):

    col.update_many({}, {"$rename":{f"{col_name}" : f"{new_name}"}})

def select_category(col, category):

    query = {"Categoria do Produto": f"{category}"}
    
    lista_categoria = []
    for doc in col.find(query):
        lista_categoria.append(doc)

    return lista_categoria

def make_regex(col, coluna, regex):

    query = {f"{coluna}" : {"$regex": f"{regex}"}}

    lista_regex = []

    for doc in col.find(query):
        lista_regex.append(doc)

    return lista_regex

def create_dataframe(lista):

    df = pd.DataFrame(lista)
    return df

def format_date(df):

    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

def save_csv(df, path):

    df.to_csv(path, index=False)
    print(f"\nO Arquivo {path} foi salvo")


if __name__ == "__main__":

    uri = "mongodb+srv://mathgatto:O0o7PyRPLLnOI5Iw@cluster-pipeline.yng9pbu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline"
    # Executando a conexão ao mongodb e lendo os dados
    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos_desafio")

    # Visualizando os dados
    #visualize_collection(col)
    
    # Renomeando as colunas
    rename_column(col,"lat", "Latitude")
    rename_column(col,"lon", "Longitude")

    # Salvando os dados da categoria Livro
    lista_livros = select_category(col, "livros")
    print("\nLista Livros criada")

    df_livros = create_dataframe(lista_livros)
    print("\nDF Livros criado")

    format_date(df_livros)
    save_csv(df_livros, "../data/tabela_livros_desafio.csv")


    # Salvando os dados da Produtos vendidos a partir de 2021
    lista_produtos = make_regex(col, "Data da Compra", "/202[1-9]")
    print("\nLista Produtos criada")

    df_produtos = create_dataframe(lista_produtos)
    print("\nDF Produtos criado")

    format_date(df_produtos)
    save_csv(df_produtos, "../data/tabela_produtos_2021_em_diante_desafio.csv")

    client.close()
    

