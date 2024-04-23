if __name__ == "__main__":

    # estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo("sua_uri")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # renomeando as colunas de latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "../data_teste/tb_livros.csv")

    # salvando os dados dos produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "../data_teste/tb_produtos.csv")
