if __name__ == "__main__":
    
    # realizando a conexão com mysql
    cnx = connect_mysql("localhost", "millenagena", "12345")
    cursor = create_cursor(cnx)

    # criando a base de dados
    create_database(cursor, "db_produtos_teste")
    show_databases(cursor)

    # criando tabela
    create_product_table(cursor, "db_produtos_teste", "tb_livros")
    show_tables(cursor, "db_produtos_teste")

    # lendo e adicionando os dados
    df = read_csv("../data_teste/tbl_livros.csv")
    add_product_data(cnx, cursor, df, "db_produtos_teste", "tb_livros")
