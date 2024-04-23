from extract_and_save_data import elt_produto 

produto = elt_produto()
print(f' status da conexão => {produto.stats}')
client_produto = produto.connect_mongo()
print(f' status da conexão => {produto.stats}')
banco_produto = produto.create_connect_db(client_produto,'db_produto')

collection_produto = produto.create_connect_collection(banco_produto,'produto')

data_produto = produto.extract_api_data()

produtos_inseridos =  produto.insert_data(collection_produto ,data_produto)

print(f' total de produtos inseridos => {produtos_inseridos}')

produto.drop_database(client_produto,'db_produto')
