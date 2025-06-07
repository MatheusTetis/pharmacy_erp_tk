import requests

payload = {
    "id": "cfbfba56-558a-4799-8988-6f3c58f1369f",
    "idConjuntoDados": "933159db-7f90-4227-adbf-a100280902b4",
    "titulo": "Preço de Medicamentos no Brasil – Governo",
    "descricao": "Os dados representam a lista de preços de Medicamentos , contemplando o preço Fábrica, ou preço fabricante (PF), que é o preço máximo praticado que pode ser praticado pelas empresas produtoras ou importadoras do produto e pelas empresas distribuidoras. O PF indica o preço máximo permitido para venda a farmácias e drogarias e o Preço Máximo de Venda ao Governo (PMVG) indica o preço teto de venda aos entes da administração pública quando for aplicável o desconto do Coeficiente de Adequação de Preços (CAP), quando não for o preço teto é o PF.\n\nA data de atualização deste conjunto de dados pode ser vista no portal de Dados Abertos da Anvisa, endereço www.dados.anvisa.gov.br.\nO nome do arquivo referente a este conjunto de dados: TA_PRECO_MEDICAMENTO_GOV.CSV",
    "link": "https://dados.anvisa.gov.br/dados/TA_PRECO_MEDICAMENTO_GOV.csv",
    "formato": "csv",
    "tipo": 1
}

def download_medicine_pricing_table():
    local_filename = payload["link"].split("/")[-1]

    with requests.get(payload["link"], verify=False, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

    return local_filename

#download_remedy_pricing_table()