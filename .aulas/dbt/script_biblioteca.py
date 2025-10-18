import csv
import random
from datetime import datetime, timedelta

def random_date(start, end):
    """Gera uma data aleatória entre start e end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

# Listas para geração dos dados
first_names = ["João", "Maria", "Carlos", "Ana", "Pedro", "Paula", "Lucas", "Mariana", "Gabriel", "Beatriz"]
last_names = ["Silva", "Souza", "Oliveira", "Costa", "Pereira", "Rodrigues", "Almeida", "Nascimento", "Lima", "Gomes"]
nationalities = ["Brasileiro", "Estadunidense", "Britânico", "Canadense", "Australiano"]
categories = ["Ficção", "Não Ficção", "Romance", "Mistério", "Fantasia", "História", "Biografia"]
addresses = ["Rua A, 123", "Avenida B, 456", "Travessa C, 789", "Praça D, 101", "Alameda E, 202"]

# Geração do arquivo autores.csv (100 registros)
with open("autores.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["autor_id", "nome", "sobrenome", "nacionalidade", "data_nascimento"])
    for autor_id in range(1, 101):
        nome = random.choice(first_names)
        sobrenome = random.choice(last_names)
        nacionalidade = random.choice(nationalities)
        birth_date = random_date(datetime(1940, 1, 1), datetime(1990, 12, 31)).strftime("%Y-%m-%d")
        writer.writerow([autor_id, nome, sobrenome, nacionalidade, birth_date])

# Geração do arquivo editoras.csv (20 registros)
with open("editoras.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["editora_id", "nome_editora", "endereco", "telefone", "email"])
    for editora_id in range(1, 21):
        # Usa letras A, B, C... para nomear as editoras
        nome_editora = f"Editora {chr(64+editora_id)}"
        endereco = random.choice(addresses)
        telefone = f"+55 11 {random.randint(1000,9999)}-{random.randint(1000,9999)}"
        email = f"contato@editora{chr(64+editora_id)}.com"
        writer.writerow([editora_id, nome_editora, endereco, telefone, email])

# Geração do arquivo livros.csv (200 registros)
with open("livros.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["livro_id", "titulo", "autor_id", "editora_id", "ano_publicacao", "isbn", "categoria", "status"])
    for livro_id in range(1, 201):
        titulo = f"Livro {livro_id}"
        autor_id = random.randint(1, 100)     # FK para autores.csv
        editora_id = random.randint(1, 20)      # FK para editoras.csv
        ano_publicacao = random.randint(1950, 2020)
        isbn = "".join([str(random.randint(0,9)) for _ in range(13)])
        categoria = random.choice(categories)
        status = random.choice(["disponível", "emprestado"])
        writer.writerow([livro_id, titulo, autor_id, editora_id, ano_publicacao, isbn, categoria, status])

# Geração do arquivo clientes.csv (300 registros)
with open("clientes.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["cliente_id", "nome", "sobrenome", "data_nascimento", "endereco", "telefone", "email"])
    for cliente_id in range(1, 301):
        nome = random.choice(first_names)
        sobrenome = random.choice(last_names)
        birth_date = random_date(datetime(1960, 1, 1), datetime(2000, 12, 31)).strftime("%Y-%m-%d")
        endereco = random.choice(addresses)
        telefone = f"+55 21 {random.randint(1000,9999)}-{random.randint(1000,9999)}"
        email = f"{nome.lower()}.{sobrenome.lower()}@exemplo.com"
        writer.writerow([cliente_id, nome, sobrenome, birth_date, endereco, telefone, email])

# Geração do arquivo emprestimos.csv (5000 registros)
with open("emprestimos.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["emprestimo_id", "livro_id", "cliente_id", "data_emprestimo", "data_devolucao", "status"])
    for emprestimo_id in range(1, 5001):
        livro_id = random.randint(1, 200)      # FK para livros.csv
        cliente_id = random.randint(1, 300)      # FK para clientes.csv
        data_emprestimo_date = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31))
        data_emprestimo = data_emprestimo_date.strftime("%Y-%m-%d")
        status = random.choice(["ativo", "finalizado", "atrasado"])
        # Se o empréstimo já foi finalizado ou está atrasado, define uma data de devolução
        if status in ["finalizado", "atrasado"]:
            data_devolucao_date = data_emprestimo_date + timedelta(days=random.randint(1, 30))
            data_devolucao = data_devolucao_date.strftime("%Y-%m-%d")
        else:
            data_devolucao = ""
        writer.writerow([emprestimo_id, livro_id, cliente_id, data_emprestimo, data_devolucao, status])

print("Arquivos CSV gerados com sucesso!")
