# 🩺 Pipeline Datasus

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-green)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-%20Relational%20DB-blue)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Automatize a coleta, o tratamento e a persistência de dados do **DATASUS** com este pipeline robusto e extensível. Ideal para projetos que exigem atualização frequente e controle rigoroso de dados de saúde pública.

---

## 🚀 Funcionalidades

- 🔹 Download automático de arquivos do servidor FTP do DATASUS
- 🔹 Leitura de arquivos `.dbf` e conversão para DataFrame
- 🔹 Controle de inserção com `file_name` e `file_hash`
- 🔹 Criação automática da tabela no PostgreSQL, se não existir
- 🔹 API REST com **FastAPI**
- 🔹 Logging estruturado para monitoramento

---

## 📂 Estrutura

```
pipeline_datasus/
│
├── app/
│   ├── main.py                      # Roteamento e inicialização da API
│   ├── DataHandler.py               # Lógica de ingestão e persistência
│   ├── DataProcess.py               # Conversão dos arquivos DBF
│   ├── PostgresConnector.py         # Classe para conexão e execução no banco
│   └── DataSusFileAcquisition.py    # Download de arquivos do FTP
│
├── .env                             # Variáveis de ambiente
├── requirements.txt                 # Dependências
└── README.md
```

---

## ⚙️ Setup do Projeto

### 1. Clone o repositório

```bash
git clone https://github.com/mvrossetto/pipeline_datasus.git
cd pipeline_datasus
```

### 2. Crie um ambiente virtual

```bash
python -m venv .venv
source .venv/bin/activate       # Linux/Mac
.venv\Scripts\activate          # Windows
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Configure o `.env`

Crie o arquivo `.env` com as seguintes variáveis:

```
POSTGRES_HOST=localhost
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=senha
```

---

## ▶️ Executando

### API com reload automático

```bash
uvicorn app.main:app --reload
```

Acesse a documentação automática:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

## 📡 Endpoints

| Método | Rota             | Função                                              |
| ------ | ---------------- | --------------------------------------------------- |
| `POST` | `/process-files` | Recebe lista de arquivos específicos para processar |

### Exemplo de payload (POST `/process-files`)

```json
[
  {
    "directory": "dissemin/publicos/SINAN/DADOS/PRELIM/",
    "file": "VARCBR10"
  },
  {
    "directory": "dissemin/publicos/SINAN/DADOS/PRELIM/",
    "file": "DENG2023"
  }
]
```

---

## 🔧 Melhorias Futuras

- [ ] Fila assíncrona (Celery/RabbitMQ)
- [ ] Cache de arquivos já inseridos
- [ ] Testes automatizados

---

## 📝 Licença

MIT. Livre para uso, modificação e redistribuição.

---

**Autor:** [@mvrossetto](https://github.com/mvrossetto)
