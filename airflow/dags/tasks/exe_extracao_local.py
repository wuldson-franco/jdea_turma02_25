import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def google_sheet_to_local_parquet_all_sheets(sheet_id, output_dir="./jdea_turma01_25/"):
    """
    Extrai dados de todas as abas de uma Google Sheet e salva em arquivos Parquet locais.
    """
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file('./airflow/config_airflow/credentials.json', scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)

        # Cria diretório se não existir
        os.makedirs(output_dir, exist_ok=True)

        # Percorre todas as abas
        for sheet in spreadsheet.worksheets():
            sheet_name = sheet.title
            logging.info(f"Lendo dados da aba: {sheet_name}")

            try:
                data = sheet.get_all_records()
            except gspread.exceptions.GSpreadException as e:
                logging.warning(f"Erro com get_all_records() na aba {sheet_name}: {e}")
                continue  # pula para a próxima aba em caso de erro

            if not data:
                logging.warning(f"Nenhum dado encontrado na aba {sheet_name}")
                continue

            df = pd.DataFrame(data)
            parquet_file = os.path.join(output_dir, f"{sheet_name}.parquet")
            df.to_parquet(parquet_file, index=False)
            logging.info(f"Arquivo salvo localmente em: {os.path.abspath(parquet_file)}")

    except Exception as e:
        logging.error(f"Erro ao processar a planilha {sheet_id}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logging.info("Iniciando extração de TODAS as abas do Google Sheets...")
    google_sheet_to_local_parquet_all_sheets(
        sheet_id="1XMrFMPptCKfy6u2tsP0FuAgPlbJ-anA0caH2e6LL5F0",  # substitua pelo ID real da planilha
        output_dir="./jdea_turma01_25/"
    )
