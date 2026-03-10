import asyncio
from playwright.async_api import async_playwright
from datetime import datetime
import os
import shutil
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import zipfile  # NOVO: Importa a biblioteca para lidar com ZIP
import io       # NOVO: Importa a biblioteca para lidar com E/S em memória

DOWNLOAD_DIR = "/tmp"

# ==============================
# Função de renomear arquivo
# ==============================
def rename_downloaded_file(download_dir, download_path):
    try:
        current_hour = datetime.now().strftime("%H")
        # Mesmo que seja um ZIP, mantemos a sua lógica de nome
        new_file_name = f"PEND-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None


# ==============================
# Função de atualização Google Sheets (MODIFICADA)
# ==============================
def update_packing_google_sheets(zip_file_path): # NOVO: Nome da variável mudo para clareza
    try:
        if not os.path.exists(zip_file_path):
            print(f"Arquivo {zip_file_path} não encontrado.")
            return

        df = None # NOVO: Inicializa o DataFrame como nulo

        # --- INÍCIO DA LÓGICA DE DESCOMPACTAÇÃO ---
        try:
            # 1. Abre o arquivo ZIP
            with zipfile.ZipFile(zip_file_path, 'r') as zf:
                
                # 2. Encontra o nome do arquivo .csv DENTRO do .zip
                csv_filename_inside_zip = None
                for file in zf.namelist():
                    if file.endswith('.csv'):
                        csv_filename_inside_zip = file
                        break
                
                if not csv_filename_inside_zip:
                    print(f"Erro: Nenhum arquivo .csv foi encontrado dentro do {zip_file_path}")
                    return # Aborta se não achar o CSV
                
                print(f"Lendo o arquivo '{csv_filename_inside_zip}' de dentro do ZIP...")

                # 3. Abre o arquivo CSV de dentro do ZIP para a memória
                with zf.open(csv_filename_inside_zip) as f:
                    # 4. Tenta ler o CSV (agora sim, é um CSV)
                    try:
                        # Tenta como UTF-8
                        df = pd.read_csv(f).fillna("")
                    except UnicodeDecodeError:
                        # Se falhar, é o erro de encoding original. Tenta 'latin-1'
                        print("Falha no UTF-8, tentando com 'latin-1'...")
                        f.seek(0) # Volta ao início do arquivo em memória
                        df = pd.read_csv(f, encoding='latin-1').fillna("")
        
        except zipfile.BadZipFile:
            print(f"Erro: O arquivo {zip_file_path} não é um arquivo ZIP válido.")
            return
        # --- FIM DA LÓGICA DE DESCOMPACTAÇÃO ---

        # 5. Continua com o upload para o Google Sheets
        if df is None:
            print("Erro: DataFrame não foi carregado, upload abortado.")
            return
            
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url(
            "https://docs.google.com/spreadsheets/d/1LZ8WUrgN36Hk39f7qDrsRwvvIy1tRXLVbl3-wSQn-Pc/edit#gid=734921183"
        )
        worksheet1 = sheet1.worksheet("3PL")
        
        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba '3PL'.")

    except Exception as e:
        # A mensagem de erro original 'utf-8' não deve mais aparecer aqui
        print(f"Erro durante o processo: {e}")


# ==============================
# Fluxo principal Playwright (Sem alterações)
# ==============================
async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            # LOGIN
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=10000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops134882')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee123')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_load_state("networkidle", timeout=20000)

            try:
                await page.locator('.ssc-dialog-close').click(timeout=10000)
            except:
                print("Nenhum pop-up foi encontrado.")
                await page.keyboard.press("Escape")

            # ================== DOWNLOAD: PENDING ==================
            print("\nIniciando Download: 3PL")
            await page.goto("https://spx.shopee.com.br/#/general-three-pl-handover/task-list")
            await page.wait_for_timeout(15000)

            await page.get_by_role("button", name="Exportar").nth(0).click()
            await page.wait_for_timeout(15000)
            await page.get_by_role("menuitem", name="Exportar").click()
            await page.wait_for_timeout(10000)

            await page.goto("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
            await page.wait_for_timeout(10000)
            await page.get_by_text("Exportar tarefa").click()

            async with page.expect_download() as download_info:
                await page.get_by_role("button", name="Baixar").nth(0).click()

            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)

            new_file_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
            if new_file_path:
                # Esta função agora sabe como lidar com o ZIP
                update_packing_google_sheets(new_file_path)

            print("\n✅ Processo concluído com sucesso.")

        except Exception as e:
            print(f"❌ Erro fatal durante o processo: {e}")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
