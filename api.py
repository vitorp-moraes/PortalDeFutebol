import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/escudos", StaticFiles(directory="escudos"), name="escudos")


@app.get("/api/jogos")
def listar_jogos():
    try:
        # 1. Carrega as tabelas
        df_dimensao = pd.read_excel('dimensao_campeonato_completo.xlsx')
        df_fato = pd.read_excel('fato_estatisticas.xlsx')

        # --- A FAXINA CIRÚRGICA PARA O PIVOT_TABLE ---

        # A) Remove espaços acidentais nos nomes das colunas
        df_fato.columns = df_fato.columns.str.strip()

        if 'Mando' in df_fato.columns and 'Period' in df_fato.columns:

            # B) Filtra apenas as estatísticas do jogo completo
            # Converte a coluna Period para string, tira espaços e transforma em maiúsculas
            df_fato['Period'] = df_fato['Period'].astype(str).str.strip().str.upper()
            df_fato = df_fato[df_fato['Period'].isin(['ALL', 'MATCH', 'TOTAL'])]

            # Se após o filtro não sobrar nada, tenta sem o filtro (caso o robô não grave 'ALL')
            if df_fato.empty:
                df_fato = pd.read_excel('fato_estatisticas.xlsx')
                df_fato.columns = df_fato.columns.str.strip()

            # C) Padroniza a coluna Mando para 'Casa' e 'Fora'
            df_fato['Mando'] = df_fato['Mando'].astype(str).str.strip().str.lower()
            df_fato['Mando'] = df_fato['Mando'].replace({
                'home': 'Casa', 'away': 'Fora',
                '1': 'Casa', '2': 'Fora',
                'casa': 'Casa', 'fora': 'Fora'
            })

            # D) Remove a coluna Period antes do pivot, pois já filtramos e não precisamos dela nas colunas finais
            if 'Period' in df_fato.columns:
                df_fato = df_fato.drop(columns=['Period'])

            # E) A Mágica do Pivot! Deita a tabela.
            df_fato_pivot = df_fato.pivot_table(index='Match_ID', columns='Mando', aggfunc='first')

            # F) Limpa os nomes das colunas geradas
            df_fato_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_fato_pivot.columns]
            df_fato_pivot = df_fato_pivot.reset_index()

        else:
            df_fato_pivot = df_fato

        # 3. O Cruzamento Definitivo
        df_completo = pd.merge(df_dimensao, df_fato_pivot, on='Match_ID', how='left')

        # 4. Blindagem Total contra erros do JSON
        # O astype(object) é o que permite trocar NaN por None em colunas numéricas
        df_completo = df_completo.astype(object)
        df_completo = df_completo.where(pd.notnull(df_completo), None)

        # 5. Envia
        dados_json = df_completo.to_dict(orient='records')
        return {"status": "sucesso", "total_jogos": len(dados_json), "dados": dados_json}

    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}


# Para rodar isso, digite no terminal:
# uvicorn api:app --reload