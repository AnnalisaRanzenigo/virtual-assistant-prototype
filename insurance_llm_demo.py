'''
DISCLAIMER:
This code is for demonstration purposes only. 
The CSV files referenced are private and are not included.
Please replace them with your own sample or anonymized data before running.
'''


import pandas as pd
import ollama
import pandasql as psql
import os
import streamlit as st
import collections
import spacy

# ---------------------------
# --- CARICAMENTO DATI ---
# ---------------------------

merged_db = pd.read_csv(file_path_merged)
pivot_clienti = pd.read_csv (file_path_pivot)

# ---------------------------
# --- AVVIO CONVERSAZIONE ---
# ---------------------------
storico_domande = []
storico_risposte = []
tabelle_risultati = []

print("Benvenuto! Con chi parlo?")
nome_completo = input("Nome e Cognome: ").strip()
parti_nome = nome_completo.split()

if len(parti_nome) < 2:
    print("Per favore, riferisca il suo nome completo.")
else:
    nome_cliente = parti_nome[0].lower()
    cognome_cliente = " ".join(parti_nome[1:]).lower()

    cliente_presente_pivot = pivot_clienti[
        ((pivot_clienti['Nome'].str.lower() == nome_cliente) & (pivot_clienti['Cognome'].str.lower() == cognome_cliente)) |
        ((pivot_clienti['Nome'].str.lower() == cognome_cliente) & (pivot_clienti['Cognome'].str.lower() == nome_cliente))
    ]

    if not cliente_presente_pivot.empty:
        print(f"Ciao {nome_cliente.title()}, per accedere alla sua reportistica, mi dica il suo codice cliente.")
        codice_cliente = input("Codice Cliente: ").strip()

        if codice_cliente in cliente_presente_pivot['codice_cliente'].astype(str).values:
            print(f"Grazie {nome_cliente.title()}! Come posso aiutarla?")
        else:
            print("Sembra che il codice non le corrisponda, ma possiamo aiutarla comunque, di cosa ha bisogno oggi?")
    else:
        print(f"Ciao {nome_cliente.title()}, vedo che lei non è ancora nostro cliente. Come possiamo aiutarla oggi?")


# ---------------------------
# --- PROMPT MODELLI ---
# ---------------------------

# PROMPT SQL
PROMPT_SQL = """
Sei un assistente AI specializzato nella generazione di query SQL per un database rappresentato dalla tabella `merged_db`.

**Obiettivo:** Generare **esclusivamente** la query SQL che risponde alla domanda dell'utente.

**Vincoli:**
1.  Devi restituire **solo** codice SQL.
2.  La query SQL deve essere valida per essere eseguita con pandasql sulla tabella `merged_db`.
3.  La tua risposta **deve iniziare** con la parola chiave `SELECT`.
4.  Non includere **nessun** altro testo, spiegazione o preambolo nella tua risposta.
5. Quando esegui confronti numerici su colonne che potrebbero contenere testo rappresentante numeri, utilizza `CAST` o `CONVERT` per assicurarti che vengano trattati come numeri (REAL o INTEGER) nel confronto. Gestisci anche potenziali separatori di migliaia (come la virgola) utilizzando `REPLACE` prima della conversione.
6. **Considera che i dati nelle colonne numeriche (come `Reddito`, `Premio_Unico`, `Capitale_Rivalutato`, ecc.) potrebbero essere stati letti come testo. Applica le conversioni necessarie (`CAST` o `CONVERT`) prima di eseguire operazioni numeriche o confronti.**

**Struttura della tabella `merged_db`:**
- `column name`: column description (type).

### Esempio 1:
Domanda cliente: Qual è il reddito medio dei clienti che hanno un premio unico non nullo?
Query:
SELECT AVG(T1.Reddito)
FROM merged_db AS T1
WHERE T1.codice_cliente IN (SELECT codice_cliente FROM merged_db WHERE Premio_Unico IS NOT NULL);

### Esempio 2:
Domanda del cliente: "Quali sono i prodotti assicurativi più popolari tra i clienti della mia fascia d'età (50 anni)?"
Query:
SELECT T1.Prodotto, COUNT(DISTINCT T1.codice_cliente) AS NumeroClienti
FROM merged_db AS T1
WHERE T1.codice_cliente IN (SELECT codice_cliente FROM merged_db WHERE Età = 50)
GROUP BY T1.Prodotto
ORDER BY NumeroClienti DESC;

### Esempio 3:
Domanda cliente: Mi puoi dire se c'è una correlazione tra il reddito e il tipo di prodotto scelto per l'assicurazione?
Query:
SELECT
    AVG(T1.Reddito) AS Average_Income,
    T1.Prodotto,
    COUNT(DISTINCT T1.codice_cliente) AS Number_of_Clients
FROM merged_db AS T1
GROUP BY T1.Prodotto
ORDER BY AVG(T1.Reddito) DESC;

Restituisci solo la query SQL senza spiegazioni.
"""

# PROMPT RISPOSTA
PROMPT_RESPONSE = """
Agisci come un esperto e cordiale consulente assicurativo. Il tuo obiettivo principale è assistere i clienti indecisi, guidandoli verso la soluzione assicurativa più adatta alle loro esigenze, attraverso una conversazione coinvolgente e persuasiva.

### Istruzioni:
- Inizia la conversazione in modo diretto e amichevole, rivolgendoti al cliente con un tono rassicurante e professionale. Evita saluti generici iniziali.
- Utilizza la tabella risultante dalla query SQL (result) per guidare la risposta.
- Utilizza un linguaggio chiaro, naturale e persuasivo. Sii empatico con l'indecisione del cliente e focalizzati sui benefici che una soluzione assicurativa può offrire.
- Trasforma i risultati numerici delle query SQL in informazioni facilmente comprensibili e contestualizzate per il cliente. Evidenzia l'importanza di questi dati per la sua situazione specifica.
- Se la query restituisce un elenco di prodotti, presentali in modo interessante, descrivendone brevemente i vantaggi e suggerendo quelli potenzialmente più rilevanti in base al contesto della conversazione.
- Poni domande aperte e mirate per stimolare la riflessione del cliente sulle sue esigenze, priorità e preoccupazioni. Incoraggialo a condividere maggiori dettagli per poter offrire una consulenza più personalizzata.
- Assicurati che ogni domanda si basi sull'ultima risposta del cliente o su un argomento introdotto precedentemente nella conversazione.
- Evidenzia i vantaggi concreti di considerare diverse opzioni assicurative o di investimento, sottolineando come queste possano offrire protezione, sicurezza e tranquillità per il futuro.
- In caso di mancanza di dati specifici, offri comunque consigli generali e pertinenti, invitando il cliente a fornire maggiori informazioni per un'analisi più approfondita.
- Se la richiesta del cliente indica chiaramente la necessità di un intervento umano, informa il cliente in modo cortese e concludi la conversazione, assicurandolo che un operatore lo contatterà a breve.
- il segnaposto {result} indica il risulatato della query SQL, sostituiscilo con il risultato della query SQL quando possibile.
- **Limita le tue risposte a un massimo di 3 frasi.**

---
### Esempio 1:
Domanda del cliente: "Quanto mi costerebbe, più o meno, un'assicurazione? Non so ancora per cosa esattamente..."
Risposta dell'assistente : "Capisco la tua incertezza. In media, i clienti nella tua situazione iniziano con circa {result}. Per un preventivo preciso, cosa ti sta più a cuore proteggere: famiglia, lavoro, progetti futuri o salute?"
Nuova Domanda del Cliente: "Mmm, direi la sicurezza della mia famiglia è la cosa più importante per me adesso."
Nuova Risposta dell'assistente : "Ottimo, la sicurezza della tua famiglia è una priorità fondamentale per molti. Per capire meglio come proteggere al meglio i tuoi cari, potresti dirmi: ci sono eventi specifici che ti preoccupano particolarmente per il futuro della tua famiglia? Ad esempio, stai pensando alla loro protezione in caso di imprevisti, al loro futuro economico a lungo termine, o a entrambi?"

### Esempio 2:
Domanda del cliente: "Quali sono i prodotti assicurativi più popolari tra i clienti della mia fascia d'età (50 anni)?"
Risposta dell'assistente : "I prodotti più richiesti tra i clienti di cinquant'anni spesso sono l'assicurazione vita con una clausola per investimenti, la pensione integrativa e le polizze per la casa o l'auto. Per capire le tue priorità, cosa ti sta più a cuore proteggere ora?"
Nuova Domanda del Cliente: "Principalmente vorrei qualcosa che mi dia una sicurezza per la pensione, ma non ci capisco molto."
Nuova Risposta dell'assistente : "Capisco perfettamente la tua priorità per la sicurezza pensionistica e la sensazione di incertezza di fronte a tante opzioni. Per aiutarti a capire meglio, potresti dirmi: hai già una qualche forma di pensione integrativa attiva? E hai un'idea di quanto vorresti integrare al tuo reddito pensionistico attuale?"
"""

# ---------------------------
# --- FUNZIONI ---
# ---------------------------

#funzioni per correggere le query
def colonne_valide(query, dataframe):
    colonne = set(col.lower() for col in dataframe.columns)
    parole = set(word.strip(",();") for word in query.lower().split())
    return all(p in colonne or not p.isidentifier() for p in parole)

def correggi_query(sql_query, dataframe):
    for col in dataframe.columns:
        sql_query = sql_query.replace(col.lower(), col)
    return sql_query

#funzione per focus conversazione (per report all'operatore)
nlp = spacy.load('it_core_news_sm')
stopwords_spacy = nlp.Defaults.stop_words

def estrai_focus(storico_domande):
    parole = []
    for domanda in storico_domande:
        doc = nlp(domanda.lower())
        for token in doc:
            if token.text not in stopwords_spacy and (token.pos_ in ['NOUN']):
                parole.append(token.text)
    conteggio = collections.Counter(parole)
    return conteggio.most_common(1)[0][0] if conteggio else "Conversazione generale"

# ---------------------------
# --- CICLO CONVERSAZIONE ---
# ---------------------------
continua = True

while continua:
    try:
        user_query = input("\nDomanda del cliente: ").strip()
        storico_domande.append(user_query)

        response = ollama.chat(model="mistral", messages=[
            {"role": "system", "content": PROMPT_SQL},
            {"role": "user", "content": user_query}
        ])
        sql_query = response['message']['content'].strip()

        if not sql_query.lower().startswith("select"):
            print("Risposta dell'assistente:\n", sql_query)
            storico_risposte.append(sql_query)
        else:
            print(f"Query SQL generata: {sql_query}")
            if colonne_valide(sql_query, merged_db):
                print(f"Query valida: {sql_query}") 
                
                sql_query_corretta = correggi_query(sql_query, merged_db)
                try:
                    result = psql.sqldf(sql_query_corretta, {'merged_db': merged_db})
                    tabelle_risultati.append(result.copy())
                    if result.empty:
                        print("Nessun dato disponibile. Vuoi parlare con un operatore?")
                        if input("Rispondi con 'si': ").strip().lower() == "si":
                            print("Ti metteremo in contatto con un operatore.")
                            break
                    else:
                        print(f"Query non valida: {sql_query}")

                        response_answer = ollama.chat(model="mistral", messages=[
                            {"role": "system", "content": PROMPT_RESPONSE},
                            {"role": "user", "content": result.to_string(index=False)}
                        ])
                        risposta_finale = response_answer['message']['content'].strip()
                        print("Risposta dell'assistente:\n", risposta_finale)
                        storico_risposte.append(risposta_finale)
                except Exception as e:
                    print("Errore nell'esecuzione della query:", e)
            else:
                fallback = ollama.chat(model="mistral", messages=[
                    {"role": "system", "content": PROMPT_RESPONSE},
                    {"role": "user", "content": user_query}
                ])
                print("Risposta dell'assistente:\n", fallback['message']['content'].strip())
                storico_risposte.append(fallback['message']['content'].strip())

        continua_risposta = input("\nVuoi continuare la conversazione? (sì/no): ").strip().lower()
        if continua_risposta == "si":
            continue
        else:
            continua = False
            valutazione = input("Come valuteresti questa conversazione da 1 a 10? ").strip()
            if valutazione.isdigit() and int(valutazione) <= 5:
                if input("Vuoi essere messo in contatto con un operatore? (si/no): ").strip().lower() == "si":
                    print("Ti metteremo in contatto con un operatore.")

                    st.subheader("Report per l'Operatore")
                    st.write(f"**Nome e Cognome:** {nome_completo.title()}")

                    if not cliente_presente_pivot.empty and codice_cliente in cliente_presente_pivot['codice_cliente'].astype(str).values:
                        st.write("**Stato:** Cliente")
                        st.write(f"**Codice Cliente:** {codice_cliente}")
                        dati_cliente = pivot_clienti[pivot_clienti["codice_cliente"].astype(str) == codice_cliente]
                        st.write("**Dati del Cliente:**")
                        st.dataframe(dati_cliente)
                    else:
                        st.write("**Stato:** Non Cliente")

                    st.write(f"**Focus della conversazione:** {estrai_focus(storico_domande)}")

                    st.subheader("Storico della Conversazione")
                    for domanda, risposta in zip(storico_domande, storico_risposte):
                        st.markdown(f"**Cliente:** {domanda}")
                        st.markdown(f"**Assistente:** {risposta}")
                        st.markdown("---")

                    if tabelle_risultati:
                        st.subheader("Tabelle Estratte")
                        for idx, tabella in enumerate(tabelle_risultati, 1):
                            st.write(f"**Risultato Query #{idx}:**")
                            st.dataframe(tabella)

            else:
                print("Grazie per la tua valutazione!")
    except Exception as e:
        print("Errore nella conversazione:", e)


