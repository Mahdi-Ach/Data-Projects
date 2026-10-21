"""
RAG bancaire Aurore — pipeline complet.
Usage :
    python rag.py ingest        # PDF -> data/chunks.jsonl
    python rag.py index         # chunks.jsonl -> chroma_db
    python rag.py search "..."  # recherche brute
    python rag.py ask "..."     # question + reponse LLM
    python rag.py eval          # evaluation (sans rerank)
    python rag.py eval_rerank   # evaluation (avec rerank)
    python rag.py debug         # diagnostic
    python rag.py all           # ingest + index + eval
"""
import json
import re
import sys
import csv
from pathlib import Path
from rank_bm25 import BM25Okapi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
try:
    ROOT = Path(__file__).resolve().parent
except NameError:
    ROOT = Path.cwd()

PDF_DIR   = ROOT / "pdfs"
DATA_DIR  = ROOT / "data"
OUT_JSONL = DATA_DIR / "chunks.jsonl"
CHROMA_DIR = ROOT / "chroma_db"
QUESTIONS_CSV = ROOT / "questions_test.csv"

REGISTRY = {
    "tarifs_particuliers_2026.pdf":     dict(title="Brochure tarifaire 2026",
                                             doc_type="tarifs", year=2026, status="en_vigueur"),
    "tarifs_particuliers_2025.pdf":     dict(title="Brochure tarifaire 2025",
                                             doc_type="tarifs", year=2025, status="perime"),
    "guide_comptes_cartes_epargne.pdf": dict(title="Guide comptes, cartes et epargne",
                                             doc_type="guide",  year=2026, status="en_vigueur"),
    "faq_securite_reclamations.pdf":    dict(title="FAQ, securite et reclamations",
                                             doc_type="faq",    year=2026, status="en_vigueur"),
}

FOOTER_HEIGHT = 57
MAX_CHARS     = 900
HEADING       = re.compile(r"^\d+\.\s+\S")
PARA_GAP      = 17

MODEL_NAME = "intfloat/multilingual-e5-small"
COLLECTION = "aurore"
MAX_DIST   = 0.20     # garde-fou "je ne sais pas" (cosine)

OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "mistral"


RERANK_MODEL_NAME = "BAAI/bge-reranker-v2-m3"
_reranker = None

# Hybrid search : fusion BM25 + dense via RRF
_bm25 = None
_bm25_chunks = None
RRF_K = 60   # parametre standard du RRF


def _tokenize(text):
    """Tokenisation simple pour BM25 : minuscules, mots de 2+ caracteres."""
    return re.findall(r"\b\w{2,}\b", text.lower())

MUST_CONTAIN = {
    1:  ["6,50 EUR"],
    2:  ["130 EUR"],
    3:  ["= 2 000 EUR"],
    4:  ["26 ans"],
    5:  ["5 000 EUR par operation"],
    6:  ["numero d'opposition"],
    7:  ["13 mois"],
    8:  ["20,00 EUR par mois"],
    9:  ["Virement instantane en ligne : Essentiel = 0,90 EUR"],
    10: ["17,50 EUR"],
    11: ["42 EUR", "45 EUR"],
    12: ["75 000 EUR"],
    13: ["Livret A"],
    14: ["conseil en investissement"],
    }

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean(s):
    return re.sub(r"\s+", " ", (s or "").replace("\n", " ")).strip()


def _norm(s):
    """Normalise tous types d'espaces (y compris insecables) pour le matching."""
    return re.sub(r"[\s\u00a0\u202f\u2009\u2007\u2028\u2029]+", " ", s).strip()


def _contains_all(text, needles):
    t = _norm(text)
    return all(_norm(n) in t for n in needles)


def split_long(text, max_chars=MAX_CHARS):
    if len(text) <= max_chars:
        return [text]
    parts, cur = [], ""
    for sent in re.split(r"(?<=[.!?])\s+", text):
        if cur and len(cur) + len(sent) > max_chars:
            parts.append(cur); cur = sent
        else:
            cur = f"{cur} {sent}".strip()
    return parts + [cur] if cur else parts


def merge_continuation_rows(rows):
    """Fusionne les lignes dont la 1re cellule est vide avec la precedente."""
    merged = []
    for r in rows:
        cells = [clean(c) for c in r]
        if not any(cells):
            continue
        if merged and not cells[0]:
            for i, v in enumerate(cells):
                if v:
                    merged[-1][i] = (merged[-1][i] + " " + v).strip() if merged[-1][i] else v
        else:
            merged.append(cells)
    return merged


def table_rows_to_sentences(rows, section=""):
    rows = merge_continuation_rows(rows)
    if len(rows) < 2:
        return []
    header = rows[0]
    out = []
    for cells in rows[1:]:
        label = cells[0] or section or "Ligne"
        pairs = "; ".join(
            f"{header[i] or f'col{i}'} = {v}"
            for i, v in enumerate(cells[1:], start=1) if v
        )
        if pairs:
            out.append(f"{label} : {pairs}")
    return out


def page_items(page):
    page = page.crop((0, 0, page.width, page.height - FOOTER_HEIGHT))
    tables = page.find_tables()
    boxes = [t.bbox for t in tables]
    items = [(t.bbox[1], "table", t.extract()) for t in tables]
    for line in page.extract_text_lines():
        cx = (line["x0"] + line["x1"]) / 2
        cy = (line["top"] + line["bottom"]) / 2
        if any(b[0] <= cx <= b[2] and b[1] <= cy <= b[3] for b in boxes):
            continue
        items.append((line["top"], "text", line["text"]))
    return sorted(items, key=lambda x: x[0])


# ---------------------------------------------------------------------------
# 1. Ingestion PDF -> chunks.jsonl
# ---------------------------------------------------------------------------
def ingest_pdf(path, meta):
    import pdfplumber
    chunks = []
    section = "Introduction"
    buf, buf_page = [], 1
    footnotes = []

    def add(kind, text, page, sec):
        chunks.append(dict(
            id=f"{path.stem}_{len(chunks):03d}",
            kind=kind, page=page, section=sec,
            source=path.name, title=meta["title"], doc_type=meta["doc_type"],
            year=meta["year"], status=meta["status"],
            text=f"[{meta['title']} | {sec}] {text}",
        ))

    def flush():
        nonlocal buf
        text = clean(" ".join(buf))
        if text:
            for piece in split_long(text):
                add("text", piece, buf_page, section)
        buf = []

    with pdfplumber.open(path) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            prev_top = None
            for top, kind, content in page_items(page):
                if kind == "table":
                    flush(); prev_top = None
                    for sent in table_rows_to_sentences(content, section):
                        add("table_row", sent, pno, section)
                    continue

                line = content.strip()
                new_para = (
                    prev_top is not None
                    and top - prev_top > PARA_GAP
                    and meta["doc_type"] != "faq"
                )
                prev_top = top

                if HEADING.match(line):
                    flush(); section = line; buf_page = pno; prev_top = None
                elif meta["doc_type"] == "faq" and line.endswith("?"):
                    flush(); buf_page = pno; buf.append(f"Question : {line} Reponse :")
                elif line.startswith("*"):
                    footnotes.append((section, clean(line)))
                elif new_para:
                    flush(); buf_page = pno; buf.append(line)
                else:
                    if not buf: buf_page = pno
                    buf.append(line)
            flush()

    for sec, note in footnotes:
        for c in chunks:
            if c["kind"] == "table_row" and c["section"] == sec and "*" in c["text"]:
                c["text"] += f" Note : {note}"
    return chunks


def cmd_ingest():
    all_chunks = []
    for name, meta in REGISTRY.items():
        p = PDF_DIR / name
        if not p.exists():
            print(f"  [skip] {name} (fichier absent)")
            continue
        c = ingest_pdf(p, meta)
        print(f"{name:40s} {len(c):3d} chunks")
        all_chunks += c

    DATA_DIR.mkdir(exist_ok=True)
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for c in all_chunks:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"\nTotal : {len(all_chunks)} chunks -> {OUT_JSONL.relative_to(ROOT)}")
    return all_chunks


# ---------------------------------------------------------------------------
# 2. Indexation ChromaDB
# ---------------------------------------------------------------------------
_model = None
_col   = None

def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def get_collection():
    global _col
    if _col is None:
        import chromadb
        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        _col = client.get_or_create_collection(
            COLLECTION, metadata={"hnsw:space": "cosine"}
        )
    return _col


def cmd_index():
    import chromadb
    chunks = [json.loads(l) for l in open(OUT_JSONL, encoding="utf-8")]
    model = get_model()

    emb = model.encode(
        ["passage: " + c["text"] for c in chunks],
        normalize_embeddings=True,
        show_progress_bar=True,
    )

    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    try:
        client.delete_collection(COLLECTION)
    except Exception:
        pass
    col = client.create_collection(COLLECTION, metadata={"hnsw:space": "cosine"})
    col.add(
        ids=[c["id"] for c in chunks],
        embeddings=emb.tolist(),
        documents=[c["text"] for c in chunks],
        metadatas=[
            {k: c[k] for k in ("source", "title", "doc_type", "section",
                               "kind", "page", "year", "status")}
            for c in chunks
        ],
    )
    print(f"{col.count()} chunks indexed -> {CHROMA_DIR.relative_to(ROOT)}")


# ---------------------------------------------------------------------------
# 3. Recherche
# ---------------------------------------------------------------------------
def search(question, k=5, include_outdated=None, guard=True):
    if include_outdated is None:
        include_outdated = bool(re.search(r"\b20(1\d|2[0-5])\b", question))
    model = get_model()
    col = get_collection()

    q = model.encode(["query: " + question], normalize_embeddings=True).tolist()
    where = None if include_outdated else {"status": "en_vigueur"}
    r = col.query(query_embeddings=q, n_results=k, where=where)
    hits = [
        dict(id=i, text=d, dist=dist, **m)
        for i, d, dist, m in zip(
            r["ids"][0], r["documents"][0],
            r["distances"][0], r["metadatas"][0],
        )
    ]
    if guard and hits and hits[0]["dist"] > MAX_DIST:
        return []
    return hits


def cmd_search(question):
    hits = search(question, k=5)
    if not hits:
        print("Je ne sais pas (aucun chunk pertinent).")
        return
    for h in hits:
        print(f"{h['dist']:.3f}  {h['source']}  p{h['page']}  |  {h['text'][:160]}")

def get_bm25():
    """Construit l'index BM25 a partir des chunks (mis en cache)."""
    global _bm25, _bm25_chunks
    if _bm25 is None:
        print("Construction de l'index BM25...")
        _bm25_chunks = [json.loads(l) for l in open(OUT_JSONL, encoding="utf-8")]
        corpus_tokens = [_tokenize(c["text"]) for c in _bm25_chunks]
        _bm25 = BM25Okapi(corpus_tokens)
    return _bm25, _bm25_chunks


def search_bm25(question, k=20, include_outdated=None):
    """Recherche BM25 (mots-cles)."""
    if include_outdated is None:
        include_outdated = bool(re.search(r"\b20(1\d|2[0-5])\b", question))

    bm25, chunks = get_bm25()
    tokens = _tokenize(question)
    scores = bm25.get_scores(tokens)

    # Filtre par statut AVANT le tri
    indexed = list(enumerate(scores))
    if not include_outdated:
        indexed = [(i, s) for i, s in indexed if chunks[i]["status"] == "en_vigueur"]

    indexed.sort(key=lambda x: x[1], reverse=True)
    hits = []
    for i, score in indexed[:k]:
        c = chunks[i]
        hits.append({
            "id": c["id"],
            "text": c["text"],
            "source": c["source"],
            "title": c["title"],
            "doc_type": c["doc_type"],
            "section": c["section"],
            "kind": c["kind"],
            "page": c["page"],
            "year": c["year"],
            "status": c["status"],
            "bm25_score": float(score),
        })
    return hits

# ---------------------------------------------------------------------------
# 4. Recherche hybride
# ---------------------------------------------------------------------------

def get_bm25():
    """Construit l'index BM25 a partir des chunks (mis en cache)."""
    global _bm25, _bm25_chunks
    if _bm25 is None:
        print("Construction de l'index BM25...")
        _bm25_chunks = [json.loads(l) for l in open(OUT_JSONL, encoding="utf-8")]
        corpus_tokens = [_tokenize(c["text"]) for c in _bm25_chunks]
        _bm25 = BM25Okapi(corpus_tokens)
    return _bm25, _bm25_chunks


def search_bm25(question, k=20, include_outdated=None):
    """Recherche BM25 (mots-cles)."""
    if include_outdated is None:
        include_outdated = bool(re.search(r"\b20(1\d|2[0-5])\b", question))

    bm25, chunks = get_bm25()
    tokens = _tokenize(question)
    scores = bm25.get_scores(tokens)

    # Filtre par statut AVANT le tri
    indexed = list(enumerate(scores))
    if not include_outdated:
        indexed = [(i, s) for i, s in indexed if chunks[i]["status"] == "en_vigueur"]

    indexed.sort(key=lambda x: x[1], reverse=True)
    hits = []
    for i, score in indexed[:k]:
        c = chunks[i]
        hits.append({
            "id": c["id"],
            "text": c["text"],
            "source": c["source"],
            "title": c["title"],
            "doc_type": c["doc_type"],
            "section": c["section"],
            "kind": c["kind"],
            "page": c["page"],
            "year": c["year"],
            "status": c["status"],
            "bm25_score": float(score),
        })
    return hits


def search_hybrid(question, k=5, k_candidates=20, include_outdated=None):
    """Hybrid search : fusion BM25 + dense via RRF, avec priorite temporelle."""
    # Detection d'annee
    year_match = re.search(r"\b(20\d{2})\b", question)
    target_year = int(year_match.group(1)) if year_match else None

    # 1. Les deux retrievals
    dense_hits = search(question, k=k_candidates,
                        include_outdated=include_outdated, guard=False)
    bm25_hits = search_bm25(question, k=k_candidates,
                            include_outdated=include_outdated)

    # 2. Si une annee est mentionnee, ne garder QUE ces chunks
    if target_year:
        dense_hits = [h for h in dense_hits if h["year"] == target_year]
        bm25_hits = [h for h in bm25_hits if h["year"] == target_year]

    # 3. RRF : score = 1 / (K + rang)
    scores = {}
    for rank, h in enumerate(dense_hits, start=1):
        scores[h["id"]] = scores.get(h["id"], 0) + 1 / (RRF_K + rank)
    for rank, h in enumerate(bm25_hits, start=1):
        scores[h["id"]] = scores.get(h["id"], 0) + 1 / (RRF_K + rank)

    # 4. Fusion : garde les metadonnees du premier hit rencontre
    by_id = {}
    for h in dense_hits + bm25_hits:
        by_id.setdefault(h["id"], h)

    if not scores:
        return []

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top = [by_id[i] for i, _ in ranked[:k]]

    # 5. Garde-fou : on regarde la distance dense du meilleur hit
    top_dense = next((h for h in dense_hits if h["id"] == top[0]["id"]), None)
    if top_dense and top_dense.get("dist", 1.0) > MAX_DIST:
        return []
    return top

# ---------------------------------------------------------------------------
# 4. ReRanking
# ---------------------------------------------------------------------------
def get_reranker():
    global _reranker
    if _reranker is None:
        from sentence_transformers import CrossEncoder
        print(f"Chargement du reranker {RERANK_MODEL_NAME}...")
        _reranker = CrossEncoder(RERANK_MODEL_NAME)
    return _reranker


def search_with_rerank(question, k_final=5, k_candidates=20, include_outdated=None):
    """Retrieval bi-encoder + reranking cross-encoder."""
    # 1. Recupere plus de candidats avec le bi-encoder
    candidates = search(question, k=k_candidates,
                        include_outdated=include_outdated, guard=False)
    if not candidates:
        return []

    # 2. Rerank avec le cross-encoder
    reranker = get_reranker()
    pairs = [(question, h["text"]) for h in candidates]
    scores = reranker.predict(pairs)

    # 3. Trie par score decroissant
    ranked = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)
    top = [h for h, _ in ranked[:k_final]]

    # 4. Garde-fou sur le meilleur resultat
    if top and top[0]["dist"] > MAX_DIST:
        return []
    return top

# ---------------------------------------------------------------------------
# 4. Evaluation
# ---------------------------------------------------------------------------
def load_questions():
    with open(QUESTIONS_CSV, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def expected_sources(row):
    return [s.strip() for s in row["source_doc"].split("+") if s.strip().endswith(".pdf")]


def cmd_eval(k=5):
    # 4a) source attendue presente dans le top-k
    hits = total = 0
    for r in load_questions():
        exp = expected_sources(r)
        if not exp:
            continue
        got = [h["source"] for h in search(r["question"], k, guard=False)]
        ok = all(e in got for e in exp)
        hits += ok; total += 1
        print(f'{"OK  " if ok else "FAIL"} Q{r["id"]:>2}  expected={exp}  got={got}')
    if total:
        print(f"\nHit@{k} (source): {hits}/{total} = {hits/total:.0%}")

    # 4b) fait attendu present dans le top-k
    ranks = []
    print()
    for r in load_questions():
        qid = int(r["id"])
        if qid not in MUST_CONTAIN:
            continue
        needed = MUST_CONTAIN[qid]
        hs = search(r["question"], k, guard=False)
        rank = next((i + 1 for i, h in enumerate(hs)
                     if _contains_all(h["text"], needed)), None)
        if rank is None and all(any(_norm(n) in _norm(h["text"]) for h in hs) for n in needed):
            rank = "union"
        ranks.append((qid, rank))
        print(f'{"OK  " if rank else "FAIL"} Q{qid:>2} rank={rank}')

    if ranks:
        ok1 = sum(1 for _, r in ranks if r == 1)
        ok3 = sum(1 for _, r in ranks if isinstance(r, int) and r <= 3)
        okk = sum(1 for _, r in ranks if r)
        mrr = sum(1 / r for _, r in ranks if isinstance(r, int)) / len(ranks)
        print(f"\nHit@1: {ok1}/{len(ranks)} | Hit@3: {ok3}/{len(ranks)} "
              f"| Hit@{k}: {okk}/{len(ranks)} | MRR: {mrr:.2f}")

def cmd_eval_rerank(k=5):
    """Evalue avec le reranker active."""
    ranks = []
    for r in load_questions():
        qid = int(r["id"])
        if qid not in MUST_CONTAIN:
            continue
        needed = MUST_CONTAIN[qid]
        hs = search_with_rerank(r["question"], k_final=k, k_candidates=20)
        rank = next((i + 1 for i, h in enumerate(hs)
                     if _contains_all(h["text"], needed)), None)
        if rank is None and all(any(_norm(n) in _norm(h["text"]) for h in hs) for n in needed):
            rank = "union"
        ranks.append((qid, rank))
        print(f'{"OK  " if rank else "FAIL"} Q{qid:>2} rank={rank}')

    if ranks:
        ok1 = sum(1 for _, r in ranks if r == 1)
        ok3 = sum(1 for _, r in ranks if isinstance(r, int) and r <= 3)
        okk = sum(1 for _, r in ranks if r)
        mrr = sum(1 / r for _, r in ranks if isinstance(r, int)) / len(ranks)
        print(f"\n[AVEC RERANK] Hit@1: {ok1}/{len(ranks)} | Hit@3: {ok3}/{len(ranks)} "
              f"| Hit@{k}: {okk}/{len(ranks)} | MRR: {mrr:.2f}")

def cmd_eval_hybrid(k=5):
    """Evalue avec hybrid search."""
    ranks = []
    for r in load_questions():
        qid = int(r["id"])
        if qid not in MUST_CONTAIN:
            continue
        needed = MUST_CONTAIN[qid]
        hs = search_hybrid(r["question"], k=k, k_candidates=20)
        rank = next((i + 1 for i, h in enumerate(hs)
                     if _contains_all(h["text"], needed)), None)
        if rank is None and all(any(_norm(n) in _norm(h["text"]) for h in hs) for n in needed):
            rank = "union"
        ranks.append((qid, rank))
        print(f'{"OK  " if rank else "FAIL"} Q{qid:>2} rank={rank}')

    if ranks:
        ok1 = sum(1 for _, r in ranks if r == 1)
        ok3 = sum(1 for _, r in ranks if isinstance(r, int) and r <= 3)
        okk = sum(1 for _, r in ranks if r)
        mrr = sum(1 / r for _, r in ranks if isinstance(r, int)) / len(ranks)
        print(f"\n[HYBRID] Hit@1: {ok1}/{len(ranks)} | Hit@3: {ok3}/{len(ranks)} "
              f"| Hit@{k}: {okk}/{len(ranks)} | MRR: {mrr:.2f}")
# ---------------------------------------------------------------------------
# 5. Diagnostic
# ---------------------------------------------------------------------------
def cmd_debug():
    print("=== A. Tables brutes de la page 1 (PDF 2026) ===")
    try:
        import pdfplumber
        with pdfplumber.open(PDF_DIR / "tarifs_particuliers_2026.pdf") as pdf:
            for ti, t in enumerate(pdf.pages[0].find_tables()):
                print(f"-- table {ti} --")
                for row in t.extract():
                    print([repr(c) for c in row])
    except Exception as e:
        print("  erreur:", e)

    print("\n=== B. Chunks contenant 'investis' ===")
    if not OUT_JSONL.exists():
        print("  chunks.jsonl absent — lance `python rag.py ingest` d'abord.")
        return
    chunks = [json.loads(l) for l in open(OUT_JSONL, encoding="utf-8")]
    found = 0
    for c in chunks:
        if "investis" in c["text"].lower():
            found += 1
            print(f"  {c['id']} | {c['source']} p{c['page']} | {c['text'][:220]}")
    if not found:
        print("  AUCUN chunk ne contient 'investis' -> perte a l'ingestion.")

    print("\n=== C. Top-10 pour 'conseil en investissement' ===")
    try:
        for h in search("conseil en investissement", k=10, guard=False):
            print(f"  {h['dist']:.3f} | {h['source']} p{h['page']} | {h['text'][:150]}")
    except Exception as e:
        print("  index non construit:", e)

    print("\n=== D. Calibration MAX_DIST (top-1 par question) ===")
    try:
        for r in load_questions():
            hs = search(r["question"], k=1, guard=False)
            if hs:
                print(f"  Q{r['id']:>2}  dist={hs[0]['dist']:.3f}  {r['question'][:60]}")
    except Exception as e:
        print("  erreur:", e)


# ---------------------------------------------------------------------------
# 6. Generation avec LLM (Ollama local)
# ---------------------------------------------------------------------------
PROMPT_TEMPLATE = """Tu es un assistant bancaire de Banque Aurore.
Reponds UNIQUEMENT a partir des extraits ci-dessous.
Si la reponse n'y est pas, reponds exactement : "Je ne sais pas."
Ne fais aucune supposition. Cite toujours la source entre parentheses.

Extraits :
{contexte}

Question : {question}
Reponse :"""


def answer(question, k=5):
    """Repond a partir des chunks trouves. Refuse si rien de pertinent."""
    hits = search(question, k=k)
    if not hits:
        return {"answer": "Je ne sais pas.", "sources": []}

    contexte = "\n\n".join(
        f"[{h['source']} p{h['page']} | {h['section']}]\n{h['text']}"
        for h in hits
    )
    prompt = PROMPT_TEMPLATE.format(contexte=contexte, question=question)

    import requests
    try:
        r = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=120,
        )
        r.raise_for_status()
        reponse = r.json()["response"].strip()
    except Exception as e:
        reponse = f"[Erreur Ollama: {e}] Verifie que 'ollama serve' tourne."

    return {
        "answer": reponse,
        "sources": [
            {"source": h["source"], "page": h["page"], "section": h["section"]}
            for h in hits
        ],
    }


def cmd_ask(question):
    r = answer(question)
    print("Reponse :", r["answer"])
    if r["sources"]:
        print("\nSources :")
        for s in r["sources"]:
            print(f"  - {s['source']} p{s['page']} | {s['section']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def cmd_all():
    cmd_ingest()
    cmd_index()
    cmd_eval(k=5)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    cmd = sys.argv[1]
    if cmd == "ingest":
        cmd_ingest()
    elif cmd == "index":
        cmd_index()
    elif cmd == "search":
        if len(sys.argv) < 3:
            print("Usage: python rag.py search \"ta question\"")
            return
        cmd_search(" ".join(sys.argv[2:]))
    elif cmd == "ask":
        if len(sys.argv) < 3:
            print('Usage: python rag.py ask "ta question"')
            return
        cmd_ask(" ".join(sys.argv[2:]))
    elif cmd == "eval_rerank":
        cmd_eval_rerank(k=5)
    elif cmd == "eval_hybrid":
        cmd_eval_hybrid(k=5)
    elif cmd == "eval":
        cmd_eval(k=5)
    elif cmd == "debug":
        cmd_debug()
    elif cmd == "all":
        cmd_all()
    else:
        print(f"Commande inconnue: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()