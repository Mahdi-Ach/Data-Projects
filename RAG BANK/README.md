# 🏦 RAG bancaire Aurore

Assistant Q&A sur documents bancaires PDF, avec retrieval vectoriel et génération LLM locale.

## 📋 Description

Ce projet implémente un pipeline **RAG (Retrieval-Augmented Generation)** complet pour répondre en langage naturel à des questions sur des documents bancaires. Il combine :

- **Extraction structurée de PDF** (texte + tableaux complexes avec cellules wrappées)
- **Indexation vectorielle** pour la recherche sémantique
- **Retrieval avec filtres temporels** (gestion de documents périmés vs en vigueur)
- **Génération par LLM local** (privacy-safe, aucune donnée envoyée à un tiers)
- **Garde-fou anti-hallucination** (refus explicite sur questions hors-corpus)

## 🏗️ Architecture

```
                        OFFLINE
    ┌───────────────────────────────────────────────┐
    │  PDF → Cleaning → Chunks → Embeddings → Index │
    └───────────────────────────────────────────────┘
                          ↓
                        ONLINE
    ┌─────────────────────────────────────────────────┐
    │  Question → Retrieval → Prompt → LLM → Response │
    └─────────────────────────────────────────────────┘
```

**Pipeline détaillé :**

1. **Ingestion** : 4 PDF bancaires → 98 chunks structurés
2. **Nettoyage** : fusion des cellules de tableau wrappées, rattachement des notes de bas de page
3. **Chunking** : par section + paragraphe, taille max 900 caractères
4. **Embeddings** : `intfloat/multilingual-e5-small` avec préfixes `query:` / `passage:`
5. **Index** : ChromaDB persistant, similarité cosinus
6. **Retrieval** : top-K avec filtres par métadonnées (année, statut)
7. **Prompt** : contexte + question, template strict anti-hallucination
8. **LLM** : Ollama + Mistral en local, streaming token par token
9. **Response** : réponse + sources citées

## 🛠️ Stack technique

| Composant | Technologie |
|---|---|
| Langage | Python 3.11 |
| Extraction PDF | pdfplumber |
| Embeddings | sentence-transformers (`multilingual-e5-small`) |
| Base vectorielle | ChromaDB |
| LLM | Ollama + Mistral (local) |
| Interface web | Flask + Socket.IO |
| Interface alternative | Streamlit |

## 📁 Structure du projet

```
RAG BANK/
├── pdfs/                              # Documents sources
│   ├── tarifs_particuliers_2026.pdf
│   ├── tarifs_particuliers_2025.pdf
│   ├── guide_comptes_cartes_epargne.pdf
│   └── faq_securite_reclamations.pdf
├── data/
│   └── chunks.jsonl                   # Chunks générés (98)
├── chroma_db/                         # Base vectorielle
├── rag.py                             # Pipeline RAG principal
├── server.py                          # Interface Flask + Socket.IO
├── questions_test.csv                 # 14 questions d'évaluation
├── requirements.txt                   # Dépendances
└── README.md
```

## 🚀 Installation

### 1. Cloner le projet

```bash
git clone https://github.com/Mahdi-Ach/RAG-for-Bank.git
cd "RAG BANK"
```

### 2. Créer un environnement virtuel

```bash
python -m venv venv
venv\Scripts\activate     # Windows
# source venv/bin/activate  # Linux/macOS
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Installer Ollama (LLM local)

Télécharger et installer depuis [ollama.com/download](https://ollama.com/download), puis :

```bash
ollama pull mistral
```

## 💻 Utilisation

### Ingestion et indexation (une seule fois)

```bash
python rag.py ingest     # PDF → chunks.jsonl
python rag.py index      # chunks.jsonl → ChromaDB
```

### Poser une question (ligne de commande)

```bash
python rag.py ask "Combien coute le Compte Confort ?"
```

**Exemple de sortie :**

```
Reponse : Le Compte Confort coûte 6,50 EUR par mois
          (source : tarifs_particuliers_2026.pdf p1).

Sources :
  - tarifs_particuliers_2026.pdf p1 | 1. Comptes
```

### Interface web (chat temps réel)

Dans un premier terminal :

```bash
ollama serve
```

Dans un second terminal :

```bash
python server.py
```

Ouvrir **http://localhost:5000** dans un navigateur.

**Fonctionnalités du chat :**
- Streaming token par token (la réponse s'affiche en direct)
- Statuts intermédiaires ("Recherche...", "Génération...")
- Sources citées sous chaque réponse
- Refus automatique "Je ne sais pas" sur questions hors-corpus

### Évaluation

```bash
python rag.py eval       # évalue sur questions_test.csv
python rag.py debug      # diagnostic détaillé
```

## 📊 Résultats

Évaluation sur 14 questions de test :

| Métrique | Score |
|---|---|
| **Hit@5 (source)** | 14/14 = **100 %** |
| **Hit@1 (fait attendu)** | 13/14 = **93 %** |
| **Hit@3 (fait attendu)** | 13/14 = **93 %** |
| **Hit@5 (fait attendu)** | 14/14 = **100 %** |
| **MRR** | **0.93** |

**Comportements validés :**

- ✅ Refus correct sur questions hors-corpus (*"Quelle est la capitale de l'Australie ?"* → *"Je ne sais pas"*)
- ✅ Versioning temporel (*"Combien coutait le Compte Confort en 2025 ?"* → 6,00 EUR, PDF 2025)
- ✅ Sources citées dans chaque réponse
- ✅ Zéro hallucination

## 🔒 Sécurité et confidentialité

- **LLM 100 % local** (Ollama + Mistral) : aucune donnée n'est envoyée à OpenAI, Anthropic ou un autre tiers
- **Base vectorielle locale** (ChromaDB) : pas de service cloud
- **Garde-fou anti-hallucination** : refus explicite si aucun chunk n'est pertinent (seuil de distance cosinus)
- Compatible avec les exigences RGPD d'un contexte bancaire

## 🔧 Commandes disponibles

```bash
python rag.py ingest         # Ingestion PDF → chunks.jsonl
python rag.py index          # Indexation chunks → ChromaDB
python rag.py search "..."   # Recherche brute (sans LLM)
python rag.py ask "..."      # Question + réponse générée
python rag.py eval           # Évaluation sur questions_test.csv
python rag.py debug          # Diagnostic (tables, chunks, distances)
python rag.py all            # ingest + index + eval
```

## 🎯 Cas d'usage

- Documentation tarifaire bancaire (comparaison multi-années)
- Guides produits (comptes, cartes, épargne)
- FAQ clients (sécurité, réclamations)
- Support interne sur documents réglementaires

## 📝 Améliorations futures

- [ ] Reranking cross-encoder (`bge-reranker-base`)
- [ ] Recherche hybride BM25 + dense
- [ ] Évaluation sur questions réelles utilisateurs
- [ ] Interface Streamlit alternative
- [ ] Logging des questions pour analyse

## 👤 Auteur

Projet personnel de démonstration RAG sur corpus bancaire fictif.

## 📄 Licence

Usage éducatif et démonstration.
